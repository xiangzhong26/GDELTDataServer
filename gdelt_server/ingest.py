"""Durable slot scheduling and sequential, size-limited temporary downloads."""
from __future__ import annotations

from datetime import datetime, timezone
import logging
import shutil
import tempfile
import threading
from pathlib import Path
import httpx

from .config import Settings
from .parser import parse_events, parse_gkg
from .store import Store, DAY, SLOT, utcnow

LOG = logging.getLogger(__name__)
BASE = "https://data.gdeltproject.org/gdeltv2"


def file_url(kind, ts):
    if kind not in ("events", "gkg"):
        raise ValueError("未知数据源")
    stamp = datetime.fromtimestamp(ts, timezone.utc).strftime("%Y%m%d%H%M%S")
    suffix = "export.CSV.zip" if kind == "events" else "gkg.csv.zip"
    return f"{BASE}/{stamp}.{suffix}"


class Ingestor:
    def __init__(self, store: Store, settings: Settings, client=None):
        self.store, self.settings = store, settings
        self.client = client or httpx.Client(timeout=settings.request_timeout, follow_redirects=True,
                                            headers={"User-Agent": "DSI-GDELTDataServer/0.1"})
        self.owns_client = client is None
        self.temp_dir = settings.data_dir / "tmp"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.busy = threading.Lock()
        self.cancel = threading.Event()
        self.phase = "idle"
        self.current_file = None
        self.on_update = None
        self.last_prune = 0.

    def cleanup_abandoned(self):
        # Called only after the service's exclusive instance lock is acquired.
        for p in self.temp_dir.glob("gdelt-*.zip"):
            p.unlink(missing_ok=True)

    def check_disk(self):
        free = shutil.disk_usage(self.settings.data_dir).free
        if free < self.settings.min_free_gb * 1024**3:
            raise RuntimeError("可用磁盘低于安全余量，已暂停下载；清理或调整保留期后重试")
        files = [self.store.path, Path(str(self.store.path)+"-wal")]
        size = sum(p.stat().st_size for p in files if p.exists())
        if size > self.settings.max_database_gb * 1024**3:
            raise RuntimeError("数据库超过配置的容量预算，已暂停下载；缩短保留期或扩大预算后重试")

    def schedule(self, now=None):
        now = now if now is not None else int(utcnow().timestamp())
        # Include every fully elapsed slot up to (but excluding) the current slot.
        horizon = now // SLOT * SLOT
        start = self.store.get_state("scheduled_until")
        if start is None:
            start = horizon - self.settings.initial_hours * 3600
            with self.store.connect() as db:
                latest = [r[0] for r in db.execute("SELECT MAX(file_ts) FROM ingest_file WHERE status='done' GROUP BY kind")]
            if len(latest) == 2:
                # Use the slower source and overlap its final slot, so one source's
                # newer timestamp cannot hide the other source's unfinished tail.
                start = min(latest)
        if self.store.get_state('incremental_start') is None:
            self.store.set_state('incremental_start', min(start, horizon-self.settings.initial_hours*3600))
        oldest = horizon - self.settings.day_retention_days * DAY
        if start < oldest:
            self.store.set_state("unrecoverable_gap", {"start": start, "end": oldest,
                                                        "reason": "超过配置的日聚合保留期"})
            start = oldest
        if start >= horizon:
            return 0
        # At most one day of pending rows is seeded per pass; cursor persists progress.
        end = min(horizon, start + DAY)
        return self.store.enqueue(start, end, cursor=end)

    def backfill(self, hours, start_ts=None, end_ts=None):
        if hours < 1 or hours > self.settings.day_retention_days * 24:
            raise ValueError("回填范围必须在1小时至日保留期之间")
        horizon = int(utcnow().timestamp()) // SLOT * SLOT
        return self.store.enqueue(start_ts if start_ts is not None else horizon-hours*3600,
                                  end_ts if end_ts is not None else horizon)

    def process(self, kind, ts):
        self.check_disk()
        if ts < int(utcnow().timestamp()) // DAY * DAY - self.settings.day_retention_days * DAY:
            raise ValueError("批次已超出保留期，请先扩大日保留期")
        path = None
        try:
            with tempfile.NamedTemporaryFile(prefix="gdelt-", suffix=".zip", dir=self.temp_dir, delete=False) as f:
                path = Path(f.name)
                size = 0
                with self.client.stream("GET", file_url(kind, ts)) as response:
                    response.raise_for_status()
                    for block in response.iter_bytes(1024*1024):
                        if self.cancel.is_set():
                            raise InterruptedError("同步已停止，批次将在后续重试")
                        size += len(block)
                        if size > self.settings.max_download_mb*1024*1024:
                            raise ValueError("下载文件超过单文件大小上限")
                        f.write(block)
            parser = parse_gkg if kind == "gkg" else parse_events
            parsed = parser(path, ts, self.settings.max_uncompressed_mb)
            if parsed.rows == 0:
                raise ValueError("文件没有可解析数据行，不标记为完成")
            if self.cancel.is_set():
                raise InterruptedError("同步已停止")
            return self.store.apply(kind, ts, parsed.tables, parsed.rows, parsed.skipped)
        finally:
            if path is not None:
                path.unlink(missing_ok=True)

    def run_once(self, schedule=True, limit=None, clear_cancel=True, ranges=None, exclude_range=None, descending=False):
        if not self.busy.acquire(blocking=False):
            return {"skipped": "已有同步或快照任务运行中"}
        done = failed = 0
        try:
            if clear_cancel:
                self.cancel.clear()
            self.phase = "scheduling"
            import time
            if time.monotonic()-self.last_prune >= 3600:
                self.store.prune(self.settings.hour_retention_days, self.settings.day_retention_days)
                self.last_prune = time.monotonic()
            if schedule:
                self.schedule()
            self.check_disk()
            pending = self.store.pending(limit or self.settings.batch_files, ranges=ranges,
                                         exclude_range=exclude_range, descending=descending)
            self.phase = "processing"
            for row in pending:
                if self.cancel.is_set():
                    break
                kind, ts = row["kind"], row["file_ts"]
                self.current_file = {"kind": kind, "file_ts": ts}
                try:
                    done += int(self.process(kind, ts))
                except InterruptedError:
                    break
                except Exception as exc:
                    failed += 1
                    self.store.mark_failed(kind, ts, exc)
                    LOG.warning("批次失败 %s %s: %s", kind, ts, exc)
            if done:
                self.store.set_state("last_ingest_at", utcnow().isoformat())
                self.store.set_state("snapshot_dirty", True)
            if self.on_update:
                self.on_update()
            result = {"processed": done, "failed": failed, "queued": len(pending)}
            self.store.set_state("last_run", result)
            self.store.set_state("last_error", None)
            return result
        except Exception as exc:
            self.store.set_state("last_error", str(exc))
            raise
        finally:
            self.phase, self.current_file = "idle", None
            self.busy.release()

    def close(self):
        self.cancel.set()
        if self.owns_client:
            self.client.close()
