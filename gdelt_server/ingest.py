"""Durable scheduling, bounded concurrent downloads and process-based parsing."""
from __future__ import annotations

from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, FIRST_COMPLETED
from concurrent.futures.process import BrokenProcessPool
import logging
import multiprocessing
import shutil
import tempfile
import threading
import time
from pathlib import Path
import httpx

from .config import Settings
from .parser import parse_events, parse_gkg
from .store import Store, DAY, SLOT, bucket_of, utcnow

LOG = logging.getLogger(__name__)
BASE = "https://data.gdeltproject.org/gdeltv2"

_PARSER_CANCEL = None


def _init_parser(cancel):
    global _PARSER_CANCEL
    _PARSER_CANCEL = cancel


def _parse_file(kind, path, ts, max_mb):
    parser = parse_gkg if kind == 'gkg' else parse_events
    return parser(path, ts, max_mb, cancel=_PARSER_CANCEL)


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
        self.write_lock = threading.Lock()
        self.active_lock = threading.Lock()
        self.active = {}
        self.parser_pool = None
        self.parser_cancel = None
        self.parser_broken = False

    def ensure_parser_pool(self):
        if self.settings.parser_workers > 1 and self.parser_pool is None:
            # Spawn avoids inheriting SQLite connections and locks from the web server.
            context = multiprocessing.get_context('spawn')
            self.parser_cancel = context.Event()
            self.parser_pool = ProcessPoolExecutor(self.settings.parser_workers, mp_context=context,
                                                  initializer=_init_parser, initargs=(self.parser_cancel,))
        if self.parser_cancel is not None:
            if self.cancel.is_set():
                self.parser_cancel.set()
            else:
                self.parser_cancel.clear()

    def concurrency_status(self):
        with self.active_lock:
            return {'download_workers': self.settings.download_workers,
                    'parser_workers': self.settings.parser_workers,
                    'active_files': list(self.active.values())}

    def tracked_process(self, kind, ts):
        with self.active_lock:
            self.active[(kind, ts)] = {'kind': kind, 'file_ts': ts}
            self.current_file = next(iter(self.active.values()))
        try:
            if self.cancel.is_set():
                raise InterruptedError('同步已停止')
            return self.process(kind, ts)
        finally:
            with self.active_lock:
                self.active.pop((kind, ts), None)
                self.current_file = next(iter(self.active.values()), None)

    def cleanup_abandoned(self):
        # Called only after the service's exclusive instance lock is acquired.
        for p in self.temp_dir.glob("gdelt-*.zip"):
            p.unlink(missing_ok=True)

    def check_disk(self):
        free = shutil.disk_usage(self.settings.data_dir).free
        reserve = self.settings.download_workers*self.settings.max_download_mb*1024**2
        if free < self.settings.min_free_gb * 1024**3 + reserve:
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

    def repair_gaps(self, now=None):
        """Requeue missing slots inside known history, retaining completed batches."""
        horizon = int(now if now is not None else utcnow().timestamp())//SLOT*SLOT
        oldest = bucket_of(horizon, 'day')-self.settings.day_retention_days*DAY
        with self.store.connect() as db:
            first = db.execute("SELECT MIN(file_ts) FROM ingest_file WHERE file_ts>=? AND file_ts<?",
                               (oldest, horizon)).fetchone()[0]
        return self.store.enqueue(first, horizon) if first is not None else 0

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
            if self.parser_pool is None:
                parsed = parser(path, ts, self.settings.max_uncompressed_mb, cancel=self.cancel)
            else:
                try:
                    future = self.parser_pool.submit(_parse_file, kind, path, ts, self.settings.max_uncompressed_mb)
                    # Keep the ZIP until the parser finishes, including during cancellation.
                    while not future.done():
                        wait((future,), timeout=.05)
                        if self.cancel.is_set():
                            self.parser_cancel.set()
                            future.cancel()
                    if future.cancelled():
                        raise InterruptedError('同步已停止')
                    parsed = future.result()
                except BrokenProcessPool:
                    self.parser_broken = True
                    raise
            if parsed.rows == 0:
                raise ValueError("文件没有可解析数据行，不标记为完成")
            with self.write_lock:
                if self.cancel.is_set():
                    raise InterruptedError("同步已停止")
                self.check_disk()
                return self.store.apply(kind, ts, parsed.tables, parsed.rows, parsed.skipped)
        finally:
            if path is not None:
                path.unlink(missing_ok=True)

    def run_once(self, schedule=True, limit=None, clear_cancel=True, ranges=None, exclude_range=None, descending=False):
        if not self.busy.acquire(blocking=False):
            return {"skipped": "已有同步或快照任务运行中"}
        done = failed = 0
        started = time.monotonic()
        try:
            if clear_cancel:
                self.cancel.clear()
            self.phase = "scheduling"
            if time.monotonic()-self.last_prune >= 3600:
                self.store.prune(self.settings.hour_retention_days, self.settings.day_retention_days)
                self.last_prune = time.monotonic()
            if schedule:
                self.schedule()
            self.check_disk()
            pending = self.store.pending(limit or self.settings.batch_files, ranges=ranges,
                                         exclude_range=exclude_range, descending=descending)
            self.phase = "processing"
            if pending and not self.cancel.is_set():
                self.ensure_parser_pool()
                rows = iter(pending)
                # Submit only one task per download worker, never the entire backfill.
                with ThreadPoolExecutor(self.settings.download_workers, thread_name_prefix='gdelt-file') as executor:
                    inflight = {}
                    def submit_next():
                        if self.cancel.is_set():
                            return
                        row = next(rows, None)
                        if row is not None:
                            future = executor.submit(self.tracked_process, row['kind'], row['file_ts'])
                            inflight[future] = row
                    for _ in range(self.settings.download_workers):
                        submit_next()
                    while inflight:
                        completed, _ = wait(inflight, timeout=.1, return_when=FIRST_COMPLETED)
                        if self.cancel.is_set() and self.parser_cancel is not None:
                            self.parser_cancel.set()
                        for future in completed:
                            row = inflight.pop(future)
                            try:
                                done += int(future.result())
                            except InterruptedError:
                                pass
                            except Exception as exc:
                                if not self.cancel.is_set():
                                    failed += 1
                                    self.store.mark_failed(row['kind'], row['file_ts'], exc)
                                    LOG.warning('批次失败 %s %s: %s', row['kind'], row['file_ts'], exc)
                            submit_next()
            if done:
                self.store.set_state("last_ingest_at", utcnow().isoformat())
                self.store.set_state("snapshot_dirty", True)
            if self.on_update:
                self.on_update()
            elapsed = time.monotonic()-started
            result = {"processed": done, "failed": failed, "queued": len(pending),
                      "seconds": round(elapsed, 2), "files_per_second": round(done/max(elapsed, .001), 2)}
            self.store.set_state("last_run", result)
            self.store.set_state("last_error", None)
            return result
        except Exception as exc:
            self.store.set_state("last_error", str(exc))
            raise
        finally:
            if self.parser_broken:
                self.parser_pool.shutdown(wait=True, cancel_futures=True)
                self.parser_pool = self.parser_cancel = None
                self.parser_broken = False
            self.phase, self.current_file = "idle", None
            self.busy.release()

    def close(self):
        self.cancel.set()
        if self.parser_cancel is not None:
            self.parser_cancel.set()
        if self.parser_pool is not None:
            self.parser_pool.shutdown(wait=True, cancel_futures=True)
            self.parser_pool = None
        if self.owns_client:
            self.client.close()
