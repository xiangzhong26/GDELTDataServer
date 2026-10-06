"""Durable scheduling, bounded concurrent downloads and process-based parsing."""
from __future__ import annotations

from datetime import datetime, timezone
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, wait, FIRST_COMPLETED
from concurrent.futures.process import BrokenProcessPool
import logging
import multiprocessing
import queue
from collections import deque
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
_PARSER_PROGRESS = None


def _init_parser(cancel, progress=None):
    global _PARSER_CANCEL, _PARSER_PROGRESS
    _PARSER_CANCEL = cancel
    _PARSER_PROGRESS = progress
    if progress is not None:
        progress.cancel_join_thread()  # Workers must not wait for UI telemetry to drain on exit.


def _parse_file(kind, path, ts, max_mb):
    parser = parse_gkg if kind == 'gkg' else parse_events
    started = time.monotonic()
    def report(rows, stage='parsing'):
        if _PARSER_PROGRESS is not None:
            try:
                _PARSER_PROGRESS.put_nowait((kind, ts, stage, rows, time.monotonic()))
            except queue.Full:
                pass  # Observability must never block processing.
    report(0)
    parsed = parser(path, ts, max_mb, cancel=_PARSER_CANCEL, progress=report)
    parsed.parse_seconds = time.monotonic()-started
    report(parsed.rows, 'parse_transfer')
    return parsed


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
                                            limits=httpx.Limits(max_connections=64, max_keepalive_connections=64),
                                            headers={"User-Agent": "DSI-GDELTDataServer/0.1"})
        self.owns_client = client is None
        self.temp_dir = settings.data_dir / "tmp"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.busy = threading.Lock()
        self.cancel = threading.Event()
        self.reconfigure = threading.Event()
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
        self.timings = {}
        self.parser_progress = None
        self.progress_lock = threading.Lock()
        self.cycle = {}
        self.recent_files = deque(maxlen=12)
        self.last_completed = None
        self.phase_started = time.monotonic()
        self.last_stall_check = 0.

    def set_phase(self, phase):
        self.phase = phase
        self.phase_started = time.monotonic()

    def file_progress(self, kind, ts, **updates):
        now = time.monotonic()
        with self.active_lock:
            row = self.active.get((kind, ts))
            if row is not None:
                if updates.get('stage', row['stage']) != row['stage']:
                    row['stage_started'] = now
                row.update(updates)
                row['last_activity'] = now

    def drain_parser_progress(self):
        with self.progress_lock:
            self._drain_parser_progress()

    def _drain_parser_progress(self):
        if self.parser_progress is not None:
            while True:
                try:
                    kind, ts, stage, rows, reported = self.parser_progress.get_nowait()
                except queue.Empty:
                    break
                # A delayed child message must not revert a file already committing.
                with self.active_lock:
                    row = self.active.get((kind, ts))
                    if row is not None and reported >= row['started'] and row['stage'] in ('parse_pipeline', 'parsing', 'parse_transfer'):
                        if stage != row['stage']:
                            row['stage_started'] = reported
                        row.update(stage=stage, parsed_rows=rows, last_activity=reported)

    def stop(self):
        self.cancel.set()
        if self.parser_cancel is not None:
            self.parser_cancel.set()

    def set_concurrency(self, download_workers, parser_workers):
        # Called only between cycles with the ingest lock held and all files drained.
        if parser_workers != self.settings.parser_workers and self.parser_pool is not None:
            self.parser_pool.shutdown(wait=True, cancel_futures=True)
            self.parser_pool = self.parser_cancel = None
            self.close_progress_queue()
        self.settings.download_workers = download_workers
        self.settings.parser_workers = parser_workers
        self.reconfigure.clear()

    def record_timing(self, stage, elapsed):
        with self.active_lock:
            self.timings[stage] = self.timings.get(stage, 0.)+elapsed

    @contextmanager
    def stage(self, kind, ts, name):
        self.file_progress(kind, ts, stage=name)
        started = time.monotonic()
        try:
            yield
        finally:
            self.record_timing(name, time.monotonic()-started)

    def ensure_parser_pool(self):
        if self.settings.parser_workers > 1 and self.parser_pool is None:
            # Spawn avoids inheriting SQLite connections and locks from the web server.
            context = multiprocessing.get_context('spawn')
            self.parser_cancel = context.Event()
            self.parser_progress = context.Queue(maxsize=256)
            self.parser_pool = ProcessPoolExecutor(self.settings.parser_workers, mp_context=context,
                                                  initializer=_init_parser, initargs=(self.parser_cancel, self.parser_progress))
        if self.parser_cancel is not None:
            if self.cancel.is_set():
                self.parser_cancel.set()
            else:
                self.parser_cancel.clear()

    def concurrency_status(self):
        self.drain_parser_progress()
        now = time.monotonic()
        with self.active_lock:
            return {'download_workers': self.settings.download_workers,
                    'parser_workers': self.settings.parser_workers,
                    'active_files': [{**{k:v for k,v in row.items() if k not in ('started', 'stage_started', 'last_activity', 'warned_at')},
                                      'elapsed_seconds': round(now-row['started'], 1),
                                      'stage_seconds': round(now-row['stage_started'], 1),
                                      'inactive_seconds': round(max(0., now-row['last_activity']), 1)}
                                     for row in self.active.values()],
                    'cycle': dict(self.cycle), 'recent_files': list(self.recent_files),
                    'seconds_since_completion': round(now-self.last_completed, 1) if self.last_completed else None,
                    'phase_seconds': round(now-self.phase_started, 1)}

    def warn_stalled(self):
        now = time.monotonic()
        if now-self.last_stall_check < 5:
            return
        self.last_stall_check = now
        warnings = []
        with self.active_lock:
            for row in self.active.values():
                inactive = now-row['last_activity']
                if inactive >= 60 and now-row.get('warned_at', 0) >= 60:
                    row['warned_at'] = now
                    warnings.append((row['filename'], row['stage'], round(inactive), row['parsed_rows']))
        for filename, stage, inactive, rows in warnings:
            LOG.warning('文件较久未报告进展 filename=%s stage=%s inactive_seconds=%s parsed_rows=%s',
                        filename, stage, inactive, rows)

    def tracked_process(self, kind, ts):
        with self.active_lock:
            now = time.monotonic()
            self.active[(kind, ts)] = {'kind': kind, 'file_ts': ts, 'stage': 'starting',
                                      'filename': file_url(kind, ts).rsplit('/', 1)[1],
                                      'started': now, 'stage_started': now, 'last_activity': now,
                                      'downloaded_bytes': 0, 'total_bytes': None, 'parsed_rows': 0}
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
            download_started = time.monotonic()
            with self.stage(kind, ts, 'download'), tempfile.NamedTemporaryFile(prefix="gdelt-", suffix=".zip", dir=self.temp_dir, delete=False) as f:
                path = Path(f.name)
                size = 0
                with self.client.stream("GET", file_url(kind, ts), timeout=httpx.Timeout(
                        self.settings.request_timeout, connect=min(5, self.settings.request_timeout),
                        read=min(5, self.settings.request_timeout))) as response:
                    response.raise_for_status()
                    total = response.headers.get('Content-Length', '')
                    self.file_progress(kind, ts, total_bytes=int(total) if total.isdigit() else None)
                    # Check cancellation on each received chunk, not after accumulating 1MB.
                    for block in response.iter_bytes():
                        if self.cancel.is_set():
                            raise InterruptedError("同步已停止，批次将在后续重试")
                        if time.monotonic()-download_started > self.settings.request_timeout:
                            raise httpx.ReadTimeout('单文件下载超过总时限，保留进度后自动重试')
                        size += len(block)
                        if size > self.settings.max_download_mb*1024*1024:
                            raise ValueError("下载文件超过单文件大小上限")
                        f.write(block)
                        self.file_progress(kind, ts, downloaded_bytes=size)
            parser = parse_gkg if kind == "gkg" else parse_events
            parse_started = time.monotonic()
            with self.stage(kind, ts, 'parse_pipeline'):
                if self.parser_pool is None:
                    self.file_progress(kind, ts, stage='parsing')
                    parsed = parser(path, ts, self.settings.max_uncompressed_mb, cancel=self.cancel,
                                    progress=lambda rows: self.file_progress(kind, ts, parsed_rows=rows))
                    parsed.parse_seconds = time.monotonic()-parse_started
                else:
                    try:
                        future = self.parser_pool.submit(_parse_file, kind, path, ts, self.settings.max_uncompressed_mb)
                        # Keep the ZIP until the parser finishes, including during cancellation.
                        while not future.done():
                            wait((future,), timeout=.05)
                            self.drain_parser_progress()
                            if self.cancel.is_set():
                                self.parser_cancel.set()
                                future.cancel()
                        if future.cancelled():
                            raise InterruptedError('同步已停止')
                        parsed = future.result()
                    except BrokenProcessPool:
                        self.parser_broken = True
                        raise
            self.file_progress(kind, ts, parsed_rows=parsed.rows)
            self.record_timing('parse_compute', parsed.parse_seconds)
            self.record_timing('parse_queue_transfer', max(0., time.monotonic()-parse_started-parsed.parse_seconds))
            if parsed.rows == 0:
                raise ValueError("文件没有可解析数据行，不标记为完成")
            with self.stage(kind, ts, 'commit_wait'):
                self.write_lock.acquire()
            try:
                if self.cancel.is_set():
                    raise InterruptedError("同步已停止")
                with self.stage(kind, ts, 'commit'):
                    self.check_disk()
                    return self.store.apply(kind, ts, parsed.tables, parsed.rows, parsed.skipped)
            finally:
                self.write_lock.release()
        finally:
            if path is not None:
                path.unlink(missing_ok=True)

    def run_once(self, schedule=True, limit=None, clear_cancel=True, ranges=None, exclude_range=None, descending=False):
        if not self.busy.acquire(blocking=False):
            return {"skipped": "已有同步或快照任务运行中"}
        done = failed = 0
        started = time.monotonic()
        try:
            with self.active_lock:
                self.timings = {}
                self.cycle = {'total': 0, 'completed': 0, 'succeeded': 0, 'failed': 0, 'cancelled': 0}
            if clear_cancel:
                self.cancel.clear()
            self.set_phase('scheduling')
            if time.monotonic()-self.last_prune >= 3600:
                self.set_phase('pruning')
                self.store.prune(self.settings.hour_retention_days, self.settings.day_retention_days)
                self.last_prune = time.monotonic()
                self.set_phase('scheduling')
            if schedule:
                self.schedule()
            self.check_disk()
            pending = self.store.pending(limit or max(self.settings.batch_files, self.settings.download_workers), ranges=ranges,
                                         exclude_range=exclude_range, descending=descending)
            scheduling_seconds = time.monotonic()-started
            with self.active_lock:
                self.cycle['total'] = len(pending)
            if pending:
                LOG.info('开始处理本轮 %s 个文件，并发文件=%s 解析进程=%s',
                         len(pending), self.settings.download_workers, self.settings.parser_workers)
            self.set_phase('processing')
            if pending and not self.cancel.is_set():
                self.ensure_parser_pool()
                rows = iter(pending)
                # Submit only one task per download worker, never the entire backfill.
                with ThreadPoolExecutor(self.settings.download_workers, thread_name_prefix='gdelt-file') as executor:
                    inflight = {}
                    def submit_next():
                        if self.cancel.is_set() or self.reconfigure.is_set():
                            return
                        row = next(rows, None)
                        if row is not None:
                            future = executor.submit(self.tracked_process, row['kind'], row['file_ts'])
                            inflight[future] = row
                    for _ in range(self.settings.download_workers):
                        submit_next()
                    while inflight:
                        completed, _ = wait(inflight, timeout=.1, return_when=FIRST_COMPLETED)
                        self.drain_parser_progress()
                        self.warn_stalled()
                        if self.cancel.is_set() and self.parser_cancel is not None:
                            self.parser_cancel.set()
                        for future in completed:
                            row = inflight.pop(future)
                            outcome, error = 'done', None
                            try:
                                done += int(future.result())
                            except InterruptedError:
                                outcome = 'cancelled'
                            except Exception as exc:
                                outcome, error = ('cancelled' if self.cancel.is_set() else 'failed'), str(exc)
                                if not self.cancel.is_set():
                                    failed += 1
                                    self.store.mark_failed(row['kind'], row['file_ts'], exc)
                                    LOG.warning('批次失败 %s %s: %s', row['kind'], row['file_ts'], exc)
                            with self.active_lock:
                                self.cycle['completed'] += 1
                                self.cycle[{'done':'succeeded','failed':'failed','cancelled':'cancelled'}[outcome]] += 1
                                self.last_completed = time.monotonic()
                                self.recent_files.appendleft({'kind':row['kind'], 'file_ts':row['file_ts'],
                                    'filename':file_url(row['kind'], row['file_ts']).rsplit('/', 1)[1],
                                    'outcome':outcome, 'error':error, 'finished_at':time.time()})
                            submit_next()
            if done:
                self.store.set_state("last_ingest_at", utcnow().isoformat())
                self.store.set_state("snapshot_dirty", True)
            publish_started = time.monotonic()
            if self.on_update:
                self.on_update()
            publishing_seconds = time.monotonic()-publish_started
            elapsed = time.monotonic()-started
            result = {"processed": done, "failed": failed, "queued": len(pending),
                      "seconds": round(elapsed, 2), "files_per_second": round(done/max(elapsed, .001), 2),
                      "scheduling_seconds": round(scheduling_seconds, 3),
                      "publishing_seconds": round(publishing_seconds, 3),
                      "stage_seconds": {key: round(value, 3) for key, value in self.timings.items()}}
            self.store.set_state("last_run", result)
            if pending:
                LOG.info('本轮结束：成功=%s 失败=%s 耗时=%.2fs', done, failed, elapsed)
            self.store.set_state("last_error", None)
            return result
        except Exception as exc:
            self.store.set_state("last_error", str(exc))
            raise
        finally:
            if self.parser_broken:
                self.parser_pool.shutdown(wait=True, cancel_futures=True)
                self.parser_pool = self.parser_cancel = None
                self.close_progress_queue()
                self.parser_broken = False
            self.set_phase('idle')
            self.current_file = None
            self.busy.release()

    def close_progress_queue(self):
        with self.progress_lock:
            if self.parser_progress is not None:
                self.parser_progress.close()
                self.parser_progress.join_thread()
                self.parser_progress = None

    def close(self):
        self.stop()
        if self.parser_pool is not None:
            self.parser_pool.shutdown(wait=True, cancel_futures=True)
            self.parser_pool = None
        self.close_progress_queue()
        if self.owns_client:
            self.client.close()
