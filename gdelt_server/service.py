from __future__ import annotations

import logging
from datetime import date, datetime, timezone
import math
import threading
import time

from .ingest import Ingestor
from .snapshot import SnapshotFiles, build_snapshot
from .store import Store, DAY, SLOT, utcnow
from .query import QueryCache

LOG = logging.getLogger(__name__)


class Service:
    def __init__(self, settings):
        self.settings = settings = settings.model_copy(deep=True)
        self.store = Store(settings.data_dir/"gdelt.db")
        self.store.initialize()
        self.queries = QueryCache(self.store)
        self.runtime_error = None
        settings.day_retention_days = max(settings.day_retention_days,
                                          self.store.get_state("backfill_retention_days", 0))
        self.ingestor = Ingestor(self.store, settings)
        self.ingestor.cleanup_abandoned()
        self.snapshots = SnapshotFiles(settings.data_dir/"snapshots")
        self.enabled = self.store.get_state("monitor_enabled", settings.monitor_enabled)
        self.shutdown = threading.Event()
        self.wake = threading.Event()
        self.control = threading.Lock()
        self.job = self.store.get_state("active_backfill")
        self.paused_backfill = self.store.get_state('paused_backfill')
        if self.job and 'end_ts' not in self.job:
            horizon = int(utcnow().timestamp())//SLOT*SLOT
            self.job.setdefault('start_ts', horizon-self.job['hours']*3600)
            self.job['end_ts'] = horizon
            self.store.set_state('active_backfill', self.job)
        if self.job and self.job.get('paused'):
            self.paused_backfill = self.job
            self.store.set_state('paused_backfill', self.job)
            self.store.set_state('active_backfill', None)
            self.job = None
        self.last_job = None
        self.last_export = 0.
        self.thread = threading.Thread(target=self.loop, name="gdelt-worker", daemon=True)
        self.ingestor.on_update = self.maybe_export

    def start(self):
        self.thread.start()
        self.wake.set()

    def request(self, action, hours=None, start_date=None):
        with self.control:
            if self.job or self.ingestor.busy.locked():
                if action == 'retry' and self.job and self.job['action'] == 'backfill' and not self.ingestor.busy.locked():
                    self.store.retry_failed()
                    self.wake.set()
                    return {'accepted': True, 'job': self.job}
                return {"accepted": False, "reason": "已有任务在运行"}
            if action == 'retry':
                self.store.retry_failed()
            start_ts = None
            end_ts = None
            if action == "backfill" and start_date is not None:
                horizon = int(utcnow().timestamp()) // SLOT * SLOT
                start_ts = int(datetime.combine(start_date, datetime.min.time(), timezone.utc).timestamp())
                if start_date < date(2015, 2, 19) or start_ts >= horizon:
                    raise ValueError("起始日期须在2015-02-19之后且早于最新完整时段（UTC）")
                if horizon-start_ts > 3650*DAY:
                    raise ValueError("回填范围最多支持3650天")
                hours = math.ceil((horizon-start_ts)/3600)
                with self.store.connect() as db:
                    earliest = db.execute("SELECT MIN(file_ts) FROM ingest_file WHERE status='done'").fetchone()[0]
                end_ts = min(horizon, earliest+SLOT) if earliest is not None else horizon
                if self.paused_backfill:
                    end_ts = max(end_ts, self.paused_backfill.get('end_ts', end_ts))
                if start_ts >= end_ts:
                    return {'accepted': False, 'reason': '已有数据已早于目标日期，无需向前回填；缺口请使用重试功能检查'}
                # Persist before enqueueing so old configurations cannot prune this history on restart.
                self.store.set_state("backfill_retention_days", 3650)
                self.settings.day_retention_days = 3650
            if action == "backfill":
                if hours is None or not 1 <= hours <= self.settings.day_retention_days*24:
                    raise ValueError("回填小时数超出保留期")
            self.job = {"action": action, "hours": hours}
            if start_ts is not None:
                self.job.update(start_ts=start_ts, end_ts=end_ts, start_date=start_date.isoformat())
            if action == "backfill":
                if start_ts is None:
                    horizon = int(utcnow().timestamp())//SLOT*SLOT
                    self.job.update(start_ts=horizon-hours*3600, end_ts=horizon)
                self.paused_backfill = None
                self.store.set_state('paused_backfill', None)
                self.store.set_state("active_backfill", self.job)
            self.ingestor.cancel.clear()
        self.wake.set()
        return {"accepted": True, "job": self.job}

    def pause_backfill(self):
        with self.control:
            if not self.job or self.job['action'] != 'backfill':
                return {'accepted': False, 'reason': '当前没有正在进行的回填'}
            self.job['paused'] = True
            self.paused_backfill = self.job
            self.store.set_state('paused_backfill', self.job)
            self.store.set_state('active_backfill', None)
            self.ingestor.cancel.set()
            self.wake.set()
        return {'accepted': True, 'reason': '回填正在暂停，已完成进度已保存'}

    def resume_backfill(self):
        with self.control:
            if self.job or self.ingestor.busy.locked():
                return {'accepted': False, 'reason': '请等待当前批次停止后继续'}
            if not self.paused_backfill:
                return {'accepted': False, 'reason': '没有可继续的回填'}
            self.job = dict(self.paused_backfill)
            self.job.pop('paused', None)
            self.store.set_state('active_backfill', self.job)
            self.store.set_state('paused_backfill', None)
            self.paused_backfill = None
            self.ingestor.cancel.clear()
            self.wake.set()
        return {'accepted': True, 'job': self.job}

    def work_options(self, job=None):
        horizon = int(utcnow().timestamp())//SLOT*SLOT
        if job and job['action'] == 'backfill':
            ranges = [(job['start_ts'], job['end_ts'])]
            if self.enabled:
                ranges.append((self.store.get_state('incremental_start', horizon-self.settings.initial_hours*3600), None))
            return {'ranges': ranges, 'descending': True}
        if self.paused_backfill:
            opts = {'exclude_range': (self.paused_backfill['start_ts'], self.paused_backfill['end_ts'])}
            if self.enabled:
                opts['ranges'] = [(self.store.get_state('incremental_start', horizon-self.settings.initial_hours*3600), None)]
            return opts
        if self.enabled and not (job and job['action'] == 'retry'):
            return {'ranges': [(self.store.get_state('incremental_start', horizon-self.settings.initial_hours*3600), None)]}
        return {}

    def monitor(self, enabled):
        with self.control:
            self.enabled = enabled
            self.store.set_state("monitor_enabled", enabled)
            if not enabled and not (self.job and self.job['action'] == 'backfill'):
                self.ingestor.cancel.set()
            elif enabled and not (self.job and self.job.get('paused')):
                self.ingestor.cancel.clear()
            self.wake.set()

    def export_locked(self):
        self.ingestor.check_disk()
        manifest = self.snapshots.publish(build_snapshot(self.store, self.settings.snapshot_days))
        self.store.set_state("snapshot_dirty", False)
        self.store.set_state('last_error', None)
        self.runtime_error = None
        self.last_export = time.monotonic()
        return manifest

    def maybe_export(self):
        if not self.store.get_state("data_version", 0):
            return
        backlog = bool(self.store.pending(1))
        if (self.store.get_state("snapshot_dirty", False) and
                (not backlog or time.monotonic()-self.last_export >= self.settings.poll_seconds)):
            self.ingestor.phase = "exporting"
            self.export_locked()

    def loop(self):
        delay = 0
        while not self.shutdown.is_set():
            self.wake.wait(delay)
            self.wake.clear()
            if self.shutdown.is_set():
                break
            with self.control:
                job = self.job
                enabled = self.enabled
            result = None
            try:
                if job and job["action"] == "export":
                    with self.ingestor.busy:
                        result = self.export_locked()
                elif enabled or job:
                    if job and job["action"] == "backfill" and not job.get("seeded") and not job.get('paused'):
                        job["scheduled_files"] = self.ingestor.backfill(job["hours"], job.get("start_ts"), job.get('end_ts'))
                        job["seeded"] = True
                        self.store.set_state("active_backfill", job)
                    with self.control:
                        should_run = self.enabled or self.job is not None
                    if should_run:
                        result = self.ingestor.run_once(schedule=enabled or bool(job and job["action"] == "sync"),
                                                       clear_cancel=False, **self.work_options(job))
                if job:
                    scope = {'ranges': [(job['start_ts'], job['end_ts'])]} if job['action'] == 'backfill' else {k:v for k,v in self.work_options(job).items() if k != 'descending'}
                    more = (job["action"] in ('backfill', 'retry') and self.store.has_unfinished(**scope)
                            and not self.ingestor.cancel.is_set())
                    if not more and not self.shutdown.is_set():
                        # Force a final publish, even if the last partial batch was throttled.
                        if self.store.get_state("snapshot_dirty", False):
                            with self.ingestor.busy:
                                self.export_locked()
                        with self.control:
                            self.last_job = {**job, "result": result, "finished_at": time.time()}
                            if job["action"] == "backfill":
                                if job.get('paused'):
                                    self.paused_backfill = job
                                    self.store.set_state('paused_backfill', job)
                                self.store.set_state("active_backfill", None)
                            self.job = None
                            self.ingestor.cancel.clear()
            except Exception as exc:
                LOG.exception("后台任务失败")
                self.record_error(exc)
                # A storage/export failure must not discard durable history work.
            else:
                if result is not None:
                    self.runtime_error = None
            try:
                delay = self.next_delay()
            except Exception as exc:
                LOG.exception('后台状态读取失败')
                self.record_error(exc)
                delay = 10

    def record_error(self, exc):
        code = getattr(exc, 'sqlite_errorname', '')
        self.runtime_error = f'{code}: {exc}' if code else str(exc)
        try:
            self.store.set_state('last_error', self.runtime_error)
        except Exception:
            LOG.exception('无法写入错误状态；保留在内存和服务日志中')

    def next_delay(self):
        if self.runtime_error:
            return 10
        opts = self.work_options(self.job)
        scope = {k:v for k,v in opts.items() if k != 'descending'}
        ready = self.store.pending(1, **opts)
        delay = 2 if ((self.enabled or self.job) and ready) else self.settings.poll_seconds
        if (self.enabled or self.job) and self.store.has_unfinished(**scope):
            delay = min(delay, self.store.retry_delay(**scope))
        if self.enabled and not ready:
            if time.monotonic()-self.last_export >= self.settings.poll_seconds and self.store.get_state("data_version", 0):
                try:
                    with self.ingestor.busy:
                        self.export_locked()
                except Exception as exc:
                    self.record_error(exc)
        horizon = int(time.time())//900*900
        if self.enabled and self.store.get_state("scheduled_until", horizon) < horizon:
            delay = 2
        return delay

    def close(self):
        self.shutdown.set()
        self.ingestor.cancel.set()
        self.wake.set()
        self.thread.join(timeout=self.settings.request_timeout+10)
        if self.thread.is_alive():
            raise RuntimeError("下载线程未结束；不能在同一数据目录启动另一个服务")
        self.ingestor.close()

    def status(self):
        return {"monitor_enabled": self.enabled, "running": self.enabled and self.thread.is_alive(),
                "phase": self.ingestor.phase, "busy": self.ingestor.busy.locked(),
                "current_file": self.ingestor.current_file, "job": self.job, "last_job": self.last_job,
                "paused_backfill": self.paused_backfill,
                "last_error": self.store.get_state("last_error") or self.runtime_error,
                "worker_alive": self.thread.is_alive(),
                "last_ingest_at": self.store.get_state("last_ingest_at"),
                "data_version": self.store.get_state("data_version", 0),
                "parameter_version": self.store.get_state("parameter_version", 0),
                "storage": self.store.stats(), "snapshot": self.snapshots.manifest(),
                "unrecoverable_gap": self.store.get_state("unrecoverable_gap"),
                "poll_seconds": self.settings.poll_seconds,
                "day_retention_days": self.settings.day_retention_days,
                "hour_retention_days": self.settings.hour_retention_days}
