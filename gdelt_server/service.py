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
        self.store = Store(settings.data_dir/"gdelt.db", settings.max_file_attempts)
        self.store.initialize()
        self.queries = QueryCache(self.store)
        self.runtime_error = None
        settings.day_retention_days = max(settings.day_retention_days,
                                          self.store.get_state("backfill_retention_days", 0), 1185)
        # Upgrade existing installations without requiring manual config edits.
        settings.snapshot_days = sorted(set(settings.snapshot_days) | {1, 1095})
        settings.hour_retention_days = max(9, settings.hour_retention_days)
        saved = self.store.get_state('concurrency_settings')
        if saved:
            from .config import Settings
            validated = Settings.model_validate({**settings.model_dump(), **saved})
            settings.download_workers = validated.download_workers
            settings.parser_workers = validated.parser_workers
        self.pending_concurrency = None
        self.ingestor = Ingestor(self.store, settings)
        self.ingestor.cleanup_abandoned()
        self.snapshots = SnapshotFiles(settings.data_dir/"snapshots")
        self.enabled = self.store.get_state("monitor_enabled", settings.monitor_enabled)
        self.shutdown = threading.Event()
        self.wake = threading.Event()
        self.control = threading.Lock()
        self.job = self.store.get_state("active_backfill") or self.store.get_state('active_repair')
        self.paused_backfill = self.store.get_state('paused_backfill')
        if self.job and self.job['action'] == 'backfill' and 'end_ts' not in self.job:
            horizon = int(utcnow().timestamp())//SLOT*SLOT
            self.job.setdefault('start_ts', horizon-self.job['hours']*3600)
            self.job['end_ts'] = horizon
            self.store.set_state('active_backfill', self.job)
        if self.job and self.job.get('paused'):
            if self.job['action'] == 'backfill':
                self.paused_backfill = self.job
                self.store.set_state('paused_backfill', self.job)
                self.store.set_state('active_backfill', None)
            else:
                self.store.set_state('active_repair', None)
            self.job = None
        self.last_job = None
        self.last_export = 0.
        self.next_run_at = None
        self.thread = threading.Thread(target=self.loop, name="gdelt-worker", daemon=True)
        self.ingestor.on_update = self.maybe_export

    def start(self):
        self.thread.start()
        self.wake.set()

    def configure_concurrency(self, download_workers, parser_workers):
        from .config import Settings
        desired = {'download_workers': download_workers, 'parser_workers': parser_workers}
        Settings.model_validate({**self.settings.model_dump(), **desired})
        with self.control:
            self.store.set_state('concurrency_settings', desired)
            self.pending_concurrency = desired
            self.ingestor.reconfigure.set()
            self.wake.set()
        return {'accepted': True, 'requested': desired, 'reason': '并发设置已保存，在途文件收尾后生效，无需重启'}

    def apply_pending_concurrency(self):
        with self.ingestor.busy:
            with self.control:
                desired = self.pending_concurrency
            if desired is not None:
                # Pool shutdown must not hold the control lock needed by pause buttons.
                self.ingestor.set_concurrency(**desired)
                with self.control:
                    if self.pending_concurrency is desired:
                        self.pending_concurrency = None
                    else:
                        self.ingestor.reconfigure.set()

    def pause_all(self):
        with self.control:
            self.ingestor.stop()
            self.enabled = False
            self.store.set_state('monitor_enabled', False)
            if self.job and self.job['action'] == 'backfill':
                self.job['paused'] = True
                self.paused_backfill = self.job
                self.store.set_state('paused_backfill', self.job)
                self.store.set_state('active_backfill', None)
            elif self.job and self.job['action'] == 'repair':
                self.job['paused'] = True
                self.store.set_state('active_repair', None)
            self.wake.set()
        return {'accepted': True, 'reason': '全部采集正在暂停，已完成进度已保存；不会再提交新文件'}

    def request(self, action, hours=None, start_date=None):
        with self.control:
            if action == 'repair':
                if self.job:
                    return {'accepted': False, 'reason': '已有任务在运行；请等待结束，或先暂停全部并等待在途文件收尾后，再点击查缺补漏'}
                # Queue during a monitoring batch; reset budgets only after its files finish.
                repair_job = {'action': 'repair'}
                self.store.set_state('active_repair', repair_job)
                self.job = repair_job
                self.ingestor.cancel.clear()
                self.wake.set()
                return {'accepted': True, 'job': self.job, 'reason': f'查缺补漏已提交，在途文件收尾后为失败文件开启新一轮，每个文件最多尝试{self.settings.max_file_attempts}次'}
            if self.job or self.ingestor.busy.locked():
                if action == 'retry' and self.job and self.job['action'] == 'backfill' and not self.job.get('paused'):
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
            self.ingestor.stop()
            self.job['paused'] = True
            self.paused_backfill = self.job
            self.store.set_state('paused_backfill', self.job)
            self.store.set_state('active_backfill', None)
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
        if job and job['action'] == 'repair':
            # Manual repair may retry failures inside paused history, but never resumes
            # that history's unprocessed pending files.
            return {'failed_only': True}
        if job and job['action'] == 'backfill':
            opts = {'descending': True}
            if not self.enabled:
                opts['ranges'] = [(job['start_ts'], job['end_ts'])]
            return opts
        if self.paused_backfill:
            opts = {'exclude_range': (self.paused_backfill['start_ts'], self.paused_backfill['end_ts'])}
            return opts
        # Monitoring drains all outstanding batches, including older failures.
        # Only an explicitly paused historical range is excluded.
        return {}

    def monitor(self, enabled):
        with self.control:
            self.enabled = enabled
            self.store.set_state("monitor_enabled", enabled)
            if not enabled and not (self.job and self.job['action'] in ('backfill','repair')):
                self.ingestor.stop()
            elif enabled and not (self.job and self.job.get('paused')):
                self.ingestor.cancel.clear()
            self.wake.set()

    def export_locked(self):
        previous = self.ingestor.phase
        self.ingestor.set_phase('exporting')
        started = time.monotonic()
        LOG.info('开始计算并发布结果快照')
        try:
            self.ingestor.check_disk()
            manifest = self.snapshots.publish(build_snapshot(self.store, self.settings.snapshot_days))
            LOG.info('结果快照发布完成，耗时=%.2fs', time.monotonic()-started)
        finally:
            self.ingestor.set_phase(previous)
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
            self.export_locked()

    def loop(self):
        delay = 0
        while not self.shutdown.is_set():
            self.wake.wait(delay)
            self.wake.clear()
            self.next_run_at = None
            if self.shutdown.is_set():
                break
            with self.control:
                job = self.job
                enabled = self.enabled
            result = None
            try:
                self.apply_pending_concurrency()
                if job and job["action"] == "export":
                    with self.ingestor.busy:
                        result = self.export_locked()
                elif enabled or job:
                    if job and job['action'] == 'repair' and not job.get('seeded'):
                        with self.control:
                            if not job.get('paused') and not self.ingestor.cancel.is_set():
                                job['reopened_files'] = self.store.reopen_failed(job)
                                job['seeded'] = True
                    if job and job['action'] == 'retry' and not job.get('seeded'):
                        job['scheduled_files'] = self.ingestor.repair_gaps()
                        job['seeded'] = True
                    if job and job["action"] == "backfill" and not job.get("seeded") and not job.get('paused'):
                        self.ingestor.set_phase('seeding')
                        try:
                            job["scheduled_files"] = self.ingestor.backfill(job["hours"], job.get("start_ts"), job.get('end_ts'))
                        finally:
                            self.ingestor.set_phase('idle')
                        job["seeded"] = True
                        self.store.set_state("active_backfill", job)
                    with self.control:
                        should_run = self.enabled or self.job is not None
                    if should_run:
                        result = self.ingestor.run_once(schedule=enabled or bool(job and job["action"] == "sync"),
                                                       clear_cancel=False, **self.work_options(job))
                        if job and job['action'] == 'repair' and enabled and not self.ingestor.cancel.is_set():
                            # Keep live monitoring moving while manual failures back off.
                            self.ingestor.run_once(schedule=True, clear_cancel=False, **self.work_options())
                if job:
                    scope = {'ranges': [(job['start_ts'], job['end_ts'])]} if job['action'] == 'backfill' else {k:v for k,v in self.work_options(job).items() if k != 'descending'}
                    more = (job["action"] in ('backfill', 'retry', 'repair') and self.store.has_unfinished(**scope)
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
                                self.store.set_state('last_backfill', self.last_job)
                            if job['action'] == 'repair':
                                self.store.set_state('active_repair', None)
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
            self.next_run_at = time.time()+delay if self.enabled or self.job else None

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
        delay = .1 if ((self.enabled or self.job) and ready) else self.settings.poll_seconds
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
            delay = min(delay, .1)
        return delay

    def close(self):
        self.shutdown.set()
        self.ingestor.stop()
        self.wake.set()
        self.thread.join(timeout=self.settings.request_timeout+10)
        if self.thread.is_alive():
            raise RuntimeError("下载线程未结束；不能在同一数据目录启动另一个服务")
        self.ingestor.close()

    def status(self):
        job = dict(self.job or self.paused_backfill or
                   (self.last_job if self.last_job and self.last_job.get('action') == 'backfill' else {}) or self.store.get_state('last_backfill', {}))
        progress = None
        if job.get('action') == 'backfill':
            start, end = job['start_ts'], job['end_ts']
            total = max(0, (end-start)//SLOT*2)
            with self.store.connect() as db:
                counts = {r['status']:r['n'] for r in db.execute(
                    'SELECT status,COUNT(*) n FROM ingest_file WHERE file_ts>=? AND file_ts<? GROUP BY status', (start, end))}
            done = counts.get('done', 0)
            progress = {'start_ts':start, 'end_ts':end, 'total':total, 'done':done,
                        'pending':counts.get('pending', 0), 'failed':counts.get('failed', 0),
                        'exhausted':counts.get('exhausted', 0),
                        'unseeded':max(0, total-sum(counts.values())),
                        'percent':round(done/total*100, 2) if total else 100,
                        'paused':bool(job.get('paused'))}
            progress['work_finished'] = not (progress['pending'] or progress['failed'] or progress['unseeded'])
        return {"monitor_enabled": self.enabled, "running": self.enabled and self.thread.is_alive(),
                "progress": progress, "next_run_at": self.next_run_at,
                "phase": self.ingestor.phase, "busy": self.ingestor.busy.locked(),
                "current_file": self.ingestor.current_file, "job": self.job, "last_job": self.last_job,
                "concurrency": self.ingestor.concurrency_status(),
                "pending_concurrency": self.pending_concurrency,
                "last_run": self.store.get_state('last_run'),
                "paused_backfill": self.paused_backfill,
                "last_error": self.store.get_state("last_error") or self.runtime_error,
                "worker_alive": self.thread.is_alive(),
                "last_ingest_at": self.store.get_state("last_ingest_at"),
                "data_version": self.store.get_state("data_version", 0),
                "parameter_version": self.store.get_state("parameter_version", 0),
                "storage": self.store.stats(), "snapshot": self.snapshots.manifest(),
                "consumer_receipt": self.store.get_state('consumer_receipt'),
                "unrecoverable_gap": self.store.get_state("unrecoverable_gap"),
                "poll_seconds": self.settings.poll_seconds,
                "max_file_attempts": self.settings.max_file_attempts,
                "day_retention_days": self.settings.day_retention_days,
                "hour_retention_days": self.settings.hour_retention_days}
