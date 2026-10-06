from __future__ import annotations

import logging
import threading
import time

from .ingest import Ingestor
from .snapshot import SnapshotFiles, build_snapshot
from .store import Store

LOG = logging.getLogger(__name__)


class Service:
    def __init__(self, settings):
        self.settings = settings
        self.store = Store(settings.data_dir/"gdelt.db")
        self.store.initialize()
        self.ingestor = Ingestor(self.store, settings)
        self.ingestor.cleanup_abandoned()
        self.snapshots = SnapshotFiles(settings.data_dir/"snapshots")
        self.enabled = self.store.get_state("monitor_enabled", settings.monitor_enabled)
        self.shutdown = threading.Event()
        self.wake = threading.Event()
        self.control = threading.Lock()
        self.job = None
        self.last_job = None
        self.last_export = 0.
        self.thread = threading.Thread(target=self.loop, name="gdelt-worker", daemon=True)
        self.ingestor.on_update = self.maybe_export

    def start(self):
        self.thread.start()
        self.wake.set()

    def request(self, action, hours=None):
        with self.control:
            if self.job or self.ingestor.busy.locked():
                return {"accepted": False, "reason": "已有任务在运行"}
            if action == "backfill":
                if hours is None or not 1 <= hours <= self.settings.day_retention_days*24:
                    raise ValueError("回填小时数超出保留期")
            self.job = {"action": action, "hours": hours}
            self.ingestor.cancel.clear()
        self.wake.set()
        return {"accepted": True, "job": self.job}

    def monitor(self, enabled):
        with self.control:
            self.enabled = enabled
            self.store.set_state("monitor_enabled", enabled)
            if not enabled:
                self.ingestor.cancel.set()
            else:
                self.ingestor.cancel.clear()
            self.wake.set()

    def export_locked(self):
        self.ingestor.check_disk()
        manifest = self.snapshots.publish(build_snapshot(self.store, self.settings.snapshot_days))
        self.store.set_state("snapshot_dirty", False)
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
                    if job and job["action"] == "backfill" and not job.get("seeded"):
                        job["scheduled_files"] = self.ingestor.backfill(job["hours"])
                        job["seeded"] = True
                    with self.control:
                        should_run = self.enabled or self.job is not None
                    if should_run:
                        result = self.ingestor.run_once(schedule=enabled or bool(job and job["action"] == "sync"),
                                                       clear_cancel=False)
                if job:
                    more = (job["action"] == "backfill" and self.store.pending(1)
                            and not self.ingestor.cancel.is_set())
                    if not more:
                        # Force a final publish, even if the last partial batch was throttled.
                        if self.store.get_state("snapshot_dirty", False):
                            with self.ingestor.busy:
                                self.export_locked()
                        with self.control:
                            self.last_job = {**job, "result": result, "finished_at": time.time()}
                            self.job = None
            except Exception as exc:
                LOG.exception("后台任务失败")
                self.store.set_state("last_error", str(exc))
                if job:
                    with self.control:
                        self.last_job = {**job, "error": str(exc)}
                        self.job = None
            delay = 2 if ((self.enabled or self.job) and self.store.pending(1)) else self.settings.poll_seconds
            if self.enabled and not self.store.pending(1):
                # Re-publish expired windows/metadata even when no new file succeeds.
                if time.monotonic()-self.last_export >= self.settings.poll_seconds and self.store.get_state("data_version", 0):
                    try:
                        with self.ingestor.busy:
                            self.export_locked()
                    except Exception as exc:
                        self.store.set_state("last_error", str(exc))
            # If a long outage needs several scheduling passes, advance the durable cursor.
            horizon = int(time.time())//900*900
            if self.enabled and self.store.get_state("scheduled_until", horizon) < horizon:
                delay = 2

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
                "last_error": self.store.get_state("last_error"),
                "last_ingest_at": self.store.get_state("last_ingest_at"),
                "data_version": self.store.get_state("data_version", 0),
                "parameter_version": self.store.get_state("parameter_version", 0),
                "storage": self.store.stats(), "snapshot": self.snapshots.manifest(),
                "unrecoverable_gap": self.store.get_state("unrecoverable_gap"),
                "poll_seconds": self.settings.poll_seconds}
