"""A bounded real-data probe in an isolated temporary directory."""
import re
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
import httpx

from .config import Settings
from .ingest import BASE, Ingestor
from .metrics import Metrics
from .snapshot import SnapshotFiles, build_snapshot, encode_json
from .store import SLOT, Store


def real_selftest(settings):
    started = time.monotonic()
    with httpx.Client(timeout=settings.request_timeout, follow_redirects=True) as client:
        response = client.get(BASE+"/lastupdate.txt")
        response.raise_for_status()
        match = re.search(r"/(\d{14})\.export\.CSV\.zip", response.text)
        if not match:
            raise ValueError("索引中没有Events批次")
        latest = int(datetime.strptime(match[1], "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc).timestamp())
        with tempfile.TemporaryDirectory(prefix="gdelt-selftest-") as folder:
            folder = Path(folder)
            probe_settings = settings.model_copy(update={"data_dir": folder, "min_free_gb": .1})
            store = Store(folder/"probe.db")
            store.initialize()
            ingest = Ingestor(store, probe_settings, client)
            # Probe at most two historical slots, starting 30 minutes behind index.
            sources = []
            for kind in ("events", "gkg"):
                last_error = None
                for offset in (2, 3):
                    ts = latest-offset*SLOT
                    try:
                        t = time.monotonic()
                        ingest.process(kind, ts)
                        with store.connect() as db:
                            row = dict(db.execute("SELECT * FROM ingest_file WHERE kind=? AND file_ts=?",(kind,ts)).fetchone())
                        sources.append({"source": kind, "file_ts": ts, "rows": row["row_count"],
                                        "skipped_rows": row["skipped_rows"], "seconds": round(time.monotonic()-t,2)})
                        break
                    except httpx.HTTPStatusError as exc:
                        last_error = exc
                        if exc.response.status_code != 404:
                            raise
                else:
                    raise last_error
            metrics = Metrics(store)
            t = time.monotonic()
            snapshot = build_snapshot(store, settings.snapshot_days)
            manifest = SnapshotFiles(folder/"snapshots").publish(snapshot)
            return {"ok": True, "sources": sources,
                    "country_risk_countries": len(metrics.country_risk(7)["countries"]),
                    "enterprise_risk_countries": len(metrics.enterprise_risk(7)["countries"]),
                    "database_bytes": store.stats()["database_bytes"],
                    "snapshot_bytes": manifest["bytes"], "snapshot_uncompressed_bytes": len(encode_json(snapshot)),
                    "snapshot_seconds": round(time.monotonic()-t,2),
                    "raw_files_remaining": len(list((folder/"tmp").glob("*"))),
                    "total_seconds": round(time.monotonic()-started,2),
                    "note": "小样本真实文件冒烟测试，不代表长期满量负载；临时数据退出后全部清理"}
