"""Optional cloud-side receiver; stores two compact result versions, no database.

The existing DSI app can mount create_receiver(settings) or register an adapter
for these routes. This module never downloads GDELT or performs metric calculations.
"""
from __future__ import annotations

import gzip
import hashlib
import io
import json
import threading
import zlib

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request

from .app import authorize
from .config import Settings
from .snapshot import SnapshotFiles, snapshot_view, validate_snapshot


def create_receiver(settings: Settings):
    files = SnapshotFiles(settings.data_dir/"snapshots")
    lock = threading.Lock()
    cache = {"id": None, "data": None}
    app = FastAPI(title="GDELT 云端结果接收器")

    def auth(request: Request, authorization: str = Header(default="")):
        authorize(settings, request, authorization)

    protected = [Depends(auth)]

    def data():
        with lock:
            manifest = files.manifest()
            if not manifest:
                raise HTTPException(503, "尚未收到结果快照")
            if cache["id"] != manifest["snapshot_id"]:
                cache.update(id=manifest["snapshot_id"], data=files.read())
            return cache["data"]

    @app.put("/api/snapshots", dependencies=protected)
    async def receive(request: Request, x_sha256: str = Header(default="")):
        payload = bytearray()
        async for chunk in request.stream():
            payload.extend(chunk)
            if len(payload) > 64*1024*1024:
                raise HTTPException(413, "快照压缩大小超过64MB")
        if not x_sha256 or hashlib.sha256(payload).hexdigest() != x_sha256:
            raise HTTPException(400, "SHA256校验失败")
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(payload)) as f:
                unpacked = f.read(256*1024*1024+1)
            if len(unpacked) > 256*1024*1024:
                raise HTTPException(413, "快照解压大小超过256MB")
            incoming = json.loads(unpacked, parse_constant=lambda v: (_ for _ in ()).throw(ValueError(v)))
            validate_snapshot(incoming)
            from datetime import datetime
            incoming_at = datetime.fromisoformat(incoming["created_at"])
            if incoming_at.tzinfo is None:
                raise ValueError("快照时间必须包含时区")
            with lock:
                previous = files.manifest()
                if previous:
                    if incoming["snapshot_id"] == previous["snapshot_id"]:
                        if hashlib.sha256(payload).hexdigest() != previous["sha256"]:
                            raise HTTPException(409, "相同快照ID对应不同内容")
                        return {"ok": True, "duplicate": True}
                    if incoming_at <= datetime.fromisoformat(previous["created_at"]):
                        raise HTTPException(409, "拒绝旧快照覆盖新版本")
                manifest = files.publish_bytes(bytes(payload), incoming)
                cache.update(id=incoming["snapshot_id"], data=incoming)
            return {"ok": True, "manifest": manifest}
        except (ValueError, KeyError, TypeError, OSError, EOFError, zlib.error) as exc:
            raise HTTPException(400, f"无效快照：{exc}") from exc

    @app.get("/api/snapshots/latest", dependencies=protected)
    def manifest():
        value = files.manifest()
        if not value:
            raise HTTPException(404, "尚未收到快照")
        return value

    def resolve(name, days, country):
        try:
            return snapshot_view(data(), name, days, country)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.get("/api/gdelt/overview", dependencies=protected)
    def overview(days: int = Query(30, ge=1, le=365), view: str = "china", country: str = "USA"):
        if view != "china":
            raise HTTPException(400, "发布快照目前只包含对华总览")
        return resolve("overview", days, country)

    @app.get("/api/gdelt/attitude", dependencies=protected)
    def attitude(days: int = Query(30, ge=1, le=365), country: str = "USA"):
        return resolve("attitude", days, country)

    @app.get("/api/gdelt/country-risk", dependencies=protected)
    def country_risk(days: int = Query(30, ge=1, le=365), country: str = "US"):
        return resolve("country-risk", days, country)

    @app.get("/api/gdelt/enterprise-risk", dependencies=protected)
    def enterprise_risk(days: int = Query(30, ge=1, le=365), country: str = "US"):
        return resolve("enterprise-risk", days, country)

    @app.get("/api/gdelt/config", dependencies=protected)
    def config():
        snapshot = data()
        return {"time_ranges": [{"days": int(d), "label": f"近{d}天", "granularity": "小时" if d == "7" else "天"}
                                for d in sorted(snapshot["views"], key=int)],
                "default_days": 30, "has_data": True, "monitor_running": False,
                "show_detail_panel": False, "show_source_links": False, "demo_mode": False,
                "mode": "snapshot", "snapshot_created_at": snapshot["created_at"]}

    @app.get("/api/gdelt/metrics", dependencies=protected)
    def catalog():
        snapshot = data()
        return {"params": snapshot["params"], "metrics": snapshot["metrics"]}

    @app.get("/api/gdelt/codes", dependencies=protected)
    def codes():
        return data()["codes"]

    @app.get("/health")
    def health():
        return {"ok": True, "mode": "receiver", "has_snapshot": files.manifest() is not None}

    return app
