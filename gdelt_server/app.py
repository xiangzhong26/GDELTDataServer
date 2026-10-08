from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import fields
from datetime import date, datetime, timezone
import hmac
import json
import logging
import sqlite3
from pathlib import Path

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse, Response
from starlette.background import BackgroundTask
from pydantic import BaseModel, Field, model_validator

from .config import Settings
from .instance import InstanceLock
from .metrics import Metrics, Params, metric_catalog, validate_params
from .service import Service


class SnapshotReceipt(BaseModel):
    snapshot_id: str = Field(min_length=1, max_length=128, pattern=r'^[A-Za-z0-9_-]+$')
    sha256: str = Field(pattern=r'^[a-f0-9]{64}$')


def authorize(settings, request, authorization):
    if settings.api_token:
        if not hmac.compare_digest(authorization, "Bearer "+settings.api_token):
            raise HTTPException(401, "访问令牌无效", headers={"WWW-Authenticate": "Bearer"})
    elif request.client and request.client.host not in ("127.0.0.1", "::1", "localhost", "testclient"):
        raise HTTPException(403, "未配置令牌，仅允许本机访问")


class MonitorBody(BaseModel):
    enabled: bool


class ConcurrencyBody(BaseModel):
    download_workers: int = Field(ge=1, le=64, strict=True)
    parser_workers: int = Field(ge=1, le=16, strict=True)

    model_config = {'extra': 'forbid'}


class BackfillBody(BaseModel):
    hours: int | None = Field(None, ge=1, le=87600)
    start_date: date | None = None

    @model_validator(mode="after")
    def validate_range(self):
        if self.hours is not None and self.start_date is not None:
            raise ValueError("小时数和起始日期只能选择一个")
        if self.hours is None and self.start_date is None:
            self.hours = 72
        return self


def create_app(settings=None):
    settings = settings or Settings.load()

    @asynccontextmanager
    async def lifespan(app):
        lock = InstanceLock(settings.data_dir/"service.lock")
        lock.acquire()
        service = None
        try:
            service = Service(settings)
            app.state.service = service
            service.start()
            yield
        finally:
            if service:
                service.close()
            lock.release()

    app = FastAPI(title="GDELT 独立数据服务器", version="0.1.0", lifespan=lifespan)

    @app.exception_handler(sqlite3.Error)
    async def database_error(request, exc):
        code = getattr(exc, 'sqlite_errorname', 'SQLITE_ERROR')
        logging.getLogger(__name__).exception('数据库请求失败 %s: %s', code, exc)
        return JSONResponse(status_code=503, content={
            'detail': f'数据库暂时无法完成查询：{code}。请查看服务日志，并检查数据目录、临时目录权限和磁盘状态。',
            'sqlite_error': code})

    def auth(request: Request, authorization: str = Header(default="")):
        authorize(settings, request, authorization)

    protected = [Depends(auth)]

    def snapshot_auth(request: Request, authorization: str = Header(default='')):
        token = settings.snapshot_read_token
        if token and hmac.compare_digest(authorization, 'Bearer '+token):
            return
        # A configured read token closes the unauthenticated localhost fallback for exports.
        if token and not settings.api_token:
            raise HTTPException(401, '快照访问令牌无效', headers={'WWW-Authenticate':'Bearer'})
        authorize(settings, request, authorization)

    snapshot_protected = [Depends(snapshot_auth)]

    @app.get("/", response_class=HTMLResponse)
    def dashboard():
        return Path(__file__).with_name("dashboard.html").read_text(encoding="utf-8")

    @app.get("/health")
    def health():
        return {"ok": True, "service": "gdelt-data-server"}

    @app.get("/api/gdelt/status", dependencies=protected)
    def status():
        return app.state.service.status()

    @app.get("/api/gdelt/config", dependencies=protected)
    def config():
        service = app.state.service
        return {"time_ranges": [{"days": d, "label": {1:"实时 · 24小时",1095:"近3年"}.get(d,f"近{d}天"), "granularity": "小时" if d <= 7 else "天"}
                                for d in service.settings.snapshot_days], "default_days": 30,
                "monitor_running": service.enabled, "demo_mode": False,
                "show_detail_panel": False, "show_source_links": False,
                "refresh_seconds": settings.poll_seconds,
                "has_data": bool(service.store.get_state("data_version", 0)),
                "raw_data_retained": False}

    def metric_result(name, days, country="US", view="china", fresh=False):
        if name != 'overview' or view == 'china':
            return app.state.service.queries.resolve(name, days, country, fresh)
        store = app.state.service.store
        metrics = Metrics(store)
        if name == "overview":
            return metrics.overview(days, view, country)
        return getattr(metrics, name.replace("-", "_"))(days, country)

    @app.get("/api/gdelt/overview", dependencies=protected)
    def overview(days: int = Query(30, ge=1, le=1095), country: str = "", view: str = "china", fresh: bool = False):
        if view not in ("china", "partner"):
            raise HTTPException(400, "view仅支持china或partner")
        return metric_result("overview", days, country.upper(), view, fresh)

    @app.get("/api/gdelt/attitude", dependencies=protected)
    def attitude(days: int = Query(30, ge=1, le=1095), country: str = "USA", fresh: bool = False):
        return metric_result("attitude", days, country.upper(), fresh=fresh)

    @app.get("/api/gdelt/country-risk", dependencies=protected)
    def country_risk(days: int = Query(30, ge=1, le=1095), country: str = "US", fresh: bool = False):
        return metric_result("country-risk", days, country.upper(), fresh=fresh)

    @app.get("/api/gdelt/enterprise-risk", dependencies=protected)
    def enterprise_risk(days: int = Query(30, ge=1, le=1095), country: str = "US", fresh: bool = False):
        return metric_result("enterprise-risk", days, country.upper(), fresh=fresh)

    @app.get("/api/gdelt/metrics", dependencies=protected)
    def catalog():
        p = Metrics(app.state.service.store).params()
        from .metrics import MODEL_VERSION
        return {"params": p.as_dict(), "metrics": metric_catalog(p), "model_version": MODEL_VERSION}

    @app.get("/api/gdelt/codes", dependencies=protected)
    def codes():
        return Metrics(app.state.service.store).code_reference()

    @app.get("/api/gdelt/ai-snapshot", dependencies=protected)
    def ai_snapshot(days: int = Query(30, ge=1, le=1095), iso3: str = "USA", fips: str = "US"):
        metrics = Metrics(app.state.service.store)
        data = metrics.ai_snapshot(iso3.upper(), fips.upper(), days)
        from .metrics import make_window
        win = make_window(days)
        data["coverage"] = {k: metrics.store.coverage(k, win.start, win.end) for k in ("events", "gkg")}
        return data

    @app.get("/api/gdelt/details", dependencies=protected)
    def details():
        raise HTTPException(410, "此服务不保留原始报道，仅提供聚合统计")

    @app.post("/api/admin/monitor", dependencies=protected)
    def monitor(body: MonitorBody):
        app.state.service.monitor(body.enabled)
        return {"ok": True, "enabled": body.enabled}

    @app.post("/api/admin/sync", dependencies=protected)
    def sync():
        return app.state.service.request("sync")

    @app.put('/api/admin/concurrency', dependencies=protected)
    def concurrency(body: ConcurrencyBody):
        return app.state.service.configure_concurrency(body.download_workers, body.parser_workers)

    @app.post('/api/admin/pause', dependencies=protected)
    def pause_all():
        return app.state.service.pause_all()

    @app.post('/api/admin/retry', dependencies=protected)
    def retry():
        return app.state.service.request('retry')

    @app.post('/api/admin/repair', dependencies=protected)
    def repair():
        return app.state.service.request('repair')

    @app.post("/api/admin/backfill", dependencies=protected)
    def backfill(body: BackfillBody):
        try:
            return app.state.service.request("backfill", body.hours, body.start_date)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc

    @app.post("/api/admin/export", dependencies=protected)
    def export():
        return app.state.service.request("export")

    @app.post('/api/admin/backfill/pause', dependencies=protected)
    def pause_backfill():
        return app.state.service.pause_backfill()

    @app.post('/api/admin/backfill/resume', dependencies=protected)
    def resume_backfill():
        return app.state.service.resume_backfill()

    @app.put("/api/admin/params", dependencies=protected)
    def params(body: dict):
        service = app.state.service
        allowed = {f.name for f in fields(Params)}
        if set(body)-allowed:
            raise HTTPException(400, "包含未知参数")
        if not service.ingestor.busy.acquire(blocking=False):
            raise HTTPException(409, "请等待当前同步/导出完成后修改口径")
        try:
            p = Params.load(service.store)
            for key, value in body.items():
                from .metrics import RETIRED_PARAMS
                if key in RETIRED_PARAMS:
                    raise ValueError(f'{key}属于旧模型参数，请刷新页面使用v3参数')
                old = getattr(p, key)
                if isinstance(old, tuple):
                    if not isinstance(value, list):
                        raise ValueError(f"{key} 必须是事件大类列表")
                    value = tuple(str(v).zfill(2) for v in value)
                elif isinstance(old, int):
                    if isinstance(value, bool) or not isinstance(value, (int,float)) or int(value) != value:
                        raise ValueError(f"{key} 必须是整数")
                    value = int(value)
                elif isinstance(value, bool) or not isinstance(value, (int,float)):
                    raise ValueError(f"{key} 必须是数字")
                setattr(p, key, value)
            validate_params(p)
            # Config and version change are atomic; readers never see an old version with new params.
            with service.store.connect(write=True) as db:
                service.store._state(db, "metric_params", p.as_dict())
                row = db.execute("SELECT value FROM gdelt_state WHERE key='parameter_version'").fetchone()
                service.store._state(db, "parameter_version", (json.loads(row[0]) if row else 0)+1)
                service.store._state(db, "snapshot_dirty", True)
            return {"ok": True, "params": p.as_dict(), "metrics": metric_catalog(p)}
        except (ValueError, TypeError, OverflowError) as exc:
            raise HTTPException(400, str(exc)) from exc
        finally:
            service.ingestor.busy.release()

    @app.get("/api/snapshots/latest", dependencies=snapshot_protected)
    def manifest(if_none_match: str = Header(default='')):
        source = app.state.service
        value = source.snapshots.manifest()
        if not value:
            raise HTTPException(404, "尚未生成快照")
        etag = '"'+value['sha256']+'"'
        headers = {'ETag':etag, 'Cache-Control':'no-cache',
                   'X-GDELT-Monitor': '1' if source.enabled else '0',
                   'X-GDELT-Phase': source.ingestor.phase,
                   'X-GDELT-Job': (source.job or {}).get('action', ''),
                   'X-GDELT-Worker': '1' if source.thread.is_alive() else '0',
                   'X-GDELT-Data-Version': str(source.store.get_state('data_version', 0)),
                   'X-GDELT-Last-Ingest': str(source.store.get_state('last_ingest_at') or '')}
        if any(tag.strip() in (etag, 'W/'+etag, '*') for tag in if_none_match.split(',')):
            return Response(status_code=304, headers=headers)
        return JSONResponse(value, headers=headers)

    @app.post('/api/snapshots/receipt', dependencies=snapshot_protected)
    def receipt(body: SnapshotReceipt):
        source = app.state.service
        try:
            value = source.snapshots.version_manifest(body.snapshot_id)
        except FileNotFoundError:
            raise HTTPException(404, '该版本已过保留期')
        if not hmac.compare_digest(value['sha256'], body.sha256):
            raise HTTPException(409, '版本校验值不符')
        source.store.set_state('consumer_receipt', {
            'snapshot_id': body.snapshot_id, 'data_version': value['data_version'],
            'received_at': datetime.now(timezone.utc).isoformat()})
        return {'accepted': True}

    @app.get('/api/snapshots/versions/{snapshot_id}/chunks/{digest}', dependencies=snapshot_protected)
    def chunk(snapshot_id: str, digest: str):
        try:
            value, stream = app.state.service.snapshots.open_download(snapshot_id)
        except (ValueError, FileNotFoundError):
            raise HTTPException(404, '指定版本不存在')
        part = next((p for p in value.get('chunks', []) if p['sha256'] == digest), None)
        if part is None:
            stream.close()
            raise HTTPException(404, '指定分块不存在')
        def blocks():
            try:
                stream.seek(part['offset'])
                remaining = part['bytes']
                while remaining:
                    block = stream.read(min(remaining, 256 * 1024))
                    if not block:
                        raise OSError('分块文件被截断')
                    remaining -= len(block)
                    yield block
            finally:
                stream.close()
        return StreamingResponse(blocks(), media_type='application/gzip',
            background=BackgroundTask(stream.close), headers={
                'Content-Length': str(part['bytes']), 'X-SHA256': digest,
                'X-Snapshot-ID': value['snapshot_id'], 'Cache-Control': 'private, no-store'})

    def download_version(snapshot_id=None):
        files = app.state.service.snapshots
        try:
            value, stream = files.open_download(snapshot_id)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        except FileNotFoundError as exc:
            raise HTTPException(404, str(exc)) from exc
        def chunks():
            try:
                while block := stream.read(256*1024):
                    yield block
            finally:
                stream.close()
        return StreamingResponse(chunks(), media_type='application/gzip',
            background=BackgroundTask(stream.close), headers={'X-SHA256':value['sha256'],
            'X-Snapshot-ID':value['snapshot_id'], 'ETag':'"'+value['sha256']+'"',
            'Content-Length':str(value['bytes']), 'Cache-Control':'private, no-store',
            'Content-Disposition':'attachment; filename="gdelt-snapshot.json.gz"'})

    @app.get('/api/snapshots/download', dependencies=snapshot_protected)
    def download():
        return download_version()

    @app.get('/api/snapshots/versions/{snapshot_id}/download', dependencies=snapshot_protected)
    def pinned_download(snapshot_id: str):
        return download_version(snapshot_id)

    return app


app = create_app()
