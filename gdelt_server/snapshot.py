"""Compact, atomic snapshots for a cloud server that need not run any metrics."""
from __future__ import annotations

import gzip
import hashlib
import json
import os
from pathlib import Path
import uuid
import re
import threading
import logging
import zlib
from datetime import datetime

from .metrics import Metrics, metric_catalog, _fill_series, _iso, _safe_div
from .store import Store, DAY, utcnow

SCHEMA_VERSION = 1


def publication_metadata(data):
    return {'schema_version':data['schema_version'], 'parser_version':data.get('parser_version'),
            'time_ranges':sorted(int(d) for d in data['views']),
            'view_names':['overview','attitude','country-risk','enterprise-risk'],
            'download_path':f"/api/snapshots/versions/{data['snapshot_id']}/download",
            'coverage_by_range':{days:{'events':v['country-risk']['common'].get('coverage', {}),
                                      'gkg':v['enterprise-risk']['common'].get('coverage', {})}
                                 for days,v in data['views'].items()}}


def encode_json(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


def all_series(metrics, days, view):
    """One grouped scan per view; do not recalculate each country's full ranking."""
    win, p = metrics.window(days), metrics.params()
    if view == "attitude":
        table, group, sums = "agg_relation", "actor1,bucket", metrics._REL_SUMS
        where = "actor2='CHN' AND actor1<>'CHN'"
        country_key = "actor1"
        blank = {"event_count": 0, "mentions": 0, "attitude_score": None, "avg_goldstein": None, "avg_tone": None}
        args = ()
    elif view == "country-risk":
        table, group, country_key = "agg_geo", "geo_country,bucket", "geo_country"
        placeholders = ",".join("?" for _ in p.roots_security)
        sums = ("SUM(n_events) event_count,SUM(sum_tone) sum_tone,"
                "SUM(n_quad3)+SUM(n_quad4) conflict,"
                f"SUM(CASE WHEN root_code IN ({placeholders}) THEN n_events ELSE 0 END) security")
        where, args = "1=1", p.roots_security
        blank = {"event_count": 0, "security": 0, "conflict": 0, "avg_tone": None}
    else:
        table, group, country_key, sums = "agg_gkg", "country,bucket", "country", metrics._GKG_SUMS
        where, args = "1=1", ()
        blank = {"total_docs": 0, "china_business_docs": 0, "avg_tone": None,
                 **{f"{f}_docs": 0 for f in metrics.ER_FIELDS}}
    rows = metrics._agg(table, sums, group, win, where, head_args=args)
    points = {}
    for r in rows:
        n = max(r.get("n_events", r.get("event_count", r.get("total_docs", 0))), 1)
        item = {"bucket": r["bucket"], "timestamp": _iso(r["bucket"])}
        if view == "attitude":
            item.update(event_count=int(r["n_events"]), mentions=int(r["sum_mentions"]),
                        attitude_score=round(100*_safe_div(p.w_goldstein*r["sum_gold_w"]+p.w_quad*r["sum_quad_w"]+p.w_tone*r["sum_tone_w"],r["sum_w"]),1),
                        avg_goldstein=round(10*r["sum_gold"]/n,2), avg_tone=round(10*r["sum_tone"]/n,2))
        elif view == "country-risk":
            item.update(event_count=int(r["event_count"]), conflict=int(r["conflict"]),
                        security=int(r["security"]), avg_tone=round(10*r["sum_tone"]/n,2))
        else:
            item.update(total_docs=int(r["total_docs"]), china_business_docs=int(r["china_business_docs"]),
                        avg_tone=round(r["sum_tone"]/n,2),
                        **{f"{f}_docs": int(r[f"{f}_docs"]) for f in metrics.ER_FIELDS})
        points.setdefault(r[country_key], {})[r["bucket"]] = item
    source = "gkg" if view == "enterprise-risk" else "events"
    coverage = metrics.store.coverage_buckets(source, win.start, win.end, win.size)
    output = {}
    for country, pts in points.items():
        series = _fill_series(pts, win, blank)
        for item in series:
            c = coverage.get(item["bucket"], {"expected": 0, "done": 0, "complete": False})
            item.update(complete=c["complete"], collected_files=c["done"], expected_files=c["expected"])
            if not c["done"]:
                for key in blank:
                    item[key] = None
        output[country] = series
    return output


def all_event_types(metrics, days):
    from .metrics import CAMEO_ROOT_LABELS
    rows = metrics._agg("agg_relation", metrics._REL_SUMS, "actor1,root_code", metrics.window(days),
                        "actor2='CHN' AND actor1<>'CHN'")
    result = {}
    for r in rows:
        result.setdefault(r["actor1"], []).append({"code": r["root_code"],
                    "label": CAMEO_ROOT_LABELS.get(r["root_code"], r["root_code"]),
                    "count": int(r["n_events"]), "impact": round(r["sum_absgold_w"]*10,1),
                    "avg_goldstein": round(10*r["sum_gold"]/max(1,r["n_events"]),2)})
    for entries in result.values():
        entries.sort(key=lambda r: r["count"], reverse=True)
    return result


def build_snapshot(store: Store, ranges):
    # All views and metadata must describe the same committed database version.
    with store.read_snapshot() as view:
        return _build_snapshot(view, ranges)


def _build_snapshot(store: Store, ranges):
    metrics = Metrics(store)
    views = {}
    for days in ranges:
        views[str(days)] = {}
        relation_series = all_series(metrics, days, 'attitude')
        for name, method in (("overview", metrics.overview), ("attitude", metrics.attitude),
                             ("country-risk", metrics.country_risk), ("enterprise-risk", metrics.enterprise_risk)):
            payload = method(days=days)
            if name == "overview":
                views[str(days)][name] = {"common": payload, 'series_by_country':relation_series}
            else:
                payload.pop("selected", None)
                payload.pop("series", None)
                payload.pop("event_types", None)
                views[str(days)][name] = {"common": payload, "series_by_country":
                    relation_series if name == 'attitude' else all_series(metrics, days, name)}
                allowed = {r["code"] for r in payload["countries"]}
                views[str(days)][name]["series_by_country"] = {
                    c: series for c, series in views[str(days)][name]["series_by_country"].items() if c in allowed}
                if name == "attitude":
                    views[str(days)][name]["event_types_by_country"] = all_event_types(metrics, days)
    return {"schema_version": SCHEMA_VERSION, "snapshot_id": uuid.uuid4().hex,
            "created_at": metrics.now().isoformat(), "data_version": store.get_state("data_version", 0),
            "parameter_version": store.get_state("parameter_version", 0), "parser_version": "aggregate-v1",
            "params": metrics.params().as_dict(), "metrics": metric_catalog(metrics.params()),
            "codes": metrics.code_reference(), "views": views}


def validate_snapshot(data):
    if not isinstance(data, dict) or type(data.get('schema_version')) is not int or data.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("快照格式版本不支持")
    required = ("snapshot_id", "created_at", "data_version", "parameter_version", "views", "metrics", "params", "codes")
    if any(key not in data for key in required) or not isinstance(data["snapshot_id"], str):
        raise ValueError("快照字段不完整")
    if not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', data['snapshot_id']):
        raise ValueError('快照ID不合法')
    for key in ('data_version', 'parameter_version'):
        if type(data[key]) is not int or data[key] < 0:
            raise ValueError('快照版本必须为非负整数')
    if not isinstance(data['created_at'], str) or datetime.fromisoformat(data['created_at']).tzinfo is None:
        raise ValueError('快照时间必须包含时区')
    if not isinstance(data["views"], dict) or not data["views"]:
        raise ValueError("快照没有视图")
    if any(not isinstance(data[k], dict) for k in ('metrics','params','codes')):
        raise ValueError('快照参数、口径或代码表结构不合法')
    for days, views in data["views"].items():
        if days not in ("7", "30", "90", "365"):
            raise ValueError("不支持的快照时间范围")
        if not isinstance(views, dict):
            raise ValueError('快照视图结构不合法')
        for view in ("overview", "attitude", "country-risk", "enterprise-risk"):
            record = views.get(view)
            if (not isinstance(record, dict) or not isinstance(record.get('common'), dict)
                    or not isinstance(record['common'].get('countries'), list)):
                raise ValueError("快照视图不完整")
            if view != "overview" and not isinstance(record.get("series_by_country"), dict):
                raise ValueError("快照缺少国家趋势")
            if view == "attitude" and not isinstance(record.get("event_types_by_country"), dict):
                raise ValueError("快照缺少事件类型")


class SnapshotFiles:
    def __init__(self, folder):
        self.folder = Path(folder)
        self.folder.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        previous = self.manifest()
        if previous and 'schema_version' not in previous:
            try:
                data = self.read()
                validate_snapshot(data)
                previous.update(publication_metadata(data))
                self._write_json(self._metadata_path(previous['snapshot_id']), previous)
                self._write_json(self.folder/'latest.json', previous)
            except (ValueError, OSError, EOFError, KeyError, TypeError, zlib.error) as exc:
                logging.getLogger(__name__).warning('旧快照元信息升级失败，请重新生成快照：%s', exc)

    def manifest(self):
        path = self.folder/"latest.json"
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None

    def publish(self, data):
        validate_snapshot(data)
        payload = gzip.compress(encode_json(data), compresslevel=6, mtime=0)
        return self.publish_bytes(payload, data)

    def publish_bytes(self, payload, data):
        with self.lock:
            return self._publish_bytes(payload, data)

    def _metadata_path(self, snapshot_id):
        if not isinstance(snapshot_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', snapshot_id):
            raise ValueError('快照ID不合法')
        return self.folder/('version-'+hashlib.sha256(snapshot_id.encode()).hexdigest()+'.json')

    def _write_json(self, path, value):
        tmp = path.with_name(path.name+'.tmp')
        try:
            with tmp.open('wb') as f:
                f.write(encode_json(value))
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, path)
        finally:
            tmp.unlink(missing_ok=True)

    def version_manifest(self, snapshot_id):
        with self.lock:
            path = self._metadata_path(snapshot_id)
            value = json.loads(path.read_text(encoding='utf-8')) if path.exists() else self.manifest()
            if not value or value['snapshot_id'] != snapshot_id or not (self.folder/value['filename']).is_file():
                raise FileNotFoundError('指定快照不存在或已过保留期，请重新检查最新版本')
            return value

    def open_download(self, snapshot_id=None):
        # Open before cleanup can unlink the file; an in-flight stream keeps its own descriptor.
        with self.lock:
            latest = self.manifest() if snapshot_id is None else self.version_manifest(snapshot_id)
            if latest is None:
                raise FileNotFoundError('尚未生成快照')
            return latest, (self.folder/latest['filename']).open('rb')

    def _publish_bytes(self, payload, data):
        validate_snapshot(data)
        digest = hashlib.sha256(payload).hexdigest()
        previous = self.manifest()
        # Upgrade the existing version's index without recomputing or altering its payload.
        if previous:
            self._write_json(self._metadata_path(previous['snapshot_id']), previous)
        try:
            existing = self.version_manifest(data['snapshot_id'])
        except FileNotFoundError:
            existing = None
        if existing:
            if existing['sha256'] != digest:
                raise ValueError('相同快照ID不能对应不同内容')
            return existing
        # Filename is derived locally, never taken from an uploaded path or id.
        name = f"snapshot-{uuid.uuid4().hex}.json.gz"
        tmp = self.folder/(name+".tmp")
        manifest_tmp = self.folder/"latest.json.tmp"
        published = False
        try:
            with tmp.open("wb") as f:
                f.write(payload)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, self.folder/name)
            manifest = {"filename": name, "sha256": digest,
                        "bytes": len(payload), "snapshot_id": data["snapshot_id"],
                        "created_at": data["created_at"], "data_version": data["data_version"],
                        "parameter_version": data["parameter_version"], **publication_metadata(data)}
            self._write_json(self._metadata_path(data['snapshot_id']), manifest)
            with manifest_tmp.open("wb") as f:
                f.write(encode_json(manifest))
                f.flush()
                os.fsync(f.fileno())
            os.replace(manifest_tmp, self.folder/"latest.json")
            published = True
            versions = sorted(self.folder.glob("snapshot-*.json.gz"), key=lambda p: p.stat().st_mtime, reverse=True)
            for p in versions[2:]:
                try:
                    p.unlink()
                except PermissionError:
                    pass  # A Windows download may still have this version open.
            for p in self.folder.glob('version-*.json'):
                saved = json.loads(p.read_text(encoding='utf-8'))
                if not (self.folder/saved['filename']).exists():
                    p.unlink(missing_ok=True)
            return manifest
        finally:
            tmp.unlink(missing_ok=True)
            manifest_tmp.unlink(missing_ok=True)
            if not published:
                (self.folder/name).unlink(missing_ok=True)
                self._metadata_path(data['snapshot_id']).unlink(missing_ok=True)

    def read(self):
        manifest, stream = self.open_download()
        with stream:
            payload = stream.read()
        if hashlib.sha256(payload).hexdigest() != manifest["sha256"]:
            raise ValueError("快照校验失败")
        return json.loads(gzip.decompress(payload))


def snapshot_view(data, name, days, country):
    try:
        record = data["views"][str(days)][name]
    except KeyError as exc:
        raise ValueError("快照未包含该时间范围或视图") from exc
    common = dict(record["common"])
    common["snapshot_id"] = data["snapshot_id"]
    common["snapshot_created_at"] = data["created_at"]
    # Freshness must be evaluated at read time, even if this snapshot is old.
    coverage = dict(common.get("coverage", {}))
    latest = coverage.get("last_file_ts")
    coverage["stale"] = latest is None or int(utcnow().timestamp())-latest > 2700
    common["coverage"] = coverage
    if name == 'overview':
        common['selected'] = next((r for r in common['countries'] if r['code'] == country.upper()), None)
        if country:
            # Old schema-1 bundles can reuse their existing attitude trends.
            series = record.get('series_by_country', data['views'][str(days)]['attitude']['series_by_country'])
            common['series'] = series.get(country.upper(), [])
    if name != "overview":
        common["selected"] = next((r for r in common["countries"] if r["code"] == country.upper()), None)
        common["series"] = record["series_by_country"].get(country.upper(), [])
        if name == "attitude":
            common["event_types"] = record["event_types_by_country"].get(country.upper(), [])
    return common
