"""Stream ZIP members directly into aggregate counters; never retain article rows."""
from __future__ import annotations
import csv
import io
import math
import re
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from .fips_names import CHINA_REGION_CAMEO, CHINA_REGION_FIPS
from .store import QUAD_DIRECTION,bucket_of,mention_weight,goldstein_unit,tone_unit
csv.field_size_limit(16*1024*1024)

def _text(fields: list[str], index: int) -> str:
    return fields[index].strip() if index < len(fields) else ""


def _int(fields: list[str], index: int, default: int = 0) -> int:
    try:
        return int(float(_text(fields, index) or default))
    except ValueError:
        return default


def _num(fields: list[str], index: int) -> float | None:
    try:
        value = _text(fields, index)
        number = float(value) if value else None
        return number if number is None or math.isfinite(number) else None
    except ValueError:
        return None


RISK_THEME_MARKERS = {
    "security": ("ARMEDCONFLICT", "TERROR", "TERRORISM", "KILL", "KILLING", "WOUND", "WOUNDED", "KIDNAP", "VIOLENCE",
                 "GENERALCRIME", "SECURITY_SERVICES", "MILITARY"),
    "political": ("EPU_POLICY", "ELECTION", "GENERAL_GOVERNMENT", "LEGISLATION",
                  "CORRUPTION", "REGULATION", "SANCTION", "BAN", "POLITICAL"),
    "economic": ("EPU_ECONOMY", "ECON_", "TRADE", "FINANCIAL", "DEBT", "INFLATION",
                 "TAX_ECON_PRICE", "JOBS", "ENTERPRISE", "INDUSTRY"),
    "infrastructure": ("INFRASTRUCTURE", "TRANSPORT", "ENERGY", "POWER", "WATER",
                       "LOGISTICS", "MARITIME", "SUPPLY_CHAIN"),
    "social": ("PROTEST", "UNREST", "LABOR", "MIGRATION", "ARREST", "SOCIAL_PROTECTION"),
    "health": ("GENERAL_HEALTH", "MEDICAL", "DISEASE", "PUBLIC_HEALTH", "PANDEMIC",
               "HEALTH_NUTRITION"),
}

GKG_CATEGORY_ORDER = ("security", "political", "economic",
                      "infrastructure", "social", "health")

CHINESE_COMPANY_ALIASES = (
    "huawei", "zte", "alibaba", "tencent", "bytedance", "tiktok", "catl", "byd",
    "sinopec", "petrochina", "cnpc", "cnooc", "state grid", "cosco", "lenovo",
    "xiaomi", "geely", "china mobile", "china telecom", "china railway",
)

# 原实现用 `alias in org_lower` 做子串匹配，"byd" 会命中波兰城市 Bydgoszcz，
# "zte" 会命中任何含这三个字母的词。这里改成词边界匹配。
_ALIAS_RE = re.compile(
    r"(?<![a-z0-9])(" + "|".join(re.escape(a) for a in CHINESE_COMPANY_ALIASES) + r")(?![a-z0-9])"
)


def is_china_business(organizations: str) -> bool:
    return bool(_ALIAS_RE.search(organizations.lower()))


def gkg_categories(themes: str) -> list[str]:
    values = {v.upper() for v in themes.rstrip(";").split(";") if v}

    def matches(value: str, marker: str) -> bool:
        marker = marker.upper().rstrip("_")
        return (value == marker or value.startswith(f"{marker}_")
                or value.endswith(f"_{marker}") or f"_{marker}_" in f"_{value}_")

    return [name for name, markers in RISK_THEME_MARKERS.items()
            if any(any(matches(v, m) for m in markers) for v in values)]


@dataclass
class Parsed:
    tables: dict
    rows: int
    skipped: int = 0
    parse_seconds: float = 0.


def validate_archive(archive, max_mb):
    files = [i for i in archive.infolist() if not i.is_dir()]
    if len(files) != 1 or files[0].file_size <= 0:
        raise ValueError("压缩包必须包含一个非空数据文件")
    if files[0].file_size > max_mb * 1024 * 1024:
        raise ValueError("解压大小超过上限")


def parse_events(payload: Path, file_ts: int, max_uncompressed_mb: int = 1024, cancel=None) -> Parsed:
    """
    解析一个 export.CSV.zip，直接产出聚合桶。

    返回 (rel_buckets, geo_buckets, raw_rows, activity)
    """
    rel: dict[tuple, dict] = {}
    geo: dict[tuple, dict] = {}
    rows = skipped = 0

    def rel_cell(key):
        cell = rel.get(key)
        if cell is None:
            cell = rel[key] = {
                "n_events": 0, "sum_mentions": 0, "sum_sources": 0, "sum_articles": 0,
                "sum_w": 0.0, "sum_gold_w": 0.0, "sum_quad_w": 0.0, "sum_tone_w": 0.0,
                "sum_gold": 0.0, "sum_tone": 0.0, "sum_absgold_w": 0.0,
                "n_quad1": 0, "n_quad2": 0, "n_quad3": 0, "n_quad4": 0,
            }
        return cell

    def geo_cell(key):
        cell = geo.get(key)
        if cell is None:
            cell = geo[key] = {
                "n_events": 0, "sum_mentions": 0, "sum_sources": 0,
                "sum_w": 0.0, "sum_gold_w": 0.0, "sum_tone_w": 0.0,
                "sum_gold": 0.0, "sum_tone": 0.0,
                "n_quad1": 0, "n_quad2": 0, "n_quad3": 0, "n_quad4": 0,
            }
        return cell

    with zipfile.ZipFile(payload) as archive:
        validate_archive(archive, max_uncompressed_mb)
        names = [i.filename for i in archive.infolist() if not i.is_dir()]
        if not names:
            raise ValueError("空的 GDELT 事件压缩包")
        with archive.open(names[0]) as stream:
            wrapper = io.TextIOWrapper(stream, encoding="utf-8",
                                       errors="replace", newline="")
            for fields in csv.reader(wrapper, delimiter="\t"):
                if cancel is not None and cancel.is_set():
                    raise InterruptedError('解析已暂停，未提交批次保留待处理')
                if len(fields) not in (58, 61):
                    raise ValueError(f"未知Events列布局：{len(fields)}列")
                rows += 1
                # GDELT 2.0 当前导出是 61 列（ActionGeo 含三个 ADM2 字段），
                # 旧编码本是 58 列。两种都支持。
                if len(fields) >= 61:
                    i_geo_name, i_geo_cc, i_added, i_url = 52, 53, 59, 60
                else:
                    i_geo_name, i_geo_cc, i_added, i_url = 50, 51, 56, 57

                # 涉及港澳台的事件（任一 Actor 或发生地）整条不计入
                a1 = _text(fields, 7).upper()
                a2 = _text(fields, 17).upper()
                cc = _text(fields, i_geo_cc).upper()
                if (a1 in CHINA_REGION_CAMEO or a2 in CHINA_REGION_CAMEO
                        or cc in CHINA_REGION_FIPS):
                    skipped += 1
                    continue

                ts = file_ts
                bucket = bucket_of(ts, "hour")

                root_code = _text(fields, 28) or "00"
                quad = _int(fields, 29, 0)
                gold = _num(fields, 30)
                mentions = _int(fields, 31)
                sources = _int(fields, 32)
                articles = _int(fields, 33)
                tone = _num(fields, 34)
                # Official exports occasionally contain unclassified placeholders.
                # These cannot contribute to any score; count them as skipped.
                if root_code in ("--", "---") and _text(fields, 26) == "---" and gold is None:
                    skipped += 1
                    continue
                if root_code not in {f"{i:02d}" for i in range(1,21)} or quad not in (1,2,3,4):
                    raise ValueError("Events事件大类或四分类不合法")
                if gold is None or tone is None or min(mentions, sources, articles) < 0:
                    raise ValueError("Events数值字段缺失或不合法")

                w = mention_weight(mentions)
                g = goldstein_unit(gold)
                t = tone_unit(tone)
                q = QUAD_DIRECTION.get(quad, 0.0)
                quad_key = f"n_quad{quad}" if quad in (1, 2, 3, 4) else None

                if len(a1) == 3 and len(a2) == 3 and a1 != a2:
                    c = rel_cell(("hour", bucket, a1, a2, root_code))
                    c["n_events"] += 1
                    c["sum_mentions"] += mentions
                    c["sum_sources"] += sources
                    c["sum_articles"] += articles
                    c["sum_w"] += w
                    c["sum_gold_w"] += g * w
                    c["sum_quad_w"] += q * w
                    c["sum_tone_w"] += t * w
                    c["sum_gold"] += g
                    c["sum_tone"] += t
                    c["sum_absgold_w"] += abs(g) * w
                    if quad_key:
                        c[quad_key] += 1

                if len(cc) == 2:
                    c = geo_cell(("hour", bucket, cc, root_code))
                    c["n_events"] += 1
                    c["sum_mentions"] += mentions
                    c["sum_sources"] += sources
                    c["sum_w"] += w
                    c["sum_gold_w"] += g * w
                    c["sum_tone_w"] += t * w
                    c["sum_gold"] += g
                    c["sum_tone"] += t
                    if quad_key:
                        c[quad_key] += 1

    return Parsed({"agg_relation": rel, "agg_geo": geo}, rows, skipped)


def parse_gkg(payload: Path, file_ts: int, max_uncompressed_mb: int = 1024, cancel=None) -> Parsed:
    buckets: dict[tuple, dict] = {}
    rows = skipped = 0

    def cell(key):
        c = buckets.get(key)
        if c is None:
            c = buckets[key] = {
                "total_docs": 0, "security_docs": 0, "political_docs": 0,
                "economic_docs": 0, "infrastructure_docs": 0, "social_docs": 0,
                "health_docs": 0, "china_business_docs": 0,
                "sum_tone": 0.0, "sum_polarity": 0.0,
            }
        return c

    with zipfile.ZipFile(payload) as archive:
        validate_archive(archive, max_uncompressed_mb)
        names = [i.filename for i in archive.infolist() if not i.is_dir()]
        if not names:
            raise ValueError("空的 GDELT GKG 压缩包")
        with archive.open(names[0]) as stream:
            wrapper = io.TextIOWrapper(stream, encoding="utf-8",
                                       errors="replace", newline="")
            for fields in csv.reader(wrapper, delimiter="\t"):
                if cancel is not None and cancel.is_set():
                    raise InterruptedError('解析已暂停，未提交批次保留待处理')
                if len(fields) < 27:
                    raise ValueError(f"未知GKG列布局：{len(fields)}列")
                rows += 1
                ts = file_ts
                bucket = bucket_of(ts, "hour")

                countries: set[str] = set()
                for block in _text(fields, 9).rstrip(";").split(";"):
                    parts = block.split("#")
                    if len(parts) > 2 and len(parts[2]) == 2:
                        countries.add(parts[2].upper())
                countries -= CHINA_REGION_FIPS | {"CH"}  # 海外经营风险不含中国及港澳台
                if not countries:
                    skipped += 1
                    continue

                themes = _text(fields, 7)
                organizations = _text(fields, 13)
                cats = gkg_categories(themes)
                china = int(is_china_business(organizations))

                tone_parts = _text(fields, 15).split(",")
                try:
                    tone = float(tone_parts[0]) if tone_parts and tone_parts[0] else 0.0
                    polarity = float(tone_parts[3]) if len(tone_parts) > 3 and tone_parts[3] else 0.0
                except ValueError as exc:
                    raise ValueError("GKG语调不是合法数字") from exc

                if not math.isfinite(tone) or not math.isfinite(polarity):
                    raise ValueError("GKG语调包含NaN或Infinity")
                for code in countries:
                    c = cell(("hour", bucket, code))
                    c["total_docs"] += 1
                    for name in cats:
                        c[f"{name}_docs"] += 1
                    c["china_business_docs"] += china
                    c["sum_tone"] += tone
                    c["sum_polarity"] += polarity
    return Parsed({"agg_gkg": buckets}, rows, skipped)
