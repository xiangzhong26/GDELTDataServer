"""
国家代码互查
============

三套编码要对齐：

  ISO3 / CAMEO   VNM   ← DSI 主站与 GDELT 的 Actor 字段用这个
  FIPS 10-4      VM    ← GDELT 的 ActionGeo 与 GKG 用这个
  中文名         越南   ← 用户和大模型说的是这个

FIPS ↔ ISO3 的对照表没必要手写 250 行：两张名称表的中文名是同一套写法，
按中文名对接就能自动建出 246 组对应关系，剩下 20 个全是「东南亚地区」
「西非地区」这类区域聚合码 —— 它们本来就不该有国家级对应。
"""

from __future__ import annotations

from .cameo_names import CAMEO_NAMES_ZH
from .fips_names import CHINA_REGION_CAMEO, CHINA_REGION_FIPS, FIPS_NAMES_ZH


def _build() -> tuple[dict[str, str], dict[str, str]]:
    name_to_fips: dict[str, str] = {}
    for fips, name in FIPS_NAMES_ZH.items():
        name_to_fips.setdefault(name, fips)

    iso_to_fips: dict[str, str] = {}
    for iso, name in CAMEO_NAMES_ZH.items():
        if len(iso) != 3:
            continue
        fips = name_to_fips.get(name)
        if fips:
            iso_to_fips[iso] = fips

    # 少数名称写法不一致的手工补齐
    iso_to_fips.setdefault("PSE", "WE")     # 巴勒斯坦领土 → 西岸
    iso_to_fips.setdefault("TLS", "TT")     # 东帝汶
    iso_to_fips.setdefault("SRB", "RI")     # 塞尔维亚
    iso_to_fips.setdefault("MNE", "MJ")     # 黑山

    fips_to_iso = {}
    for iso, fips in iso_to_fips.items():
        fips_to_iso.setdefault(fips, iso)
    return iso_to_fips, fips_to_iso


ISO3_TO_FIPS, FIPS_TO_ISO3 = _build()

# 中文名 → ISO3（含常见简称）
_ZH_TO_ISO3: dict[str, str] = {}
for _iso, _name in CAMEO_NAMES_ZH.items():
    if len(_iso) == 3:
        _ZH_TO_ISO3.setdefault(_name, _iso)

_ALIASES = {
    "美国": "USA", "英国": "GBR", "韩国": "KOR", "南韩": "KOR", "朝鲜": "PRK",
    "俄国": "RUS", "俄罗斯": "RUS", "阿联酋": "ARE", "沙特": "SAU",
    "老挝": "LAO", "柬埔寨": "KHM", "缅甸": "MMR", "越南": "VNM",
    "泰国": "THA", "新加坡": "SGP", "马来西亚": "MYS", "印尼": "IDN",
    "印度尼西亚": "IDN", "菲律宾": "PHL", "文莱": "BRN", "东帝汶": "TLS",
    "印度": "IND", "巴基斯坦": "PAK", "孟加拉": "BGD", "孟加拉国": "BGD",
    "斯里兰卡": "LKA", "尼泊尔": "NPL", "哈萨克斯坦": "KAZ",
    "乌兹别克斯坦": "UZB", "土耳其": "TUR", "埃及": "EGY", "尼日利亚": "NGA",
    "南非": "ZAF", "肯尼亚": "KEN", "埃塞俄比亚": "ETH", "坦桑尼亚": "TZA",
    "巴西": "BRA", "墨西哥": "MEX", "阿根廷": "ARG", "智利": "CHL",
    "秘鲁": "PER", "哥伦比亚": "COL", "中国": "CHN", "日本": "JPN",
    "德国": "DEU", "法国": "FRA", "意大利": "ITA", "西班牙": "ESP",
    "澳大利亚": "AUS", "波兰": "POL", "沙特阿拉伯": "SAU",
}
for _k, _v in _ALIASES.items():
    _ZH_TO_ISO3.setdefault(_k, _v)


def resolve(country: str | None) -> dict[str, str] | None:
    """
    把用户/模型给的任意写法解析成 {iso3, fips, name}。
    接受中文名、ISO3（VNM）、FIPS（VM）。解析不出来返回 None。
    """
    if not country:
        return None
    raw = str(country).strip()
    key = raw.upper()

    iso3 = None
    if len(key) == 3 and key in CAMEO_NAMES_ZH:
        iso3 = key
    elif len(key) == 2 and key in FIPS_NAMES_ZH:
        if key in CHINA_REGION_FIPS:
            return None
        iso3 = FIPS_TO_ISO3.get(key)
        if not iso3:
            return {"iso3": "", "fips": key, "name": FIPS_NAMES_ZH[key]}
    else:
        iso3 = _ZH_TO_ISO3.get(raw)
        if not iso3:
            # 「越南社会主义共和国」这类长写法做一次包含匹配
            for name, code in _ZH_TO_ISO3.items():
                if len(name) >= 2 and (name in raw or raw in name):
                    iso3 = code
                    break
    if not iso3 or iso3 in CHINA_REGION_CAMEO or key in CHINA_REGION_FIPS:
        return None      # 港澳台不作为独立国家/地区查询
    return {
        "iso3": iso3,
        "fips": ISO3_TO_FIPS.get(iso3, ""),
        "name": CAMEO_NAMES_ZH.get(iso3, raw),
    }


def is_china_region(fips: str | None) -> bool:
    return (fips or "").upper() in CHINA_REGION_FIPS
