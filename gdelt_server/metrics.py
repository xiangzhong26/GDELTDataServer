"""Query-time metrics derived exclusively from retained sufficient statistics.
Default coefficients preserve the DSI definitions; coverage is reported separately.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Sequence

from .cameo_names import CAMEO_NAMES_ZH
from .fips_names import CHINA_REGION_CAMEO, CHINA_REGION_FIPS, FIPS_NAMES_ZH
from .lookup import FIPS_TO_ISO3, ISO3_TO_FIPS
from .store import DAY, HOUR, Store, bucket_of, utcnow

# ============================================================
#  标签
# ============================================================

QUAD_LABELS = {1: "口头合作", 2: "实质合作", 3: "口头冲突", 4: "实质冲突"}

CAMEO_ROOT_LABELS = {
    "01": "公开声明", "02": "呼吁", "03": "表达合作意向", "04": "磋商",
    "05": "外交合作", "06": "物质合作", "07": "提供援助", "08": "让步",
    "09": "调查", "10": "提出要求", "11": "不赞成", "12": "拒绝",
    "13": "威胁", "14": "抗议", "15": "展示武力", "16": "降低关系",
    "17": "胁迫", "18": "袭击", "19": "战斗", "20": "非常规暴力",
}


def country_label(code: str | None) -> str:
    c = (code or "UNK").upper()
    return CAMEO_NAMES_ZH.get(c, "未识别国家/地区" if c == "UNK" else f"未识别地区（{c}）")


def fips_label(code: str | None) -> str:
    c = (code or "UNK").upper()
    return FIPS_NAMES_ZH.get(c, "未识别国家/地区" if c == "UNK" else f"未识别地区（{c}）")


def is_china_region(code: str | None) -> bool:
    return (code or "").upper() in CHINA_REGION_FIPS | {"CH"}


# ============================================================
#  可调参数
# ============================================================

@dataclass
class Params:
    # ── 对华态度：事件方向分的三项配比 ──
    w_goldstein: float = 0.70
    w_quad: float = 0.20
    w_tone: float = 0.10

    # ── 国家风险：CAMEO 大类归入哪个分项 ──
    roots_security: tuple[str, ...] = ("18", "19", "20")
    roots_social: tuple[str, ...] = ("14", "17")
    roots_political: tuple[str, ...] = ("10", "11", "12", "13", "16", "17")

    # ── 国家风险：饱和曲线尺度（见文件头 ②）──
    scale_security: float = 0.12
    scale_social: float = 0.10
    scale_political: float = 0.18

    # ── 国家风险：媒体负面分的除数（见文件头 ①）──
    tone_negative_divisor: float = 8.0

    # ── 国家风险：分项权重 ──
    w_cr_security: float = 0.35
    w_cr_social: float = 0.22
    w_cr_political: float = 0.18
    w_cr_media: float = 0.15
    w_cr_momentum: float = 0.10

    # ── 动量：最近 N 个完整日 vs 之前的均值（见文件头 ⑥）──
    momentum_recent_days: int = 1
    momentum_sensitivity: float = 25.0

    # ── 企业经营风险（GKG）分项权重 ──
    w_er_security: float = 0.24
    w_er_political: float = 0.20
    w_er_economic: float = 0.20
    w_er_infrastructure: float = 0.14
    w_er_social: float = 0.12
    w_er_health: float = 0.05
    w_er_negativity: float = 0.05

    # ── 证据充分度 ──
    ev_events_coef: float = 18.0
    ev_sources_coef: float = 8.0
    ev_docs_coef: float = 26.0

    # ── 最小样本：低于此值的国家不进排名 ──
    min_events: int = 20
    min_docs: int = 20

    @classmethod
    def load(cls, store: Store) -> "Params":
        saved = store.get_state("metric_params") or {}
        base = cls()
        for k, v in saved.items():
            if not hasattr(base, k):
                continue
            cur = getattr(base, k)
            try:
                if isinstance(cur, tuple):
                    setattr(base, k, tuple(str(x) for x in v))
                elif isinstance(cur, bool):
                    setattr(base, k, bool(v))
                elif isinstance(cur, int) and not isinstance(cur, bool):
                    setattr(base, k, int(v))
                else:
                    setattr(base, k, float(v))
            except (TypeError, ValueError):
                pass
        return base

    def save(self, store: Store) -> None:
        validate_params(self)
        store.set_state("metric_params", {k: (list(v) if isinstance(v, tuple) else v)
                                          for k, v in asdict(self).items()})

    def as_dict(self) -> dict[str, Any]:
        return {k: (list(v) if isinstance(v, tuple) else v) for k, v in asdict(self).items()}


# ============================================================
#  基础函数
# ============================================================

def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def saturation(rate: float | None, scale: float) -> float:
    """把一个非负占比映射到 0-100。scale 越小越容易饱和。"""
    if scale <= 0:
        return 0.0
    return clamp(100.0 * (1.0 - math.exp(-max(0.0, rate or 0.0) / scale)))


def evidence_events(n_events: int, n_sources: int, p: Params) -> float:
    return round(clamp(p.ev_events_coef * math.log1p(max(0, n_events))
                       + p.ev_sources_coef * math.log1p(max(0, n_sources))), 1)


def evidence_docs(n_docs: int, p: Params) -> float:
    """
    GKG 只有「文档数」一个样本量维度。原实现把它当成两个维度传了两遍
    （`evidence_score(total_docs, total_docs)`），数值上等于系数 26，
    这里直接写成单参数形式 —— 数值不变，口径不再自相矛盾。
    """
    return round(clamp(p.ev_docs_coef * math.log1p(max(0, n_docs))), 1)


def shrink_to_neutral(score: float, evidence: float) -> float:
    """样本量不足时把分数往 50 分（中性）拉，避免几条新闻就冲到 90 分。"""
    return clamp(50.0 + (score - 50.0) * clamp(evidence) / 100.0)


def percentile_scores(values: Sequence[float]) -> list[float]:
    """考虑并列的经验百分位，0-100。"""
    n = len(values)
    if n <= 1:
        return [50.0] * n
    ordered = sorted((v, i) for i, v in enumerate(values))
    out = [0.0] * n
    cur = 0
    while cur < n:
        end = cur
        while end + 1 < n and ordered[end + 1][0] == ordered[cur][0]:
            end += 1
        pct = 100.0 * ((cur + end) / 2.0) / (n - 1)
        for pos in range(cur, end + 1):
            out[ordered[pos][1]] = pct
        cur = end + 1
    return out


def country_score_components(a, p, momentum_value=None):
    """Shared formula for window rankings and historical bucket scores."""
    n = int(a["n_events"] or 0)
    total_w = a["sum_w"] or 1.0
    ev = evidence_events(n, int(a["sum_sources"]), p)
    security = shrink_to_neutral(saturation(a["w_sec"] / total_w, p.scale_security), ev)
    social = shrink_to_neutral(saturation(a["w_soc"] / total_w, p.scale_social), ev)
    political = shrink_to_neutral(saturation(a["w_pol"] / total_w, p.scale_political), ev)
    avg_tone = 10.0 * a["sum_tone"] / max(n, 1)
    media = shrink_to_neutral(clamp(max(0.0, -avg_tone) / max(p.tone_negative_divisor, 1e-6) * 100.0), ev)
    momentum = shrink_to_neutral(50.0 if momentum_value is None else momentum_value, ev)
    total = (p.w_cr_security*security+p.w_cr_social*social+p.w_cr_political*political+p.w_cr_media*media+p.w_cr_momentum*momentum)
    return security,social,political,avg_tone,media,momentum,total,ev


def enterprise_score_components(agg, p):
    """Same-bucket cross-country percentiles; never rank different dates together."""
    fields = (*BaseMetrics.ER_FIELDS, "negativity")
    codes = [c for c,a in agg.items() if not is_china_region(c) and a["total_docs"] >= p.min_docs]
    raw = {f:[] for f in fields}
    for c in codes:
        a=agg[c]; total=a["total_docs"] or 1.0
        for f in BaseMetrics.ER_FIELDS: raw[f].append(a[f"{f}_docs"]/total)
        raw["negativity"].append(max(0.0,-a["sum_tone"]/total))
    pct={f:percentile_scores(v) for f,v in raw.items()}
    weights={f:getattr(p,"w_er_"+f) for f in fields}
    result={}
    for i,c in enumerate(codes):
        ev=evidence_docs(int(agg[c]["total_docs"]),p)
        scores={f:shrink_to_neutral(pct[f][i],ev) for f in fields}
        result[c]=(scores,sum(weights[f]*scores[f] for f in fields),ev)
    return result


def momentum_from_coverage(day_counts,today,p,start,coverage):
    k=p.momentum_recent_days
    recent=list(range(today-k*DAY,today,DAY))
    if any(not coverage.get(d,{}).get("complete") for d in recent): return None
    base=[d for d in range(start,today-k*DAY,DAY) if coverage.get(d,{}).get("complete")]
    if not base:return None
    recent_avg=sum(day_counts.get(d,0) for d in recent)/k
    base_avg=max(1.,sum(day_counts.get(d,0) for d in base)/len(base))
    return clamp(50+p.momentum_sensitivity*(recent_avg/base_avg-1))


def risk_level(score: float) -> str:
    if score >= 75:
        return "极高"
    if score >= 60:
        return "高"
    if score >= 40:
        return "中"
    if score >= 20:
        return "较低"
    return "低"


def _iso(ts: int) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()


def _safe_div(a: float | None, b: float | None, default: float = 0.0) -> float:
    return (a or 0.0) / b if b else default


# ============================================================
#  时间窗口
# ============================================================

@dataclass
class Window:
    days: int
    granularity: str      # 读哪一档聚合：'hour' | 'day'
    series: str           # 趋势图的点粒度：'hour' | 'day'
    start: int
    end: int

    @property
    def size(self) -> int:
        return HOUR if self.series == "hour" else DAY


def make_window(days: int, now=None) -> Window:
    days = max(1, min(int(days), 1185))
    now = int((now if now is not None else utcnow()).timestamp())
    # ≤7 天用小时级（趋势图要看得出日内波动），更长一律用日级聚合：
    # 365 天如果走小时档，要扫 8760 × 国家数 × 大类数 行。
    gran = "hour" if days <= 7 else "day"
    start = bucket_of(now, gran) - ((days * 24 - 1) * HOUR if gran == "hour" else (days - 1) * DAY)
    return Window(days=days, granularity=gran, series=gran, start=start, end=now)


def _fill_series(points: dict[int, dict], win: Window,
                 blank: dict[str, Any]) -> list[dict[str, Any]]:
    """补齐缺失的时间点，前端画图不用自己处理断点。"""
    out = []
    step = win.size
    b = bucket_of(win.start, win.series)
    last = bucket_of(win.end, win.series)
    # 点数上限，防止 730 天 × 小时档意外撑爆
    guard = 0
    while b <= last and guard < 9000:
        row = points.get(b)
        out.append(row if row else {"bucket": b, "timestamp": _iso(b), **blank})
        b += step
        guard += 1
    return out


# ============================================================
#  指标说明
# ============================================================

def _explain(name: str, formula: str, source: str,
             caveat: str | None = None, unit: str = "") -> dict[str, str]:
    d = {"name": name, "formula": formula, "source": source, "unit": unit}
    if caveat:
        d["caveat"] = caveat
    return d


def metric_catalog(p: Params) -> dict[str, dict[str, str]]:
    """每一个对外暴露的指标都在这里有一条说明。前端「？」按钮直接读它。"""
    return {
        # ── 原始量 ──
        "event_count": _explain(
            "事件数",
            "GDELT Events 2.0 在窗口内识别出的事件记录条数。",
            "GDELT 2.0 Event Database，每 15 分钟更新一次。",
            "统计导出文件中的事件记录，不是逐篇报道数，也不是人工去重的现实事件数。"
            "NumMentions 才是事件的报道提及次数；事件抽取与媒体覆盖会影响记录数量。",
            "条"),
        "mentions": _explain(
            "提及次数",
            "该事件在 GDELT 监测的全部媒体中被提及的次数之和。",
            "Events 表 NumMentions 字段。",
            "只统计事件首次出现后 GDELT 持续跟踪到的提及，转载量大的新闻会显著放大。",
            "次"),
        "weight": _explain(
            "提及权重 w",
            "w = 1 + min(提及次数, 20) / 10，取值 1.0 ~ 3.0。",
            "由 NumMentions 计算。",
            "截尾上限 20 是为了防止一条被疯狂转载的新闻主导整个指数；"
            "该上限属于解析版本，不能仅靠聚合量修改历史权重。",
            ""),
        "goldstein": _explain(
            "Goldstein 冲突-合作分",
            "CAMEO 事件类型对应的固定分值，范围 -10（最冲突）到 +10（最合作）。",
            "Events 表 GoldsteinScale 字段。",
            "它由**事件类型**决定，同类事件不论规模大小取值相同：一次小规模"
            "示威和一次百万人游行的 Goldstein 值是一样的。",
            "分"),
        "quad_class": _explain(
            "四分类方向",
            "CAMEO 把事件分为口头合作(1)、实质合作(2)、口头冲突(3)、实质冲突(4)。"
            f"折算成方向值：1→+0.4，2→+1.0，3→-0.4，4→-1.0。",
            "Events 表 QuadClass 字段。",
            None, ""),
        "tone": _explain(
            "报道语调",
            "包含该事件的报道的平均情感语调，理论范围 -100 ~ +100，实际几乎都在 -20 ~ +20。",
            "Events 表 AvgTone 字段。",
            "Events 在聚合时先将每条语调截断到 ±10；页面平均语调是截断值的均值，"
            "不是原始 AvgTone 的均值。正负极端值混合时，两者可能显著不同。"
            "GKG 语调保留原值，两个来源的均值口径不同。",
            "分"),
        "evidence": _explain(
            "证据充分度",
            f"min(100, {p.ev_events_coef:g}·ln(1+事件数) + {p.ev_sources_coef:g}·ln(1+信源数))，0-100。",
            "由事件数与信源数计算。",
            "这是样本量启发式分数，不是统计置信度或采集覆盖率；信源数是逐事件"
            "NumSources 的累加，未对媒体去重。50条事件且累计信源50时，默认公式已达100。"
            "国家风险按它向50分收缩，企业风险另用文档数计算证据分。",
            "分"),

        # ── 对华态度 ──
        "attitude_score": _explain(
            "对华态度指数",
            f"事件方向分 = {p.w_goldstein:.0%}×Goldstein/10 + {p.w_quad:.0%}×四分类方向 "
            f"+ {p.w_tone:.0%}×语调/10；按提及权重 w 加权平均后 ×100，范围 -100 ~ +100。",
            "Events 表中 Actor2 为中国、Actor1 为该国的全部事件。",
            "衡量的是**媒体报道中呈现的行为倾向**，不是民意调查，也不是官方立场。"
            "英语媒体覆盖度远高于其他语种，小国样本稀疏时波动很大。",
            "分"),
        "conflict_share": _explain(
            "冲突事件占比",
            "四分类为口头冲突或实质冲突的事件数 / 事件总数 ×100。",
            "Events 表 QuadClass ∈ {3,4}。",
            None, "%"),

        # ── 国家风险 ──
        "risk_security": _explain(
            "安全冲突分",
            f"安全类事件（CAMEO 大类 {'/'.join(p.roots_security)}）的加权占比 r，"
            f"经饱和曲线 100×(1-e^(-r/{p.scale_security:g})) 映射到 0-100，再按证据充分度收缩。",
            "Events 表按 ActionGeo 国家聚合。",
            f"饱和尺度 {p.scale_security:g} 目前是经验取值，缺乏公开出处，属于待校准参数。",
            "分"),
        "risk_social": _explain(
            "社会稳定分",
            f"社会类事件（大类 {'/'.join(p.roots_social)}）加权占比经饱和曲线"
            f"（尺度 {p.scale_social:g}）映射，再按证据充分度收缩。",
            "同上。",
            "大类 17（胁迫）同时计入社会与政治两项，两个分项并非互斥。",
            "分"),
        "risk_political": _explain(
            "政治压力分",
            f"政治类事件（大类 {'/'.join(p.roots_political)}）加权占比经饱和曲线"
            f"（尺度 {p.scale_political:g}）映射，再按证据充分度收缩。",
            "同上。", None, "分"),
        "risk_media": _explain(
            "媒体负面分",
            f"max(0, -平均语调) / {p.tone_negative_divisor:g} × 100，截断到 0-100，再按证据充分度收缩。",
            "Events 表 AvgTone 按国家平均。",
            f"除数 {p.tone_negative_divisor:g} 是经验值。国家级平均语调极少低于 -3，"
            "因此该分项实际只在 0-40 分区间浮动，名义权重"
            f"{p.w_cr_media:.0%} 对总分的实际贡献明显小于标称值。这是已知口径问题。",
            "分"),
        "risk_momentum": _explain(
            "短期动量",
            f"50 + {p.momentum_sensitivity:g}×(最近 {p.momentum_recent_days} 个完整日的日均事件数 / "
            "之前各完整日的日均事件数 - 1)，截断到 0-100，再按证据充分度收缩。",
            "Events 表按日聚合。",
            "只用**完整自然日**比较。GDELT 有 15 分钟到数小时的入库延迟，"
            "当天数据天然残缺，因此不参与完整日比较。这里统计全部事件，合作报道增加"
            "也会提高动量；它表示报道活跃度变化，不等于风险恶化。尚未剔除周末效应。",
            "分"),
        "risk_score": _explain(
            "国家风险总分",
            f"{p.w_cr_security:.0%}安全 + {p.w_cr_social:.0%}社会 + {p.w_cr_political:.0%}政治 "
            f"+ {p.w_cr_media:.0%}媒体 + {p.w_cr_momentum:.0%}动量，0-100。",
            "上述五个分项。",
            "分数不参照其他国家，但属于尚未校准的事件构成评分，不是风险概率。"
            "跨时间比较需保持参数、统计窗口和覆盖口径一致；短期动量的基准随窗口变化。"
            "与下面的「企业经营风险」不是同一把尺子，两者的分数不可直接比大小。",
            "分"),

        # ── 企业经营风险（GKG）──
        "er_dimension": _explain(
            "经营风险分项",
            "该国报道中命中某类主题的文档占比，转换为**全部国家的横截面百分位**（0-100），"
            "再按证据充分度收缩。",
            "GDELT GKG 2.0 主题标签（GCAM/Themes 字段）。",
            "主题标签由 GDELT 的关键词规则生成，存在误标；本系统词表是主题领域识别，"
            "非官方风险分类。贸易、能源、医疗等普通主题也会命中，未逐篇判定负面风险。",
            "分"),
        "er_score": _explain(
            "企业经营风险总分",
            f"{p.w_er_security:.0%}安全 + {p.w_er_political:.0%}政策 + {p.w_er_economic:.0%}经济 "
            f"+ {p.w_er_infrastructure:.0%}基础设施 + {p.w_er_social:.0%}社会 "
            f"+ {p.w_er_health:.0%}健康 + {p.w_er_negativity:.0%}负面度，0-100。",
            "GKG 各主题文档密度。",
            "**相对口径**：分数是横截面百分位，各分项是横截面百分位，总分是这些分项的加权平均，不等于总分自身的排名百分位。"
            "换一批国家进来，同样的新闻会得到不同的分数，因此**不能跨时间比较**，"
            "也不能和上面的「国家风险总分」比大小。这是已知的口径不一致问题。",
            "分"),
        "china_business_docs": _explain(
            "涉中企报道数",
            "组织名字段中出现中资企业名录关键词的文档数。",
            "GKG Organizations 字段与内置中资企业别名表。",
            "别名表仅收录约 20 家头部企业，采用词边界匹配（避免把波兰城市 "
            "Bydgoszcz 误认为比亚迪），覆盖面有限，只能作为「相关度」的粗略信号。",
            "篇"),
        "negativity": _explain(
            "报道负面度",
            "max(0, -该国报道平均语调)，再转横截面百分位。",
            "GKG V2Tone 第 1 分量。", None, "分"),
    }


# ============================================================
#  查询
# ============================================================

class BaseMetrics:
    def __init__(self, store: Store):
        self.store = store

    def now(self):
        return self.store.read_time if hasattr(self.store, 'read_time') else utcnow()

    def window(self, days):
        return make_window(days, self.now())

    @staticmethod
    def momentum_window(win):
        # Momentum compares whole UTC days, independently of the hourly chart window.
        return Window(win.days, 'day', 'day', bucket_of(win.start, 'day'),
                      bucket_of(win.end, 'day'))

    def params(self) -> Params:
        return Params.load(self.store)

    # ── 通用：按窗口取聚合行 ────────────────────────────────

    # 聚合一律下推到 SQLite。
    #
    # 最初这里是把窗口内的所有聚合行读进 Python 再累加，90 天窗口要
    # 材料化二十几万行，单次请求 1.5 秒起步，真实数据量下还要再翻几倍。
    # 换成 GROUP BY 之后同样的窗口只回几十行，快两个数量级，
    # 而且内存占用不再随窗口长度增长。

    _REL_SUMS = ", ".join(f"SUM({m}) {m}" for m in (
        "n_events", "sum_mentions", "sum_sources", "sum_articles", "sum_w",
        "sum_gold_w", "sum_quad_w", "sum_tone_w", "sum_gold", "sum_tone",
        "sum_absgold_w", "n_quad1", "n_quad2", "n_quad3", "n_quad4"))
    _GEO_SUMS = ", ".join(f"SUM({m}) {m}" for m in (
        "n_events", "sum_mentions", "sum_sources", "sum_w",
        "sum_gold_w", "sum_tone_w", "sum_gold", "sum_tone",
        "n_quad1", "n_quad2", "n_quad3", "n_quad4"))
    _GKG_SUMS = ", ".join(f"SUM({m}) {m}" for m in (
        "total_docs", "security_docs", "political_docs", "economic_docs",
        "infrastructure_docs", "social_docs", "health_docs",
        "china_business_docs", "sum_tone", "sum_polarity"))

    def _agg(self, table: str, sums: str, group: str, win: Window,
             where: str = "1=1", args: Sequence = (),
             select_extra: str = "", head_args: Sequence = ()) -> list[dict]:
        """
        SELECT 子句里也可能出现 `?`（CASE WHEN root_code IN (?,?,?) 这种），
        而 SQLite 是**按出现顺序**绑定参数的。所以 SELECT 里的参数要单独走
        head_args，排在 WHERE 的 granularity/bucket 之前，顺序不能弄反。
        """
        cols = f"{group}, {sums}" if group else sums
        if select_extra:
            cols = f"{select_extra}, {cols}" if group else f"{select_extra}, {sums}"
        sql = (f"SELECT {cols} FROM {table} "
               f"WHERE granularity=? AND bucket>=? AND bucket<? AND {where}"
               + (f" GROUP BY {group}" if group else ""))
        params = (*head_args, win.granularity, win.start, win.end, *args)
        with self.store.connect() as db:
            return [dict(r) for r in db.execute(sql, params)]

    # ========================================================
    #  1. 对华态度
    # ========================================================

    def attitude(self, days: int = 30, country: str = "USA") -> dict[str, Any]:
        p = self.params()
        win = self.window(days)
        country = (country or "USA").upper()
        base = "actor2='CHN' AND actor1<>'CHN'"
        by_country = {r["actor1"]: r for r in self._agg(
            "agg_relation", self._REL_SUMS, "actor1", win, base)}
        by_bucket = {r["bucket"]: r for r in self._agg(
            "agg_relation", self._REL_SUMS, "bucket", win,
            base + " AND actor1=?", (country,))}
        by_root = {r["root_code"]: r for r in self._agg(
            "agg_relation", self._REL_SUMS, "root_code", win,
            base + " AND actor1=?", (country,))}

        def score(acc: dict) -> float:
            return 100.0 * _safe_div(
                p.w_goldstein * acc["sum_gold_w"] + p.w_quad * acc["sum_quad_w"]
                + p.w_tone * acc["sum_tone_w"], acc["sum_w"])

        countries = []
        for code, acc in by_country.items():
            if acc["n_events"] < 1 or code in CHINA_REGION_CAMEO:
                continue
            n = acc["n_events"]
            countries.append({
                "code": code, "fips": ISO3_TO_FIPS.get(code, ""),
                "name": country_label(code),
                "event_count": int(n),
                "source_count": int(acc["sum_sources"]),
                "mentions": int(acc["sum_mentions"]),
                "attitude_score": round(score(acc), 1),
                "avg_goldstein": round(10.0 * acc["sum_gold"] / n, 2),
                "avg_tone": round(10.0 * acc["sum_tone"] / n, 2),
                "conflict_share": round(100.0 * (acc["n_quad3"] + acc["n_quad4"]) / n, 1),
                "evidence": evidence_events(n, int(acc["sum_sources"]), p),
                "thin": n < p.min_events,
            })
        countries.sort(key=lambda r: r["event_count"], reverse=True)

        pts = {}
        for b, acc in by_bucket.items():
            n = max(acc["n_events"], 1)
            pts[b] = {
                "bucket": b, "timestamp": _iso(b),
                "event_count": int(acc["n_events"]),
                "mentions": int(acc["sum_mentions"]),
                "attitude_score": round(score(acc), 1),
                "avg_goldstein": round(10.0 * acc["sum_gold"] / n, 2),
                "avg_tone": round(10.0 * acc["sum_tone"] / n, 2),
            }
        series = _fill_series(pts, win, {"event_count": 0, "mentions": 0,
                                         "attitude_score": None,
                                         "avg_goldstein": None, "avg_tone": None})

        cloud = sorted(
            [{"code": rc, "label": CAMEO_ROOT_LABELS.get(rc, f"CAMEO {rc}"),
              "count": int(acc["n_events"]),
              "impact": round(acc["sum_absgold_w"] * 10.0, 1),
              "avg_goldstein": round(10.0 * acc["sum_gold"] / max(acc["n_events"], 1), 2)}
             for rc, acc in by_root.items()],
            key=lambda r: r["count"], reverse=True)

        return {
            "generated_at": self.now().isoformat(timespec="seconds"),
            "window": {"days": win.days, "granularity": win.series},
            "countries": countries,
            "selected": next((r for r in countries if r["code"] == country), None),
            "series": series,
            "event_types": cloud,
            "metrics": _subset(metric_catalog(p),
                               ("attitude_score", "conflict_share", "goldstein",
                                "quad_class", "tone", "mentions", "weight",
                                "evidence", "event_count")),
        }

    # ========================================================
    #  2. 国家风险（Events / ActionGeo）
    # ========================================================

    def country_risk(self, days: int = 30, country: str = "US") -> dict[str, Any]:
        p = self.params()
        win = self.window(days)
        country = (country or "US").upper()
        def _in(codes) -> str:
            # root_code 是我们自己生成的两位数字串，但仍然走参数化，
            # 免得以后有人把它接成用户输入。
            return ",".join("?" * len(codes)) or "''"

        sec, soc, pol = (tuple(p.roots_security), tuple(p.roots_social),
                         tuple(p.roots_political))
        cond = (f"SUM(CASE WHEN root_code IN ({_in(sec)}) THEN sum_w ELSE 0 END) w_sec, "
                f"SUM(CASE WHEN root_code IN ({_in(soc)}) THEN sum_w ELSE 0 END) w_soc, "
                f"SUM(CASE WHEN root_code IN ({_in(pol)}) THEN sum_w ELSE 0 END) w_pol, "
                "SUM(n_events) n_events, SUM(sum_sources) sum_sources, "
                "SUM(sum_w) sum_w, SUM(sum_tone) sum_tone, "
                "SUM(n_quad3) + SUM(n_quad4) conflict")
        args = (*sec, *soc, *pol)

        agg = {r["geo_country"]: r for r in self._agg(
            "agg_geo", cond, "geo_country", win, head_args=args)
            if not is_china_region(r["geo_country"])}

        # 动量只需要「国家 × 自然日」的事件数，不必把明细拉出来
        daily: dict[str, dict[int, float]] = {}
        for r in self._agg("agg_geo", "SUM(n_events) n_events",
                           f"geo_country, bucket - bucket % {DAY}", self.momentum_window(win),
                           select_extra=f"bucket - bucket % {DAY} AS day"):
            daily.setdefault(r["geo_country"], {})[r["day"]] = r["n_events"]

        sel_series = {}
        for r in self._agg(
                "agg_geo",
                f"SUM(n_events) event_count, "
                f"SUM(CASE WHEN root_code IN ({_in(sec)}) THEN n_events ELSE 0 END) security, "
                f"SUM(n_quad3) + SUM(n_quad4) conflict, SUM(sum_tone) sum_tone",
                "bucket", win, "geo_country=?", (country,), head_args=sec):
            sel_series[r["bucket"]] = {
                "bucket": r["bucket"], "timestamp": _iso(r["bucket"]),
                "event_count": int(r["event_count"] or 0),
                "security": int(r["security"] or 0),
                "conflict": int(r["conflict"] or 0),
                "sum_tone": r["sum_tone"] or 0.0}

        today = bucket_of(int(self.now().timestamp()), "day")

        out = []
        for code, a in agg.items():
            n = int(a["n_events"] or 0)
            if n < p.min_events:
                continue
            security,social,political,avg_tone,media,momentum,total,ev = country_score_components(a,p,self._momentum(daily.get(code,{}),today,p))
            out.append({
                "code": code, "iso3": FIPS_TO_ISO3.get(code, ""),
                "name": fips_label(code),
                "event_count": int(n), "source_count": int(a["sum_sources"]),
                "avg_tone": round(avg_tone, 2),
                "conflict_share": round(100.0 * a["conflict"] / n, 1),
                "risk_score": round(total, 1), "risk_level": risk_level(total),
                "security": round(security, 1), "social": round(social, 1),
                "political": round(political, 1), "media": round(media, 1),
                "momentum": round(momentum, 1),
                "evidence": ev, "thin": n < p.min_events,
            })
        out.sort(key=lambda r: r["risk_score"], reverse=True)
        for i, r in enumerate(out, 1):
            r["rank"] = i

        for s in sel_series.values():
            n = max(s["event_count"], 1)
            s["avg_tone"] = round(10.0 * s.pop("sum_tone") / n, 2)
        series = _fill_series(sel_series, win,
                              {"event_count": 0, "security": 0, "conflict": 0,
                               "avg_tone": None})

        return {
            "generated_at": self.now().isoformat(timespec="seconds"),
            "window": {"days": win.days, "granularity": win.series},
            "countries": out,
            "selected": next((r for r in out if r["code"] == country), None),
            "series": series,
            "metrics": _subset(metric_catalog(p),
                               ("risk_score", "risk_security", "risk_social",
                                "risk_political", "risk_media", "risk_momentum",
                                "evidence", "event_count", "tone", "weight")),
        }

    @staticmethod
    def _momentum(day_counts: dict[int, float], today: int, p: Params) -> float:
        """
        只用完整自然日。当天（today）永远排除在外 —— GDELT 有入库延迟，
        当天桶必然残缺，把它算进来会让所有国家的动量系统性偏低。
        """
        days = sorted(d for d in day_counts if d < today)
        if len(days) < 2:
            return 50.0
        k = max(1, min(int(p.momentum_recent_days), len(days) - 1))
        recent = days[-k:]
        base = days[:-k]
        recent_avg = sum(day_counts[d] for d in recent) / len(recent)
        base_avg = sum(day_counts[d] for d in base) / len(base)
        if base_avg < 1.0:
            base_avg = 1.0
        return clamp(50.0 + p.momentum_sensitivity * (recent_avg / base_avg - 1.0))

    # ========================================================
    #  3. 企业经营风险（GKG）
    # ========================================================

    ER_FIELDS = ("security", "political", "economic",
                 "infrastructure", "social", "health")

    def enterprise_risk(self, days: int = 30, country: str = "US") -> dict[str, Any]:
        p = self.params()
        win = self.window(days)
        country = (country or "US").upper()
        agg = {r["country"]: r for r in self._agg(
            "agg_gkg", self._GKG_SUMS, "country", win)
            if not is_china_region(r["country"])}

        sel = {}
        for r in self._agg("agg_gkg", self._GKG_SUMS, "bucket", win,
                           "country=?", (country,)):
            sel[r["bucket"]] = {
                "bucket": r["bucket"], "timestamp": _iso(r["bucket"]),
                "total_docs": int(r["total_docs"] or 0),
                "china_business_docs": int(r["china_business_docs"] or 0),
                "sum_tone": r["sum_tone"] or 0.0,
                **{f"{f}_docs": int(r[f"{f}_docs"] or 0) for f in self.ER_FIELDS}}

        scored = enterprise_score_components(agg,p)
        codes = list(scored)

        out = []
        for i, c in enumerate(codes):
            a = agg[c]
            docs = int(a["total_docs"])
            scores,total,ev = scored[c]
            row = {
                "code": c, "iso3": FIPS_TO_ISO3.get(c, ""),
                "name": fips_label(c),
                "total_docs": docs,
                "china_business_docs": int(a["china_business_docs"]),
                "avg_tone": round(a["sum_tone"] / max(docs, 1), 2),
                "avg_polarity": round(a["sum_polarity"] / max(docs, 1), 2),
                "risk_score": round(total, 1), "risk_level": risk_level(total),
                "evidence": ev, "thin": docs < p.min_docs,
            }
            for f in (*self.ER_FIELDS, "negativity"):
                row[f] = round(scores[f], 1)
                if f in self.ER_FIELDS:
                    row[f"{f}_docs"] = int(a[f"{f}_docs"])
            out.append(row)
        out.sort(key=lambda r: r["risk_score"], reverse=True)
        for i, r in enumerate(out, 1):
            r["rank"] = i

        for s in sel.values():
            s["avg_tone"] = round(s.pop("sum_tone") / max(s["total_docs"], 1), 2)
        series = _fill_series(sel, win,
                              {"total_docs": 0, "china_business_docs": 0,
                               "avg_tone": None,
                               **{f"{f}_docs": 0 for f in self.ER_FIELDS}})

        return {
            "generated_at": self.now().isoformat(timespec="seconds"),
            "window": {"days": win.days, "granularity": win.series},
            "countries": out,
            "selected": next((r for r in out if r["code"] == country), None),
            "series": series,
            "metrics": _subset(metric_catalog(p),
                               ("er_score", "er_dimension", "china_business_docs",
                                "negativity", "evidence")),
        }

    # ========================================================
    #  4. 总览
    # ========================================================

    def overview(self, days: int = 7, view: str = "china",
                 country: str = "USA", window: Window | None = None) -> dict[str, Any]:
        p = self.params()
        win = window or self.window(days)
        country = (country or "USA").upper()

        if view == "china":
            where, args = "actor2='CHN' AND actor1<>'CHN'", ()
            title = "各国家/地区 → 中国"
            direction = "Actor1（各国）→ Actor2（中国）"
        else:
            where, args = "actor1=? AND actor2<>?", (country, country)
            title = f"{country_label(country)} → 各国家/地区"
            direction = f"Actor1（{country_label(country)}）→ Actor2"

        partner_col = "actor1" if view == "china" else "actor2"

        total_rows = self._agg("agg_relation", self._REL_SUMS, "", win, where, args)
        total = total_rows[0] if total_rows else {}
        total = {k: (total.get(k) or 0) for k in _REL_KEYS}

        partners = {r[partner_col]: r for r in self._agg(
            "agg_relation", self._REL_SUMS, partner_col, win, where, args)
            if r[partner_col] not in CHINA_REGION_CAMEO}
        buckets = {r["bucket"]: r for r in self._agg(
            "agg_relation", self._REL_SUMS, "bucket", win, where, args)}
        roots = {r["root_code"]: r for r in self._agg(
            "agg_relation", self._REL_SUMS, "root_code", win, where, args)}
        quads = {q: int(total[f"n_quad{q}"] or 0) for q in (1, 2, 3, 4)}

        n = max(total["n_events"], 1)
        summary = {
            "event_count": int(total["n_events"]),
            "country_count": len(partners),
            "total_mentions": int(total["sum_mentions"]),
            "total_sources": int(total["sum_sources"]),
            "total_articles": int(total["sum_articles"]),
            "avg_goldstein": round(10.0 * total["sum_gold"] / n, 2),
            "avg_tone": round(10.0 * total["sum_tone"] / n, 2),
            "attitude_score": round(100.0 * _safe_div(
                p.w_goldstein * total["sum_gold_w"] + p.w_quad * total["sum_quad_w"]
                + p.w_tone * total["sum_tone_w"], total["sum_w"]), 1),
        }

        pts = {}
        for b, acc in buckets.items():
            m = max(acc["n_events"], 1)
            pts[b] = {"bucket": b, "timestamp": _iso(b),
                      "attitude_score": round(100*_safe_div(p.w_goldstein*acc['sum_gold_w']+p.w_quad*acc['sum_quad_w']+p.w_tone*acc['sum_tone_w'],acc['sum_w']),1) if acc['n_events'] else None,
                      "event_count": int(acc["n_events"]),
                      "mentions": int(acc["sum_mentions"]),
                      "avg_goldstein": round(10.0 * acc["sum_gold"] / m, 2),
                      "avg_tone": round(10.0 * acc["sum_tone"] / m, 2)}
        series = _fill_series(pts, win, {"event_count": 0, "mentions": 0,"attitude_score":None,
                                         "avg_goldstein": None, "avg_tone": None})

        partner_rows = sorted(
            [{"code": c, "name": country_label(c),
              "count": int(a["n_events"]), "mentions": int(a["sum_mentions"]),
              "avg_goldstein": round(10.0 * a["sum_gold"] / max(a["n_events"], 1), 2),
              "avg_tone": round(10.0 * a["sum_tone"] / max(a["n_events"], 1), 2),
              "conflict_count": int(a["n_quad3"] + a["n_quad4"]),
              "conflict_share": round(100.0 * (a["n_quad3"] + a["n_quad4"])
                                      / max(a["n_events"], 1), 1)}
             for c, a in partners.items()],
            key=lambda r: r["count"], reverse=True)

        root_rows = sorted(
            [{"code": rc, "label": CAMEO_ROOT_LABELS.get(rc, f"CAMEO {rc}"),
              "count": int(a["n_events"]), "mentions": int(a["sum_mentions"]),
              "avg_goldstein": round(10.0 * a["sum_gold"] / max(a["n_events"], 1), 2)}
             for rc, a in roots.items()],
            key=lambda r: r["count"], reverse=True)[:12]

        return {
            "generated_at": self.now().isoformat(timespec="seconds"),
            "window": {"days": win.days, "granularity": win.series},
            "view": {"mode": view, "country": "CHN" if view == "china" else country,
                     "country_name": "中国" if view == "china" else country_label(country),
                     "title": title, "direction": direction},
            "summary": summary,
            "series": series,
            "countries": partner_rows,
            "event_roots": root_rows,
            "quad_classes": [{"quad_class": q, "label": QUAD_LABELS[q], "count": c}
                             for q, c in sorted(quads.items())],
            "metrics": _subset(metric_catalog(p),
                               ("event_count", "mentions", "goldstein", "tone",
                                "quad_class", "attitude_score", "conflict_share")),
        }

    # ========================================================
    #  5. 给 AI 用的快照（只有聚合数字，没有任何原文）
    # ========================================================

    def ai_snapshot(self, country_iso3: str | None = None, fips: str | None = None,
                    days: int = 30, top: int = 8) -> dict[str, Any]:
        """
        提供给大模型的数据切片。

        **刻意只给聚合量和 CAMEO 事件大类标签**，不给任何文章标题、正文、
        组织名或 URL。明细面板因为政治敏感内容风险默认隐藏，同样的顾虑对
        模型更严重：模型会把读到的原文复述、改写进生成的报告和 PDF 里。
        这里给出的最细粒度就是「大类 19（战斗）占该国事件的 8.2%」。
        """
        p = self.params()
        cr = self.country_risk(days=days, country=fips or "US")
        er = self.enterprise_risk(days=days, country=fips or "US")
        att = self.attitude(days=days, country=country_iso3 or "USA")

        def slim_rank(rows: list[dict], keys: Sequence[str], n: int) -> list[dict]:
            return [{k: r.get(k) for k in keys} for r in rows[:n]]

        out: dict[str, Any] = {
            "data_source": "GDELT 2.0（Events + GKG），每 15 分钟更新",
            "window_days": days,
            "generated_at": self.now().isoformat(timespec="seconds"),
            "note": "以下全部为聚合统计量，不含任何新闻原文、标题、机构名或链接。",
            "country_risk_top": slim_rank(
                cr["countries"], ("code", "name", "risk_score", "risk_level",
                                  "security", "social", "political", "media",
                                  "momentum", "event_count", "evidence"), top),
            "enterprise_risk_top": slim_rank(
                er["countries"], ("code", "name", "risk_score", "risk_level",
                                  "security", "political", "economic",
                                  "infrastructure", "social", "health",
                                  "total_docs", "evidence"), top),
            "attitude_top": slim_rank(
                att["countries"], ("code", "name", "attitude_score",
                                   "conflict_share", "event_count", "evidence"), top),
        }
        if fips:
            out["focus_country_risk"] = cr.get("selected")
            out["focus_enterprise_risk"] = er.get("selected")
            out["focus_event_mix"] = [
                {"code": r["code"], "label": r["label"], "count": r["count"]}
                for r in att.get("event_types", [])[:8]
            ]
        if country_iso3:
            out["focus_attitude"] = att.get("selected")
        out["caveats"] = [
            "GDELT 统计的是「被媒体报道的事件」，不等于实际发生的事件，英语媒体覆盖度显著更高。",
            "样本证据充分度不代表时间覆盖完整，必须同时检查coverage和数据截至时间。",
            "国家风险总分是绝对口径（可跨时间比较），企业经营风险总分是横截面百分位"
            "（只能同一时点跨国比较），两者不可直接比大小。",
        ]
        return out

    # ========================================================
    #  明细（默认不对外开放，由 API 层的开关控制）
    # ========================================================

    # ── 地图/下拉用的码表 ──
    def code_reference(self) -> dict[str, list[dict[str, str]]]:
        return {
            "fips": [{"code": c, "name": n, "type": "国家/地区"}
                     for c, n in sorted(FIPS_NAMES_ZH.items())
                     if c not in CHINA_REGION_FIPS],
            "cameo": [{"code": c, "name": n} for c, n in sorted(CAMEO_NAMES_ZH.items())
                      if c not in CHINA_REGION_CAMEO],
            "cameo_roots": [{"code": c, "name": n}
                            for c, n in sorted(CAMEO_ROOT_LABELS.items())],
        }


# ============================================================
#  内部小工具
# ============================================================

_REL_KEYS = ("n_events", "sum_mentions", "sum_sources", "sum_articles", "sum_w",
             "sum_gold_w", "sum_quad_w", "sum_tone_w", "sum_gold", "sum_tone",
             "sum_absgold_w", "n_quad1", "n_quad2", "n_quad3", "n_quad4")


def _zero_rel() -> dict[str, float]:
    return {k: 0.0 for k in _REL_KEYS}


def _add_rel(acc: dict, row: dict) -> None:
    for k in _REL_KEYS:
        v = row.get(k)
        if v:
            acc[k] += v


def _subset(catalog: dict[str, dict], keys: Iterable[str]) -> dict[str, dict]:
    return {k: catalog[k] for k in keys if k in catalog}


def validate_params(p: Params):
    for name, value in asdict(p).items():
        if name.startswith("roots_"):
            if not value or len(set(value)) != len(value) or any(v not in CAMEO_ROOT_LABELS for v in value):
                raise ValueError(f"{name} 必须是不重复的01至20事件大类")
        elif not isinstance(value, (float, int)) or not math.isfinite(value) or value < 0:
            raise ValueError(f"{name} 必须是有限非负数字")
    groups = [("w_goldstein", "w_quad", "w_tone"),
              ("w_cr_security", "w_cr_social", "w_cr_political", "w_cr_media", "w_cr_momentum"),
              tuple(k for k in asdict(p) if k.startswith("w_er_"))]
    for group in groups:
        if not math.isclose(sum(getattr(p, k) for k in group), 1, abs_tol=1e-6):
            raise ValueError(f"权重之和必须为1：{','.join(group)}")
    for key in ("scale_security", "scale_social", "scale_political", "tone_negative_divisor"):
        if getattr(p, key) <= 0:
            raise ValueError(f"{key} 必须大于0")
    if not 1 <= p.momentum_recent_days <= 30 or p.min_docs < 1 or p.min_events < 1:
        raise ValueError("近期天数范围1至30，最小样本必须至少为1")


class Metrics(BaseMetrics):
    def params(self):
        p = super().params()
        validate_params(p)
        return p

    def _momentum(self, day_counts, today, p):
        value = self.momentum_value(day_counts, today, p)
        return 50.0 if value is None else value

    def momentum_value(self, day_counts, today, p):
        k = p.momentum_recent_days
        start = getattr(self, "_momentum_window_start", min(day_counts) if day_counts else today)
        if start >= today:
            return None
        cache = getattr(self, "_momentum_coverage", {})
        version = getattr(self, '_query_version', None)
        key = (start, today, self.store.get_state("data_version", 0) if version is None else version)
        if key not in cache:
            cache[key] = self.store.coverage_buckets("events", start, today, DAY)
        self._momentum_coverage = cache
        coverage = cache[key]
        return momentum_from_coverage(day_counts,today,p,start,coverage)

    def decorate(self, result, days, source, fields, window=None):
        win = window or self.window(days)
        coverage = self.store.coverage(source, win.start, win.end)
        buckets = self.store.coverage_buckets(source, win.start, win.end, win.size)
        result["coverage"] = coverage
        result["data_version"] = self.store.get_state("data_version", 0)
        result["parameter_version"] = self.store.get_state("parameter_version", 0)
        result["parser_version"] = "aggregate-v2"
        result["window"].update(start=_iso(win.start), end=_iso(win.end), timezone="UTC",
                                definition="含当前未完整桶的最近自然日/小时桶")
        for point in result.get("series", []):
            c = buckets.get(point["bucket"], {"expected": 0, "done": 0, "complete": False})
            point["complete"] = c["complete"]
            point["collected_files"] = c["done"]
            point["expected_files"] = c["expected"]
            if not c["done"]:
                for field in fields:
                    if field in point:
                        point[field] = None
        result["quality_note"] = "证据充分度衡量样本量，时间覆盖率单独显示；缺采集点为空，部分采集点标记为不完整。"
        return result

    def overview(self, days=7, view="china", country="USA", window=None):
        return self.decorate(super().overview(days, view, country, window), days, "events",
                             ("event_count", "mentions", "attitude_score", "avg_goldstein", "avg_tone"), window)

    def attitude(self, days=30, country="USA"):
        return self.decorate(super().attitude(days, country), days, "events",
                             ("event_count", "mentions", "attitude_score", "avg_goldstein", "avg_tone"))

    def country_risk(self, days=30, country="US"):
        self._query_version = self.store.get_state('data_version', 0)
        self._momentum_coverage = {}
        self._momentum_window_start = bucket_of(self.window(days).start, "day")
        result = super().country_risk(days, country)
        win, p = self.window(days), self.params()
        daily = {}
        for r in self._agg("agg_geo", "SUM(n_events) n_events", f"geo_country, bucket-bucket%{DAY}", self.momentum_window(win),
                           select_extra=f"bucket-bucket%{DAY} AS day"):
            daily.setdefault(r["geo_country"], {})[r["day"]] = r["n_events"]
        today = bucket_of(int(self.now().timestamp()), "day")
        for row in result["countries"]:
            row["momentum_available"] = self.momentum_value(daily.get(row["code"], {}), today, p) is not None
        result["minimum_events"] = p.min_events
        result["momentum_note"] = "近期完整日或基线不足时，动量用50中性值参与计算，并明确标记不可用。"
        return self.decorate(result, days, "events", ("event_count", "security", "conflict", "avg_tone"))

    def enterprise_risk(self, days=30, country="US"):
        result = super().enterprise_risk(days, country)
        result["minimum_docs"] = self.params().min_docs
        result["percentile_reference_count"] = len(result["countries"])
        return self.decorate(result, days, "gkg", ("total_docs", "china_business_docs", "avg_tone",
                                                    *[f"{f}_docs" for f in self.ER_FIELDS]))
