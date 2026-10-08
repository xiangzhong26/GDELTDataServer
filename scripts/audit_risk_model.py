"""Read-only model diagnostics using production formulas, without fetching data.

Run from the repository: python scripts/audit_risk_model.py
Optional: --annual-csv /path/to/DSI/data/risk_monthly/risk_index_2025.csv
The synthetic examples diagnose formula behavior, not real country risk.
"""
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import statistics
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from gdelt_server.metrics import (
    Params, country_score_components, evidence_docs, evidence_events,
    momentum_from_coverage, percentile_scores, saturation,
)
from gdelt_server.parser import gkg_categories
from gdelt_server.store import DAY, tone_unit


def diagnose():
    p = Params()
    names = ("security", "social", "political", "avg_tone", "media",
             "momentum", "total", "evidence")
    roots = {}
    for root in ("15", "17", "19"):
        aggregate = dict(n_events=100, sum_sources=100, sum_w=100, sum_tone=0,
                         w_sec=100 if root in p.roots_security else 0,
                         w_soc=100 if root in p.roots_social else 0,
                         w_pol=100 if root in p.roots_political else 0)
        roots[root] = dict(zip(names, (round(v, 4) for v in
                                     country_score_components(aggregate, p))))
    return {
        "scope": "synthetic examples; default Params; not production country validation",
        "single_root_examples": roots,
        "security_saturation": {str(r): round(saturation(r, p.scale_security), 4)
                                for r in (.1, .3, .5, .8)},
        "evidence": {"events_50_sources_50": evidence_events(50, 50, p),
                     "documents_46": evidence_docs(46, p)},
        "small_difference_percentiles": percentile_scores([.0100, .0101]),
        "ordinary_theme_categories": gkg_categories("ECON_TRADE;ENERGY;MEDICAL;"),
        "tone_example": {"raw_average": statistics.mean([-100, 9]),
                         "retained_average": round(10 * statistics.mean(
                             [tone_unit(t) for t in (-100, 9)]), 4)},
        "all_event_count_momentum": momentum_from_coverage(
            {0: 100, DAY: 100, 2 * DAY: 300}, 3 * DAY, p, 0,
            {d: {"complete": True} for d in (0, DAY, 2 * DAY)}),
    }


def diagnose_reference(path, month):
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        # Original combined CSV also contains repeated header rows.
        rows = [r for r in csv.DictReader(stream)
                if r.get("Month") == str(month) and r.get("Country") != "Country"]
    if not rows or len({r["Country"] for r in rows}) != len(rows):
        raise ValueError("Reference month must contain one row per country")
    weights = {"Social_Stability_Risk": .30, "Political_Institutional_Risk": .25,
               "International_Conflict_Risk": .25, "Media_Sentiment_Risk": .20}
    raw = [sum(float(row[k]) * w for k, w in weights.items()) for row in rows]
    mean, std = statistics.mean(raw), statistics.pstdev(raw)
    reconstructed = [min(100, max(0, 50 + (v - mean) / std * 100 / 6))
                     if std else v for v in raw]
    errors = [abs(float(row["General_Risk_Index"]) - v)
              for row, v in zip(rows, reconstructed)]
    return {"month": month, "countries": len(rows), "raw_mean": mean,
            "raw_std": std, "max_normalization_reconstruction_error": max(errors),
            "mean_normalization_reconstruction_error": statistics.mean(errors),
            "scope": "checks arithmetic of published components, not raw-data accuracy"}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--annual-csv", type=Path)
    parser.add_argument("--month", type=int, default=1, choices=range(1, 13))
    args = parser.parse_args()
    output = diagnose()
    if args.annual_csv:
        output["reference"] = diagnose_reference(args.annual_csv, args.month)
    print(json.dumps(output, ensure_ascii=False, indent=2, allow_nan=False))
