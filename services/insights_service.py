from collections import defaultdict
from typing import Any, Dict, List, Optional


def category_breakdown(expenses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_cat: Dict[str, float] = defaultdict(float)
    for e in expenses:
        by_cat[str(e.get("category") or "Uncategorized")] += float(e.get("amount") or 0.0)
    rows = [{"category": k, "total": round(v, 2)} for k, v in by_cat.items()]
    rows.sort(key=lambda x: x["total"], reverse=True)
    return rows


def monthly_trend(expenses: List[Dict[str, Any]], months_back: int = 12) -> List[Dict[str, Any]]:
    buckets: Dict[str, float] = defaultdict(float)
    for e in expenses:
        d = e.get("date")
        if not d or not isinstance(d, str) or len(d) < 7:
            continue
        buckets[d[:7]] += float(e.get("amount") or 0.0)
    months = sorted(buckets.keys())
    if months_back > 0:
        months = months[-months_back:]
    return [{"month": m, "total": round(buckets[m], 2)} for m in months]


def growth_trends(expenses: List[Dict[str, Any]]) -> Dict[str, Any]:
    mt = monthly_trend(expenses, months_back=18)
    if len(mt) < 2:
        return {
            "monthOverMonthGrowthPct": 0.0,
            "spendingSpikeDetected": False,
            "fastestGrowingCategory": None,
        }

    prev_total = float(mt[-2]["total"])
    curr_total = float(mt[-1]["total"])
    growth_pct = ((curr_total - prev_total) / prev_total * 100.0) if prev_total > 0 else 0.0

    # Category growth by comparing latest month against previous month.
    prev_month = mt[-2]["month"]
    curr_month = mt[-1]["month"]
    prev_cat: Dict[str, float] = defaultdict(float)
    curr_cat: Dict[str, float] = defaultdict(float)
    for e in expenses:
        month = str(e.get("date") or "")[:7]
        cat = str(e.get("category") or "Uncategorized")
        amt = float(e.get("amount") or 0.0)
        if month == prev_month:
            prev_cat[cat] += amt
        elif month == curr_month:
            curr_cat[cat] += amt

    fastest = None
    max_growth = float("-inf")
    for cat in set(prev_cat.keys()) | set(curr_cat.keys()):
        p = float(prev_cat.get(cat, 0.0))
        c = float(curr_cat.get(cat, 0.0))
        if p <= 0 and c > 0:
            gpct = 100.0
        elif p <= 0:
            gpct = 0.0
        else:
            gpct = ((c - p) / p) * 100.0
        if gpct > max_growth:
            max_growth = gpct
            fastest = {
                "category": cat,
                "growthPct": round(gpct, 2),
                "previousMonthTotal": round(p, 2),
                "currentMonthTotal": round(c, 2),
            }

    return {
        "monthOverMonthGrowthPct": round(growth_pct, 2),
        "spendingSpikeDetected": growth_pct >= 20.0,
        "fastestGrowingCategory": fastest,
    }


def risk_score(expenses: List[Dict[str, Any]], anomalies: List[Dict[str, Any]], growth: Dict[str, Any]) -> int:
    total = max(1.0, sum(float(e.get("amount") or 0.0) for e in expenses))
    top3 = sum(item["total"] for item in category_breakdown(expenses)[:3])
    concentration = min(1.0, top3 / total)
    anomaly_factor = min(1.0, len(anomalies) / max(1.0, len(expenses) * 0.15))
    growth_factor = min(1.0, max(0.0, float(growth.get("monthOverMonthGrowthPct", 0.0))) / 40.0)
    raw = (0.45 * concentration) + (0.30 * anomaly_factor) + (0.25 * growth_factor)
    return int(round(max(0.0, min(100.0, raw * 100.0))))


def savings_suggestions(
    expenses: List[Dict[str, Any]],
    growth: Dict[str, Any],
    risk: int,
) -> List[Dict[str, Any]]:
    breakdown = category_breakdown(expenses)
    suggestions: List[Dict[str, Any]] = []
    top = breakdown[:3]
    for item in top:
        monthly_avg = item["total"] / max(1, len(monthly_trend(expenses, months_back=12)))
        target_cut = 0.15 if risk < 70 else 0.22
        suggestions.append(
            {
                "category": item["category"],
                "currentMonthlyAvg": round(monthly_avg, 2),
                "suggestedMonthlyBudget": round(max(0.0, monthly_avg * (1.0 - target_cut)), 2),
                "recommendedCutPct": round(target_cut * 100.0, 1),
            }
        )

    fast = growth.get("fastestGrowingCategory")
    if fast and isinstance(fast, dict):
        suggestions.append(
            {
                "category": fast["category"],
                "currentMonthlyAvg": round(float(fast.get("currentMonthTotal") or 0.0), 2),
                "suggestedMonthlyBudget": round(float(fast.get("currentMonthTotal") or 0.0) * 0.85, 2),
                "recommendedCutPct": 15.0,
                "reason": "Fastest growing category this month.",
            }
        )
    return suggestions[:5]

