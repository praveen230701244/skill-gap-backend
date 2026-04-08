from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

try:
    from dateutil.parser import parse as date_parse  # type: ignore
except Exception:  # pragma: no cover
    date_parse = None


def category_breakdown(expenses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_cat: Dict[str, float] = defaultdict(float)
    for e in expenses:
        cat = str(e.get("category") or "Others").strip()
        if not cat or cat.lower() == "uncategorized":
            cat = "Others"
        by_cat[cat] += float(e.get("amount") or 0.0)
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


def _parse_iso_date(s: Any) -> Optional[datetime]:
    if not s:
        return None
    if isinstance(s, datetime):
        return s
    ss = str(s).strip()
    if not ss:
        return None
    try:
        return datetime.fromisoformat(ss[:10])
    except Exception:
        if date_parse:
            try:
                return date_parse(ss, dayfirst=True, fuzzy=True)
            except Exception:
                return None
    return None


def spending_behavior(expenses: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Adds behavioral signals without changing existing API callers.
    Returns a compact dict suitable for chatbot context.
    """
    if not expenses:
        return {
            "weekendSpendPct": 0.0,
            "weekdaySpendPct": 0.0,
            "impulsiveSpendingScore": 0,
            "subscriptionCandidates": [],
            "lifestyleSignals": [],
            "budgetHealth": "Unknown",
        }

    # Weekend vs weekday
    weekend_total = 0.0
    weekday_total = 0.0
    total = 0.0

    # Impulsive proxy: high-frequency small discretionary spends (top quartile frequency vendors under a cap)
    small_txn_cap = 500.0
    small_count = 0

    # Subscription detection: same vendor recurring monthly-ish with low variance
    by_vendor_dates: Dict[str, List[datetime]] = defaultdict(list)
    by_vendor_amounts: Dict[str, List[float]] = defaultdict(list)

    # Lifestyle signals by simple vendor/category keywords
    food_delivery = 0.0
    transport = 0.0

    for e in expenses:
        amt = float(e.get("amount") or 0.0)
        if amt <= 0:
            continue
        total += amt

        dt = _parse_iso_date(e.get("date"))
        if dt:
            if dt.weekday() >= 5:
                weekend_total += amt
            else:
                weekday_total += amt

        vendor = str(e.get("vendor") or "").strip() or "Unknown"
        vlow = vendor.lower()
        by_vendor_amounts[vendor].append(amt)
        if dt:
            by_vendor_dates[vendor].append(dt)

        if amt <= small_txn_cap:
            # exclude utilities-like obvious non-impulsive
            if not any(k in vlow for k in ["electric", "water", "gas", "broadband", "internet", "recharge", "insurance"]):
                small_count += 1

        if any(k in vlow for k in ["swiggy", "zomato", "food", "eat", "domino", "pizza", "burger", "ubereats"]):
            food_delivery += amt
        if any(k in vlow for k in ["uber", "ola", "metro", "rapido", "fuel", "petrol", "diesel", "transport"]):
            transport += amt

    weekend_pct = round((weekend_total / total) * 100.0, 2) if total > 0 else 0.0
    weekday_pct = round((weekday_total / total) * 100.0, 2) if total > 0 else 0.0

    # Subscription candidates
    subscription_candidates: List[Dict[str, Any]] = []
    for vendor, dates in by_vendor_dates.items():
        if len(dates) < 3:
            continue
        dates_sorted = sorted(dates)
        # compute day gaps
        gaps = [(dates_sorted[i] - dates_sorted[i - 1]).days for i in range(1, len(dates_sorted))]
        if not gaps:
            continue
        avg_gap = sum(gaps) / len(gaps)
        # "monthly-ish": around 28-35 days on average
        if 26 <= avg_gap <= 36:
            amts = by_vendor_amounts.get(vendor, [])
            if not amts:
                continue
            avg_amt = sum(amts) / len(amts)
            # low variance
            max_dev = max(abs(a - avg_amt) for a in amts)
            if avg_amt > 0 and (max_dev / avg_amt) <= 0.25:
                subscription_candidates.append(
                    {
                        "vendor": vendor,
                        "avgAmount": round(avg_amt, 2),
                        "occurrences": len(dates),
                        "avgGapDays": round(avg_gap, 1),
                    }
                )
    subscription_candidates.sort(key=lambda x: (x["avgAmount"], x["occurrences"]), reverse=True)
    subscription_candidates = subscription_candidates[:8]

    # Impulsive spending score (0-100): mostly driven by share of small discretionary txns.
    total_txns = max(1, len([e for e in expenses if float(e.get("amount") or 0.0) > 0]))
    small_share = small_count / total_txns
    impulsive_score = int(round(max(0.0, min(1.0, (small_share - 0.25) / 0.5)) * 100.0))

    lifestyle: List[str] = []
    if total > 0:
        if (food_delivery / total) >= 0.18:
            lifestyle.append("High food delivery usage")
        if (transport / total) >= 0.12:
            lifestyle.append("Frequent transport spending")

    # Budget health classification using risk score proxy if available later; fallback to spend concentration
    # Here we classify on concentration of top categories to keep it deterministic.
    top3 = sum(item["total"] for item in category_breakdown(expenses)[:3])
    concentration = (top3 / total) if total > 0 else 1.0
    if concentration <= 0.65:
        budget_health = "Good"
    elif concentration <= 0.78:
        budget_health = "Moderate"
    else:
        budget_health = "Risky"

    return {
        "weekendSpendPct": weekend_pct,
        "weekdaySpendPct": weekday_pct,
        "impulsiveSpendingScore": impulsive_score,
        "subscriptionCandidates": subscription_candidates,
        "lifestyleSignals": lifestyle,
        "budgetHealth": budget_health,
    }

