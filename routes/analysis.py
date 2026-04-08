from flask import Blueprint, current_app, jsonify

from services.anomaly_service import detect_anomalies
from services.insights_service import (
    category_breakdown,
    growth_trends,
    monthly_trend,
    risk_score,
    savings_suggestions,
)
from services.prediction_service import forecast_next_month


analysis_bp = Blueprint("analysis", __name__)

def _build_top_insights(summary: dict, expenses: list) -> list[str]:
    total = float(summary.get("totalExpenses") or 0.0)
    top_insights: list[str] = []

    # 1) Category share insight
    breakdown = summary.get("categoryBreakdown") or []
    if breakdown and total > 0:
        top = breakdown[0]
        pct = round((float(top.get("total") or 0.0) / total) * 100.0, 0)
        top_insights.append(f"You spent {int(pct)}% on {top.get('category')}")

    # 2) Weekly-ish food rising (simple: compare last 7 vs prior 7 days when dates exist)
    # Keep it lightweight and safe; if date coverage is poor, skip.
    try:
        from datetime import datetime, timedelta

        def _dt(s):
            return datetime.fromisoformat(str(s)[:10])

        dated = [(e, _dt(e.get("date"))) for e in expenses if e.get("date")]
        dated = [(e, d) for (e, d) in dated if isinstance(d, datetime)]
        if dated:
            dated.sort(key=lambda x: x[1])
            end = dated[-1][1]
            w1_start = end - timedelta(days=6)
            w0_start = end - timedelta(days=13)
            w0_end = end - timedelta(days=7)

            def _sum_between(a, b, pred):
                s = 0.0
                for e, d in dated:
                    if a <= d <= b and pred(e):
                        s += float(e.get("amount") or 0.0)
                return s

            def _is_food(e):
                v = str(e.get("vendor") or "").lower()
                c = str(e.get("category") or "").lower()
                return any(k in v for k in ["swiggy", "zomato"]) or "food" in c

            w1 = _sum_between(w1_start, end, _is_food)
            w0 = _sum_between(w0_start, w0_end, _is_food)
            if w0 > 0 and w1 >= w0 * 1.15:
                top_insights.append("Food spending increased this week")
    except Exception:
        pass

    # 3) Anomaly callout (highest amount)
    anomalies = summary.get("anomalies") or []
    if anomalies:
        a = anomalies[0]
        amt = a.get("amount")
        vendor = a.get("vendor") or "Unknown"
        top_insights.append(f"High anomaly detected: ₹{amt} {vendor}")

    # 4) Risk score
    risk = summary.get("riskScore")
    if isinstance(risk, (int, float)):
        top_insights.append(f"Risk score is {int(risk)}/100")

    # Keep 3–5 insights only
    return top_insights[:5]


@analysis_bp.route("/expenses", methods=["GET"])
def get_expenses():
    repo = current_app.extensions["repo"]
    expenses = repo.list_expenses()
    contamination = float(current_app.config.get("ANOMALY_CONTAMINATION", 0.08))

    total = round(sum(float(e.get("amount") or 0.0) for e in expenses), 2)
    category_rows = category_breakdown(expenses)
    top_category = category_rows[0] if category_rows else {"category": "N/A", "total": 0.0}
    monthly_rows = monthly_trend(expenses)
    prediction = forecast_next_month(expenses)
    anomalies = detect_anomalies(expenses, contamination=contamination)
    growth = growth_trends(expenses)
    risk = risk_score(expenses, anomalies, growth)
    suggestions = savings_suggestions(expenses, growth, risk)

    summary = {
        "totalExpenses": total,
        "topCategory": top_category,
        "categoryBreakdown": category_rows,
        "monthlyTrend": monthly_rows,
        "prediction": prediction,
        "anomalies": anomalies,
        "growthTrends": growth,
        "savingsSuggestions": suggestions,
        "riskScore": risk,
    }

    return jsonify({"status": "ok", "expenses": expenses, "summary": summary})


@analysis_bp.route("/insights", methods=["GET"])
def get_insights():
    """
    Minimal insights endpoint for a clean, dashboard-first UI.
    Does NOT replace /expenses; it complements it.
    """
    repo = current_app.extensions["repo"]
    expenses = repo.list_expenses()
    contamination = float(current_app.config.get("ANOMALY_CONTAMINATION", 0.08))

    total = round(sum(float(e.get("amount") or 0.0) for e in expenses), 2)
    category_rows = category_breakdown(expenses)
    anomalies = detect_anomalies(expenses, contamination=contamination)
    growth = growth_trends(expenses)
    risk = risk_score(expenses, anomalies, growth)

    summary = {
        "totalExpenses": total,
        "categoryBreakdown": category_rows,
        "anomalies": anomalies,
        "riskScore": risk,
    }

    return jsonify({"topInsights": _build_top_insights(summary, expenses)})

