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

