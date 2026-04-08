from typing import Any, Dict, List

from flask import Blueprint, current_app, jsonify, request

from services.anomaly_service import detect_anomalies
from services.insights_service import (
    category_breakdown,
    growth_trends,
    monthly_trend,
    risk_score,
    savings_suggestions,
    spending_behavior,
)
from services.prediction_service import forecast_next_month

chatbot_bp = Blueprint("chatbot", __name__)


SYSTEM_PROMPT = (
    "You are a personal finance advisor.\n"
    "Respond short (3–4 lines), conversational, and personalized.\n"
    "Include ₹ amounts + % shares, and give 1 actionable tip.\n"
    "No generic fluff."
)

def _compute_insights(expenses: List[Dict[str, Any]]) -> Dict[str, Any]:
    contamination = float(current_app.config.get("ANOMALY_CONTAMINATION", 0.08))
    categories = category_breakdown(expenses)
    trend = monthly_trend(expenses, months_back=12)
    anomalies = detect_anomalies(expenses, contamination=contamination)
    growth = growth_trends(expenses)
    risk = risk_score(expenses, anomalies, growth)
    suggestions = savings_suggestions(expenses, growth, risk)
    prediction = forecast_next_month(expenses)
    total = round(sum(float(e.get("amount") or 0.0) for e in expenses), 2)
    behavior = spending_behavior(expenses)
    return {
        "totalExpenses": total,
        "categoryTotals": categories,
        "overspendingCategories": categories[:3],
        "monthlyTrend": trend,
        "anomalies": anomalies,
        "growthTrends": growth,
        "riskScore": risk,
        "savingsSuggestions": suggestions,
        "prediction": prediction,
        "behavior": behavior,
    }


def _format_fallback_advice(message: str, insights: Dict[str, Any]) -> str:
    overspending = insights.get("overspendingCategories") or []
    growth = insights.get("growthTrends") or {}
    risk = int(insights.get("riskScore") or 0)
    anomalies = insights.get("anomalies") or []
    suggestions = insights.get("savingsSuggestions") or []
    prediction = insights.get("prediction")
    user_msg = (message or "").lower()

    lines: List[str] = []
    lines.append("Overspending categories:")
    if overspending:
        for item in overspending:
            lines.append(f"- {item['category']}: {item['total']}")
    else:
        lines.append("- Not enough category data yet.")

    lines.append("")
    lines.append("Practical saving tips:")
    for s in suggestions[:3]:
        lines.append(
            f"- Reduce `{s['category']}` by about {s['recommendedCutPct']}% (target: {s['suggestedMonthlyBudget']} / month)."
        )
    if not suggestions:
        lines.append("- Use category caps and review weekly transactions.")
    if anomalies:
        worst = anomalies[0]
        lines.append(
            f"- Investigate unusual spend: {worst['vendor']} on {worst['date']} ({worst['amount']})."
        )

    lines.append("")
    lines.append("Budget suggestions:")
    lines.append(f"- Current overspending risk score: {risk}/100.")
    if growth.get("spendingSpikeDetected"):
        lines.append(
            f"- Spending spike detected ({growth.get('monthOverMonthGrowthPct', 0)}% month-over-month)."
        )
    fast = growth.get("fastestGrowingCategory")
    if isinstance(fast, dict) and fast.get("category"):
        lines.append(
            f"- Fastest growing category: {fast['category']} ({fast.get('growthPct', 0)}% increase)."
        )
    if prediction:
        ci = prediction.get("confidenceInterval") or {}
        lines.append(
            f"- Next month forecast: {prediction.get('predictedTotal')} (range {ci.get('lower')} - {ci.get('upper')})."
        )

    if "waste" in user_msg or "wasting" in user_msg:
        lines.append("")
        lines.append("Direct answer:")
        if overspending:
            lines.append(f"- You are likely overspending most in `{overspending[0]['category']}`.")
    if "reduce" in user_msg or "save" in user_msg:
        lines.append("- Start with the top 1-2 categories and cut discretionary purchases first.")

    return "\n".join(lines).strip()


@chatbot_bp.route("/chat", methods=["POST"])
def chat():
    repo = current_app.extensions["repo"]
    expenses = repo.list_expenses(limit=5000)
    payload = request.get_json(silent=True) or {}

    message = (payload.get("message") or "").strip()
    if not message:
        return jsonify({"error": "Missing `message` in request body."}), 400

    msg_low = message.lower().strip()
    if msg_low in {"hi", "hello", "hey", "hii", "hola"}:
        return jsonify(
            {
                "advice": "Hi! I’m your finance advisor.\nAsk: “Where am I overspending?”\nOr: “How can I save ₹1000/month?”"
            }
        )

    if not expenses:
        advice = "No expenses found yet. Upload a CSV/PDF or add a manual entry to get personalized advice."
        return jsonify({"advice": advice})

    insights = _compute_insights(expenses)

    # Attempt configured LLM service (Gemini) first; fallback to heuristics on failure.
    try:
        openai_service = current_app.extensions.get("openai_service")
        if openai_service:
            overspending_str = "\n".join(
                [f"- {i['category']} (total: {i['total']})" for i in (insights.get("overspendingCategories") or [])]
            )
            monthly_str = "\n".join(
                [f"- {m['month']}: {m['total']}" for m in (insights.get("monthlyTrend") or [])][-12:]
            )
            growth = insights.get("growthTrends") or {}
            risk = insights.get("riskScore")
            anomalies = insights.get("anomalies") or []
            suggestions = insights.get("savingsSuggestions") or []
            prediction = insights.get("prediction") or {}
            prediction_ci = prediction.get("confidenceInterval") or {}
            behavior = insights.get("behavior") or {}

            user_prompt = (
                f"User question: {message}\n\n"
                f"Use the following structured financial insights and respond like a real financial advisor.\n"
                f"- Total expenses: {sum(float(e.get('amount') or 0.0) for e in expenses):.2f}\n"
                f"- Overspending categories:\n{overspending_str or '- None'}\n"
                f"- Monthly totals:\n{monthly_str or '- N/A'}\n"
                f"- Growth trends: {growth}\n"
                f"- Risk score: {risk}/100\n"
                f"- Budget health: {behavior.get('budgetHealth')}\n"
                f"- Weekend vs weekday spend: weekend={behavior.get('weekendSpendPct')}%, weekday={behavior.get('weekdaySpendPct')}%\n"
                f"- Impulsive spending score (0-100): {behavior.get('impulsiveSpendingScore')}\n"
                f"- Lifestyle signals: {behavior.get('lifestyleSignals')}\n"
                f"- Subscription candidates: {behavior.get('subscriptionCandidates')}\n"
                f"- Anomaly count: {len(anomalies)}\n"
                f"- Savings suggestions: {suggestions}\n"
                f"- Prediction: month={prediction.get('nextMonth')}, total={prediction.get('predictedTotal')}, "
                f"interval=({prediction_ci.get('lower')} to {prediction_ci.get('upper')})\n\n"
                f"Output:\n"
                f"- Start with a 1-2 sentence direct answer to the user’s question.\n"
                f"- Then give 3-6 bullet points with quantified insights and actionable recommendations.\n"
                f"- Where possible, include one concrete saving estimate (₹/$ per month) based on the data.\n"
                f"- End with one short follow-up question to personalize the plan.\n"
                f"Style: human, confident, practical. No generic fluff."
            )

            advice = openai_service.generate_advice(SYSTEM_PROMPT, user_prompt)
            if advice and str(advice).strip():
                return jsonify({"advice": str(advice).strip()})
    except Exception:
        # Keep fallback for reliability.
        pass

    advice = _format_fallback_advice(message, insights).strip()
    if not advice:
        advice = "I’m having trouble analyzing right now—try again in a moment."
    return jsonify({"advice": advice})

