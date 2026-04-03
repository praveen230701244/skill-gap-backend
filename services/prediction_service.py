from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
from statsmodels.tsa.arima.model import ARIMA


def monthly_totals(expenses: List[Dict[str, Any]]) -> List[Tuple[str, float]]:
    """
    Returns [(YYYY-MM, totalAmount)] sorted ascending by month.
    """
    buckets: Dict[str, float] = {}
    for e in expenses:
        d = e.get("date")
        if not d or not isinstance(d, str) or len(d) < 7:
            continue
        month = d[:7]
        buckets[month] = buckets.get(month, 0.0) + float(e.get("amount", 0.0))
    months = sorted(buckets.keys())
    return [(m, float(buckets[m])) for m in months]


def _next_month(yyyy_mm: str, months_ahead: int) -> str:
    year = int(yyyy_mm[:4])
    month = int(yyyy_mm[5:7])
    for _ in range(months_ahead):
        month += 1
        if month > 12:
            month = 1
            year += 1
    return f"{year:04d}-{month:02d}"


def forecast_next_month(expenses: List[Dict[str, Any]], months_ahead: int = 1) -> Optional[Dict[str, Any]]:
    """
    ARIMA-based forecast with confidence interval.
    Falls back to trend regression if ARIMA cannot fit.
    """
    mt = monthly_totals(expenses)
    if len(mt) < 3:
        return None

    y = np.array([t for _, t in mt], dtype=float)
    next_month = _next_month(mt[-1][0], months_ahead=months_ahead)

    # ARIMA is a practical default for short series.
    try:
        model = ARIMA(y, order=(1, 1, 1))
        fitted = model.fit()
        forecast_res = fitted.get_forecast(steps=months_ahead)
        predicted = float(forecast_res.predicted_mean[-1])
        conf = forecast_res.conf_int(alpha=0.2)  # 80% interval for actionable planning
        low = float(conf[-1][0])
        high = float(conf[-1][1])
        return {
            "nextMonth": next_month,
            "predictedTotal": round(max(0.0, predicted), 2),
            "confidenceInterval": {
                "lower": round(max(0.0, low), 2),
                "upper": round(max(0.0, high), 2),
            },
            "model": "ARIMA(1,1,1)",
        }
    except Exception:
        pass

    # Fallback: linear trend with standard error based interval.
    x = np.arange(len(y), dtype=float)
    a, b = np.polyfit(x, y, 1)
    next_idx = len(y) - 1 + months_ahead
    pred = float(a * next_idx + b)
    residuals = y - (a * x + b)
    std = float(np.std(residuals)) if len(residuals) > 1 else 0.0
    return {
        "nextMonth": next_month,
        "predictedTotal": round(max(0.0, pred), 2),
        "confidenceInterval": {
            "lower": round(max(0.0, pred - 1.28 * std), 2),  # approx 80%
            "upper": round(max(0.0, pred + 1.28 * std), 2),
        },
        "model": "linear-fallback",
    }

