from __future__ import annotations

from typing import Any, Dict, List, Tuple

from sqlalchemy import Float, case, cast, func

from .my_dashboard_dataset import MyDashboardRecord
from .my_dashboard_report_query import METRIC_FIELDS


def normalize_weights(component: str, weights: Any) -> Tuple[Dict[str, float], List[str]]:
    if not isinstance(weights, dict):
        return {}, []
    registered = set(METRIC_FIELDS.get(component, []))
    normalized: Dict[str, float] = {}
    warnings: List[str] = []
    for metric, raw_value in weights.items():
        try:
            weight = max(0.0, min(2.0, float(raw_value)))
        except (TypeError, ValueError):
            warnings.append(f"Invalid weight for {metric}")
            continue
        if metric not in registered:
            warnings.append(f"Unsupported weight metric: {metric}")
            continue
        if abs(weight - 1.0) >= 0.000001:
            normalized[str(metric)] = weight
    return normalized, warnings


def metric_expression(metric: str):
    return cast(MyDashboardRecord.metrics_json[metric].as_string(), Float)


def _clamp(expression, lower: float = -1.0, upper: float = 1.0):
    return case(
        (expression < lower, lower),
        (expression > upper, upper),
        else_=expression,
    )


def normalized_metric_expression(metric_name: str):
    value = metric_expression(metric_name)
    name = metric_name.lower()
    if "ev" in name or "velocity" in name:
        normalized = (value - 88.0) / 12.0
    elif "la" in name or "launch angle" in name:
        normalized = 1.0 - func.abs(value - 16.0) / 25.0
    elif "pitches seen" in name or name == "pa":
        normalized = value / 60.0
    elif "total" in name:
        normalized = (value - 8.5) / 4.0
    elif "score" in name or "edge" in name or "diff" in name:
        normalized = value
    elif "bb" in name:
        normalized = (0.085 - value) * 8.0
    elif "allowed" in name and ("xwoba" in name or "hardhit" in name):
        normalized = (0.34 - value) * 5.0
    else:
        normalized = value
    return case((value.is_(None), 0.0), else_=_clamp(normalized))


def weighted_score_expression(weights: Dict[str, float]):
    base = func.coalesce(MyDashboardRecord.base_score, MyDashboardRecord.score, 0.0)
    adjusted = base
    for metric_name, weight in weights.items():
        adjusted = adjusted + normalized_metric_expression(metric_name) * (weight - 1.0) * 0.25
    return adjusted


def weight_explanations(weights: Dict[str, float]) -> List[str]:
    explanations: List[str] = []
    for metric_name, weight in weights.items():
        verb = "emphasized" if weight > 1.0 else "deemphasized"
        explanations.append(f"{metric_name} {verb} at {round(weight, 2)}")
    return explanations
