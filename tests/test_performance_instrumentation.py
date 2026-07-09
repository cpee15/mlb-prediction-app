from __future__ import annotations

from mlb_app.performance import (
    estimate_payload_bytes,
    performance_snapshot,
    record_cache_status,
    record_probability_source,
    record_request_sample,
    record_span,
    timing_span,
)
from mlb_app.shared_payload_cache import clear_shared_payload_cache, get_cache, get_or_set, set_cache


def test_estimate_payload_bytes_for_json_payload() -> None:
    payload = {"route": "/matchups", "games": [{"game_pk": 1, "home_win_prob": 0.55}]}

    measured = estimate_payload_bytes(payload)

    assert isinstance(measured, int)
    assert measured > 0


def test_timing_span_records_formula_span() -> None:
    with timing_span(
        "unit.formula_span",
        category="formula",
        route="/unit-test",
        game_pk=123,
        date="2026-07-09",
        probability_source="model_projections",
    ):
        sum(range(5))

    snapshot = performance_snapshot()
    slowest = snapshot["spans"]["slowest"]

    assert any(span["name"] == "unit.formula_span" for span in slowest)
    assert any(span.get("category") == "formula" for span in slowest)
    assert any(span.get("probability_source") == "model_projections" for span in slowest)


def test_performance_snapshot_includes_route_cache_payload_and_probability_source() -> None:
    record_cache_status("HIT")
    record_probability_source("model_projections")
    record_request_sample(
        {
            "method": "GET",
            "path": "/models/projections",
            "route": "/models/projections",
            "status_code": 200,
            "duration_ms": 12.5,
            "response_size_bytes": 2048,
            "cache_status": "HIT",
            "probability_source": "model_projections",
        }
    )

    snapshot = performance_snapshot()
    route = snapshot["routes"]["/models/projections"]

    assert route["cache"]["HIT"] >= 1
    assert route["payload_bytes_p95"] >= 2048
    assert route["probability_sources"]["model_projections"] >= 1


def test_shared_payload_cache_records_cache_and_deepcopy_spans() -> None:
    clear_shared_payload_cache("unit:cache")
    set_cache("unit:cache:key", {"value": [1, 2, 3]})

    cached = get_cache("unit:cache:key", 300)

    assert cached == {"value": [1, 2, 3]}
    snapshot = performance_snapshot()
    span_names = {span["name"] for span in snapshot["spans"]["slowest"]}
    assert "shared_payload_cache.lookup" in span_names
    assert "shared_payload_cache.deepcopy.get" in span_names


def test_get_or_set_builder_records_builder_span_on_miss() -> None:
    clear_shared_payload_cache("unit:builder")

    payload = get_or_set("unit:builder:key", 300, lambda: {"built": True})

    assert payload["built"] is True
    assert payload["cache_hit"] is False
    snapshot = performance_snapshot()
    span_names = {span["name"] for span in snapshot["spans"]["slowest"]}
    assert "shared_payload_cache.builder" in span_names
