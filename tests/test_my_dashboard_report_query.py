from __future__ import annotations

from types import SimpleNamespace

from mlb_app.my_dashboard_report_query import (
    MAX_PAGE_SIZE,
    apply_report_query,
    field_metadata,
    install_full_result_finalizer,
    normalize_query,
)


def _items(count: int):
    return [
        {
            "entity_id": str(index),
            "entity_name": f"Player {index}",
            "score": float(index),
            "metrics": {"xwOBA": round(index / 1000, 3)},
        }
        for index in range(1, count + 1)
    ]


def test_normalize_query_clamps_page_size_and_direction():
    query = normalize_query(page_size=9999, page_number=-4, sort_by="entity_name", sort_direction="sideways")
    assert query == {
        "page_size": MAX_PAGE_SIZE,
        "page_number": 1,
        "offset": 0,
        "sort_by": "entity_name",
        "sort_direction": "desc",
    }


def test_query_envelope_pages_full_universe_without_top_ten_cap():
    payload = {"items": _items(27), "date": "2026-07-13", "component": "hitters"}
    page = apply_report_query(payload, "hitters", page_size=10, page_number=2, sort_by="score", sort_direction="desc")

    assert page["totalSize"] == 27
    assert page["total_count"] == 27
    assert len(page["records"]) == 10
    assert page["records"][0]["entity_name"] == "Player 17"
    assert page["records"][-1]["entity_name"] == "Player 8"
    assert page["done"] is False
    assert page["page_info"]["next_page"] == 3
    assert page["page_info"]["previous_page"] == 1


def test_query_can_sort_by_nested_metric():
    payload = {"items": _items(4)}
    page = apply_report_query(payload, "hitters", page_size=50, sort_by="metrics.xwOBA", sort_direction="asc")
    assert [record["entity_id"] for record in page["records"]] == ["1", "2", "3", "4"]


def test_field_metadata_is_server_owned_and_describe_like():
    fields = field_metadata("hitters", _items(2))
    by_name = {field["name"]: field for field in fields}
    assert by_name["entity_name"]["type"] == "string"
    assert by_name["entity_name"]["sortable"] is True
    assert by_name["metrics.xwOBA"]["filterable"] is True
    assert by_name["metrics.xwOBA"]["source"] == "server_metric_registry"


def test_installed_finalizer_preserves_every_deduped_filtered_record():
    def dedupe(items, key_fn, limit=10):
        unique = {}
        for item in items:
            unique[key_fn(item)] = dict(item)
        ranked = sorted(unique.values(), key=lambda item: item["score"], reverse=True)[:limit]
        for rank, item in enumerate(ranked, start=1):
            item["rank"] = rank
        return ranked

    def apply_filters(items, filters):
        return list(items), filters or {}, [], len(items), len(items)

    def build_response(date, component, items, data_quality, missing_data):
        # Match the shared legacy card helper: it caps presentation rows even
        # when the report adapter supplies a complete population.
        return {"date": date, "component": component, "items": items[:10]}

    fake_solver = SimpleNamespace(
        dedupe_ranked_items=dedupe,
        apply_dashboard_filters=apply_filters,
        build_response=build_response,
        available_filters_for_component=lambda component, items: {"metrics": []},
        finalize_component_response=None,
    )
    install_full_result_finalizer(fake_solver)
    candidates = _items(31)
    result = fake_solver.finalize_component_response(
        "2026-07-13",
        "hitters",
        candidates,
        lambda item: item["entity_id"],
    )

    assert len(result["items"]) == 31
    assert result["candidate_universe_count"] == 31
    assert result["deduped_universe_count"] == 31
    assert result["result_cap_applied"] is False
