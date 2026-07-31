"""Validate hitter speed snapshot acquisition evidence."""

from __future__ import annotations

from typing import Any, Iterable


REQUIRED_CSV_FIELDS = {
    "player_id",
    "competitive_runs",
    "sprint_speed",
}

RECOMMENDED_NEXT_SLICE = (
    "begin_prospective_hitter_speed_snapshot_collection"
)


def evaluate_hitter_speed_snapshot_acquisition_contract(
    observations: Iterable[dict[str, Any]],
    *,
    historical_as_of_query_supported: bool,
) -> dict[str, Any]:
    """Evaluate observed Savant acquisition capabilities."""

    rows = sorted(
        (
            dict(observation)
            for observation in observations
        ),
        key=lambda row: row.get(
            "season_requested",
            -1,
        ),
    )

    blockers = []
    seasons = [
        row.get("season_requested")
        for row in rows
    ]

    if not rows:
        blockers.append(
            "acquisition_observations_missing"
        )

    if any(
        not isinstance(season, int)
        or season < 2015
        for season in seasons
    ):
        blockers.append(
            "invalid_observation_season"
        )

    if len(set(seasons)) != len(seasons):
        blockers.append(
            "duplicate_observation_season"
        )

    response_contract_ready = bool(rows)

    required_field_failures = {}
    invalid_response_seasons = []
    replay_failures = []
    identity_failures = []
    qualification_failures = []
    measurement_failures = []
    embedded_season_fields = []
    embedded_freshness_fields = []

    semantic_hashes = []

    for row in rows:
        season = row.get("season_requested")
        fields = {
            str(field).strip().lower()
            for field in (
                row.get("fieldnames")
                or []
            )
        }
        missing_fields = sorted(
            REQUIRED_CSV_FIELDS - fields
        )

        if missing_fields:
            required_field_failures[
                str(season)
            ] = missing_fields
            response_contract_ready = False

        if (
            row.get("http_status") != 200
            or not str(
                row.get("content_type") or ""
            ).lower().startswith("text/csv")
        ):
            invalid_response_seasons.append(
                season
            )
            response_contract_ready = False

        if not (
            row.get("raw_replay_identical")
            and row.get(
                "semantic_replay_identical"
            )
        ):
            replay_failures.append(season)
            response_contract_ready = False

        if (
            row.get(
                "invalid_player_id_count",
                0,
            )
            != 0
            or row.get(
                "duplicate_player_id_count",
                0,
            )
            != 0
            or row.get(
                "unique_player_count",
                0,
            )
            != row.get("row_count", 0)
        ):
            identity_failures.append(season)
            response_contract_ready = False

        if (
            row.get(
                "underqualified_row_count",
                0,
            )
            != 0
        ):
            qualification_failures.append(
                season
            )
            response_contract_ready = False

        if (
            row.get(
                "invalid_sprint_speed_count",
                0,
            )
            != 0
        ):
            measurement_failures.append(
                season
            )
            response_contract_ready = False

        if row.get("season_field_present"):
            embedded_season_fields.append(
                season
            )

        if row.get(
            "freshness_field_present"
        ):
            embedded_freshness_fields.append(
                season
            )

        semantic_hash = row.get(
            "semantic_sha256"
        )
        if semantic_hash:
            semantic_hashes.append(
                semantic_hash
            )
        else:
            response_contract_ready = False

    cross_season_distinct = (
        len(semantic_hashes) == len(rows)
        and len(set(semantic_hashes))
        == len(semantic_hashes)
    )

    if rows and not cross_season_distinct:
        blockers.append(
            "cross_season_responses_not_distinct"
        )
        response_contract_ready = False

    if required_field_failures:
        blockers.append(
            "required_csv_fields_missing"
        )

    if invalid_response_seasons:
        blockers.append(
            "invalid_csv_response_contract"
        )

    if replay_failures:
        blockers.append(
            "response_replay_not_deterministic"
        )

    if identity_failures:
        blockers.append(
            "player_identity_contract_failed"
        )

    if qualification_failures:
        blockers.append(
            "qualification_contract_failed"
        )

    if measurement_failures:
        blockers.append(
            "speed_measurement_contract_failed"
        )

    season_metadata_external_required = (
        bool(rows)
        and not embedded_season_fields
    )
    freshness_metadata_external_required = (
        bool(rows)
        and not embedded_freshness_fields
    )

    acquisition_supported = (
        response_contract_ready
        and cross_season_distinct
    )
    prospective_collection_allowed = (
        acquisition_supported
        and season_metadata_external_required
        and freshness_metadata_external_required
    )

    retrospective_predictive_evaluation_allowed = (
        acquisition_supported
        and historical_as_of_query_supported
        and not freshness_metadata_external_required
    )

    temporal_blockers = []

    if not historical_as_of_query_supported:
        temporal_blockers.append(
            "historical_as_of_query_unsupported"
        )

    if freshness_metadata_external_required:
        temporal_blockers.append(
            "source_freshness_metadata_not_embedded"
        )

    if not retrospective_predictive_evaluation_allowed:
        temporal_blockers.append(
            "historical_capture_precedes_outcomes_unverified"
        )

    overall_blockers = sorted(
        set(blockers + temporal_blockers)
    )

    return {
        "status": (
            "ready"
            if acquisition_supported
            else "blocked"
        ),
        "acquisition_supported":
            acquisition_supported,
        "response_contract_ready":
            response_contract_ready,
        "csv_download_supported":
            acquisition_supported,
        "season_query_supported":
            acquisition_supported,
        "historical_as_of_query_supported":
            historical_as_of_query_supported,
        "cross_season_responses_distinct":
            cross_season_distinct,
        "observed_seasons": seasons,
        "observation_count": len(rows),
        "required_csv_fields": sorted(
            REQUIRED_CSV_FIELDS
        ),
        "required_field_failures":
            required_field_failures,
        "invalid_response_seasons":
            invalid_response_seasons,
        "replay_failure_seasons":
            replay_failures,
        "identity_failure_seasons":
            identity_failures,
        "qualification_failure_seasons":
            qualification_failures,
        "measurement_failure_seasons":
            measurement_failures,
        "season_metadata_external_required":
            season_metadata_external_required,
        "freshness_metadata_external_required":
            freshness_metadata_external_required,
        "prospective_collection_allowed":
            prospective_collection_allowed,
        "retrospective_predictive_evaluation_allowed":
            retrospective_predictive_evaluation_allowed,
        "retrospective_temporal_blockers":
            sorted(set(temporal_blockers)),
        "blockers": overall_blockers,
        "recommended_next_slice": (
            RECOMMENDED_NEXT_SLICE
            if prospective_collection_allowed
            else (
                "repair_hitter_speed_acquisition_contract"
            )
        ),
        "decision": {
            "source_endpoint_confirmed":
                acquisition_supported,
            "begin_prospective_collection":
                prospective_collection_allowed,
            "run_retrospective_speed_audit":
                retrospective_predictive_evaluation_allowed,
            "proxy_substitution_allowed": False,
            "parameter_selection_allowed": False,
        },
        "production_impact": {
            "external_fetch_performed": False,
            "database_schema_changed": False,
            "database_writes_performed": False,
            "production_model_modified": False,
            "simulation_authority_changed": False,
            "production_authority_changed": False,
        },
        "parameter_selected": False,
        "production_authority_changed": False,
        "shadow_only": True,
    }
