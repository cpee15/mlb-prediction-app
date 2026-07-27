# MyDashboard Report Execution Audit

This document defines the production read contract for every report registered in
`dashboard_report_types.py`.

User-facing report requests may filter, sort, count, and paginate persisted
records. They must not call external APIs, build projections, refresh canonical
players, or hydrate analytical datasets.

| Report type | Durable source | Request adapter | Grain / dedupe contract |
|---|---|---|---|
| `all_active_hitters` | `dashboard_player_current` | `dashboard_player_report_query.query_player_report` | One current row per `mlb_player_id` |
| `all_active_pitchers` | `dashboard_player_current` | `dashboard_player_report_query.query_player_report` | One current row per `mlb_player_id` |
| `hitters_current_matchup` | `dashboard_players` and current matchup data | Current-player report adapter | One player per current matchup context |
| `hitters_arsenal_splits` | `batter_pitch_type_matchups` | `dashboard_related_report_query.query_related_report` | Latest row per date, game, batter, opposing pitcher, and pitch type |
| `competitive_batter_arsenal` | `batter_pitch_type_matchups` joined to latest `pitch_arsenal` | `dashboard_related_report_query.query_related_report` | Latest matchup row plus latest seasonal pitcher-arsenal row |
| `players_lineup_history` | `dashboard_players` | `dashboard_related_report_query.query_related_report` | One row per canonical MLB player |
| `teams_daily_analysis` | `my_dashboard_records` | `my_dashboard_dataset_runtime.run_dataset_query` | One entity key per dataset version, component, and mode |
| `games_totals_analysis` | `my_dashboard_records` | `my_dashboard_dataset_runtime.run_dataset_query` | One entity key per dataset version, component, and mode |
| `overall_players_daily_analysis` | `my_dashboard_records` | `my_dashboard_dataset_runtime.run_dataset_query` | One entity key per dataset version, component, and mode |
| `model_projection_games` | warmed `model_projection_date_artifact` | `dashboard_projection_report_query.query_projection_report` | One game per projection date and `game_pk` |
| `model_projection_players` | warmed `model_projection_date_artifact` | `dashboard_projection_report_query.query_projection_report` | One player per projection date, game, player, and role |
| `model_tracker_snapshots` | `model_tracker_snapshots` | `dashboard_related_report_query.query_related_report` | Unique server-generated `tracker_key` |

## Readiness contract

Every report response exposes one of these states:

- `ready`: a current persisted snapshot was read.
- `stale`: the last successful persisted snapshot was served and scheduled
  refresh is required.
- `not_ready`: no persisted snapshot exists. The response contains no fabricated
  records and does not start a refresh.

`refreshing` is explicit and defaults to `false` until a durable refresh-job
status is attached to the response.

## Refresh ownership

- Canonical players are populated by `dashboard_projection_operator`.
- Model Projection artifacts are populated by
  `model_projection_routes.warm_model_projection_payload`.
- Component datasets are populated by the MyDashboard hydration endpoints and
  scheduled hydration workflow.
- Bet105 markets are normalized by the existing KIBL/Bet105 sportsbook runtime.

Refresh jobs must log target MLB date, source, duration, resulting row count,
duplicate rows removed, and last successful completion time.

## Performance acceptance gate

- No user report request invokes a builder or external HTTP client.
- First-page report p95 target: less than 500 ms.
- Subsequent page p95 target: less than 250 ms.
- A failed refresh never replaces the previous current dataset.
- A missing refresh never appears as a valid ready dataset with zero records.
