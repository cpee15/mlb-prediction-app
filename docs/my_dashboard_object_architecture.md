# My Dashboard canonical object architecture

## Current execution path

`MyDashboardReportBuilderPage.jsx` sends report state to `/my-dashboard/solver` or `/my-dashboard/solver/active-lineups`. Current-date substantive requests use `my_dashboard_dataset_runtime`, which hydrates unfiltered solver output into `my_dashboard_records` and delegates filtering, request-scoped weighting, sorting, counting, and pagination to `my_dashboard_sql_query`. Unfiltered and historical requests retain the legacy solver compatibility path.

`my_dashboard_records` is intentionally keyed by date, component, mode, version, and entity. It is a safe analytical snapshot/compatibility layer, but cannot represent a durable cross-date player population. A missing current-date solver snapshot can therefore yield an empty default object report even when tracked players exist on other dates.

## Verified reusable sources

- Canonical MLB IDs: `StatcastEvent.batter_id` and `pitcher_id`, MLB Stats people responses, active team rosters, probable pitchers, and boxscore lineups.
- Confirmed lineups: `active_lineup_solver` calls `generate_matchups_for_date` and `lineup_profile.fetch_boxscore_lineup`; ID is preferred and the existing name/team fallback is not accepted as canonical identity for the new object store.
- Tracked games: `statcast_events` provides `game_date`, `game_pk`, batter ID, and pitcher ID.
- Current analytics: batter/pitcher aggregates, player splits, pitch arsenal, and batter/pitch-type matchup tables.
- Active roster source: the verified MLB Stats `/teams/{team_id}/roster?rosterType=active` path already powers the public roster endpoint and matchup fallback. Population ingestion is deferred to PR 2.

## Object boundaries

- `dashboard_players`: one durable identity row per MLB player ID plus deterministic activity facts and resolution state.
- `dashboard_player_snapshots`: versioned historical analytical facts. A uniqueness constraint prevents duplicate player/date/context/version rows.
- `dashboard_player_current`: one query-optimized approved projection per player. This becomes the source for default active-player reports after hydration and query migration.
- Existing one-to-many tables remain related report sources; they are not flattened into the current player row.
- `my_dashboard_records` remains untouched as the current compatibility and date-specific snapshot layer.

## Active-player contract for PR 2

Default window: 30 days, configured by `DASHBOARD_ACTIVE_PLAYER_WINDOW_DAYS`. A resolved MLB player is eligible when at least one verified fact is true: recent confirmed lineup, recent tracked game, active roster plus usable analytics, or today's confirmed/projected lineup. Rows lacking an MLB ID remain explicitly unresolved and are excluded from canonical/current promotion. Names alone never establish canonical identity.

## Migration and failure safety

PR 1 only registers additive tables and contracts. Later hydration must build candidate rows, validate counts/identity coverage, write a new snapshot version, and promote current rows in one transaction. Empty or failed refreshes must preserve the prior projection. PostgreSQL production uses the same SQLAlchemy indexes defined and tested under SQLite; deployment must measure lock/build time before any larger backfill.

## Report types

The initial registry declares All Active Hitters, All Active Pitchers, Hitters with Current Matchup Metrics, Hitters with Arsenal Splits, Players with Lineup History, Teams with Daily Analysis, and Games with Totals Analysis. Registry presence is not a claim that a related report is queryable yet; query wiring is delivered in later PRs.

## PR sequence

1. Add this contract, canonical/snapshot/current schemas, registry, indexes, and SQLite tests.
2. Populate identities and deterministic active status from verified sources.
3. Hydrate snapshots and atomically promote the current projection.
4. Query default and filtered reports from the current projection.
5. Migrate the existing Report Builder to report types and server field metadata.
6. Add the safest related report types, observability, backfill runbook, and production evidence.
