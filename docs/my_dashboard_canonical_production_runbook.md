# My Dashboard canonical projection production runbook

## Purpose

The default Hitters and Pitchers reports read `dashboard_player_current`. They do not build a slate population at request time. This runbook is the production procedure for populating, inspecting, refreshing, and verifying that canonical projection.

Do not claim My Dashboard is populated from a successful deploy alone. A successful production refresh and plausible status counts are separate requirements.

## Read-only inspection

From a Railway shell or equivalent environment with the production `DATABASE_URL`:

```bash
python scripts/refresh_dashboard_player_projection.py
```

The command is status-only unless `--refresh` or `--backfill-days` is explicitly supplied.

The deployed API also exposes:

```text
GET /my-dashboard/canonical/status
GET /my-dashboard/report-types
```

The status response contains no player names, credentials, connection strings, or source payloads.

## Initial production refresh

Run after the schema-containing deployment is healthy:

```bash
python scripts/refresh_dashboard_player_projection.py --date YYYY-MM-DD --refresh
```

The operator command:

1. reads the verified MLB active-team endpoint and refuses fewer than 30 teams;
2. reads every verified MLB active roster;
3. reads confirmed boxscore lineups available for the target date;
4. combines those sources with tracked Statcast game activity and existing aggregate coverage;
5. populates canonical identities using MLBAM IDs only;
6. builds one snapshot row for every resolved active canonical player;
7. atomically promotes the full current projection;
8. records a durable success or failure row in `dashboard_projection_runs`;
9. emits before/after status and row-count evidence.

Missing-player deactivation is off by default. Use `--transition-missing-players` only after confirming the team and roster source set is complete. A source or projection failure preserves the previous `dashboard_player_current` projection.

## Optional historical snapshot backfill

After a successful current refresh:

```bash
python scripts/refresh_dashboard_player_projection.py --date YYYY-MM-DD --backfill-days 30
```

To perform both operations in one invocation:

```bash
python scripts/refresh_dashboard_player_projection.py --date YYYY-MM-DD --refresh --backfill-days 30
```

Backfill retains immutable snapshots. Only the final successful date is promoted as current.

## Plausibility gates

Treat the projection as production-ready only when all of the following are true:

- `status = ready`;
- `population.canonical_count > 0`;
- `population.active_hitter_count > 0`;
- `population.active_pitcher_count > 0`;
- `current_projection.row_count = population.active_count`;
- hitter and pitcher current counts match their active canonical counts;
- `current_projection.stale = false`;
- a durable `refresh_runs.latest_success` exists;
- snapshot count is non-zero;
- field coverage is inspected rather than assumed;
- default hitter/pitcher reports return the same full-population counts across pagination;
- changing weights does not change `totalSize`.

The service intentionally does not hard-code a claim such as “200+ hitters” because roster availability, season timing, aggregate coverage, and the active-window policy affect the actual population. Record the observed counts in issue #1055 after the production run.

## Field coverage

The status endpoint reports non-null counts and ratios separately for hitters and pitchers for:

- model score and confidence;
- xwOBA and xBA;
- exit velocity and launch angle;
- hard-hit and barrel rates;
- strikeout and walk rates;
- ISO, OBP, and SLG;
- plate appearances.

A low-coverage field remains visible and nillable. It must not silently remove players from an unfiltered report.

## Related reports

The query endpoint supports these additional validated related reports:

```json
{"report_type": "players_lineup_history"}
```

```json
{"report_type": "hitters_arsenal_splits"}
```

They use explicit field catalogs from `GET /my-dashboard/report-types`, SQL validation, stable pagination, and canonical-player joins. Arsenal split rows only include active resolved hitters. These one-to-many reports do not redefine the default active-player population.

## Suggested Railway schedule

Keep the command opt-in until the first production run is inspected. After acceptance, schedule the current refresh after the upstream roster, Statcast aggregate, and Stored 365 jobs finish:

```bash
python scripts/refresh_dashboard_player_projection.py --refresh
```

Recommended environment controls:

```text
DASHBOARD_ACTIVE_PLAYER_WINDOW_DAYS=30
DASHBOARD_PROJECTION_STALE_HOURS=36
```

Do not run overlapping projection refreshes. The command is idempotent for identical approved content, but simultaneous source collection wastes capacity and makes run evidence harder to interpret.

## Production verification

1. Deploy the merged schema, status endpoint, report queries, and operator command.
2. Run status-only inspection and save the empty/prior baseline.
3. Run the explicit current refresh.
4. Save the emitted JSON counts.
5. Call `GET /my-dashboard/canonical/status` from production.
6. Query `all_active_hitters` and `all_active_pitchers` without filters.
7. Page through results and confirm stable, gap-free totals.
8. Apply one team filter and one metric filter.
9. Apply a weight-only change and confirm count stability with ordering change.
10. Query the two related report types.
11. Open the Report Builder on desktop and mobile and switch among primary objects.
12. Smoke-test Matchups, Matchup Detail, Daily Odds, Model Projections, and AI Data Assistant.
13. Attach counts, versions, timestamps, tests, build result, deployment logs, and smoke-test results to issue #1055.

## Failure and rollback

- If verified team/roster collection fails, the current projection is not promoted.
- If the snapshot builder returns empty or incomplete coverage, promotion is rejected.
- If promotion fails after staging, snapshots and current changes roll back.
- The failure is recorded in `dashboard_projection_runs`.
- Continue serving the previous current projection while repairing the source.
- Do not delete `dashboard_players`, `dashboard_player_snapshots`, `dashboard_player_current`, or `my_dashboard_records`.
- Roll back application code normally if required; the additive tables and immutable snapshots are safe to retain.
