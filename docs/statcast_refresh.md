# Statcast Refresh and Data Freshness

This app does not refresh Statcast data during normal user requests. The leaderboard, pitcher overview, pitcher recent-outing, rolling-stat, and matchup detail sections are read paths over stored database rows.

## Why this exists

Pitcher overview and batter leaderboard cards can look empty or stale when the production database has not been refreshed. PR #330 enriched the starter overview payload, but that change does not fetch Statcast rows or create database aggregates by itself. It reads existing DB-backed fields first, uses MLB Stats API fallback for standard season pitching stats, and intentionally keeps FIP, xFIP, SIERA, and xSIERA null unless trusted inputs exist.

The shared root table for Batter leaderboards, Batter data-quality latest event dates, Pitcher Recent Outings, pitcher rolling stats, batter rolling stats, and Statcast fallback context is `statcast_events`. Refreshing only the day's probable starters is not enough because it misses relievers, actual non-probable starters, all hitters, and any game rows not represented by probable-pitcher schedule metadata.

## Railway jobs

There are two Railway refresh paths. Keep both separate.

### Existing fast matchup job

Keep the existing Railway job that runs:

```bash
python scripts/run_refresh_job.py
```

Do not remove or weaken this job. It is responsible for the fast app refresh path: projected/confirmed starters, probable pitchers, lineups, matchup payload warming, snapshots, Batter vs Arsenal / Stored 365 refresh, and cache clearing.

This job is good at keeping the live matchup analyzer usable. It should continue to run exactly as the production app expects.

Important: this job does **not** guarantee Batter landing page freshness by default because its heavy Statcast path is gated behind `RUN_STATCAST_ETL`, which defaults to `0`. That means it can successfully refresh probables, lineups, matchups, snapshots, and Batter vs Arsenal while `/batters/leaderboards` still shows stale `statcast_events` data.

### Add the Statcast freshness job

Add a separate Railway scheduled job for date-wide Statcast freshness:

```bash
python scripts/nightly_statcast_refresh.py --days 10 --include-today
```

This is the required job for:

- Batter landing page latest event date
- Batter leaderboard source rows
- Batter profile data quality
- Pitcher Recent Outings
- pitcher rolling stats
- batter rolling stats
- DB-backed Statcast freshness diagnostics

This job must run in the same Railway project/environment as the production backend so it uses the same attached production `DATABASE_URL`.

Do not replace the fast matchup job with this command. Add this as another scheduled job, or update an old Statcast-only scheduled job if one already exists. The goal is:

```text
fast matchup job stays in place
Statcast freshness job runs separately
```

## Statcast freshness command

Production command:

```bash
python scripts/nightly_statcast_refresh.py --days 10 --include-today
```

The script defaults to a 10-day rolling overlap when `--days` is omitted. The overlap is intentional: it handles late Statcast availability, late-night games, missed cron windows, doubleheaders, and rows that become complete after the first ingest.

Required environment:

```text
DATABASE_URL
```

Railway already provides `DATABASE_URL` to services in the environment when the database is attached. The refresh script refuses to silently use local SQLite unless `--allow-sqlite-local` is passed explicitly for local validation.

## Refresh behavior

The Statcast freshness job uses date-wide Statcast ingestion for each target date. It fetches all pitch-level Statcast rows for the date, not just the probable pitchers from the schedule.

`statcast_events` is updated idempotently:

- primary pitch identity: `game_pk + at_bat_number + pitch_number + pitcher_id + batter_id`
- fallback identity for incomplete rows: `game_date + pitcher_id + batter_id + pitch_type + events + release_speed + launch_speed + launch_angle + balls + strikes + inning + inning_topbot + outs_when_up`
- existing rows are updated with better non-null values
- rerunning the same date should be mostly no-op/update, not duplicate inserts
- the refresh does not delete or rebuild the full season

This means running the 10-day overlap every night should not duplicate `statcast_events`. The second run over the same dates should mostly report `noop` or `updated`, not new inserts for rows that already exist.

## Tables refreshed by Statcast freshness

The Statcast freshness job uses the ETL logic in `mlb_app/etl.py` and updates the surfaces the app already reads:

- `statcast_events` for pitch-level Statcast rows used by pitcher/batter rolling views, Batter leaderboards, Pitcher Recent Outings, ordered at-bats, and H2H fallback.
- `pitcher_aggregates` for DB-first pitcher overview, matchup detail pitcher cards, model projections, and starter overview.
- `batter_aggregates` for Batter profile aggregate cards and multi-season Batter views.
- `pitch_arsenal` for current-season pitch mix, usage, whiff, xwOBA, and hard-hit context when the Savant arsenal leaderboard is unavailable.

## Batter vs Arsenal refresh

`batter_pitch_type_matchups` is the Stored 365 / Batter vs Arsenal table. It is refreshed by the existing fast refresh path through `scripts/run_hitting_matchups_refresh.py`.

That Batter vs Arsenal refresh reads existing `statcast_events` rows and summarizes batter performance by `batter_id + pitch_type`. Therefore the best order is:

```text
1. Statcast freshness job keeps statcast_events current.
2. Existing fast matchup job keeps Batter vs Arsenal and matchup cache current.
```

The two jobs are complementary. The Statcast job feeds the base data. The existing fast job keeps the app-facing matchup and Batter vs Arsenal surfaces warm.

## Read-only surfaces

These endpoints should not run live Statcast pulls:

- `/batters/leaderboards`
- `/batter/{id}/profile`
- `/batter/{id}/rolling/*`
- `/pitcher/{id}`
- `/pitcher/{id}/rolling`
- `/matchups`
- `/matchup/{game_pk}`
- `/matchup/{game_pk}/competitive`

The right fix for stale data is the scheduled refresh job, not request-time scraping.

## Freshness endpoint

Use this endpoint to inspect production DB freshness without triggering ETL:

```text
GET /data/freshness
```

It returns:

- latest Statcast event date
- latest pitcher event date
- latest batter event date
- Statcast, pitcher-event, and batter-event days stale
- latest pitcher aggregate date
- latest batter aggregate date
- latest batter leaderboard source date
- current-season pitch arsenal count
- current-season batter terminal row count
- current-season Statcast row count
- distinct pitchers with rows in the last 7 days
- distinct batters with rows in the last 7 days
- latest Stored 365 / BatterPitchTypeMatchup refresh timestamp
- database URL type without exposing secrets
- status: `fresh`, `stale`, or `empty`
- warnings

## Production validation after the Statcast job runs

After the Railway Statcast freshness job completes, verify these routes:

```text
GET /data/freshness
GET /batters/leaderboards
GET /pitcher/{id}
GET /pitcher/{id}/rolling
GET /matchups?date=YYYY-MM-DD
GET /matchup/{game_pk}/competitive
```

Expected results:

- `/data/freshness.latest_statcast_event_date` is newer than the stale date previously shown in production.
- `/data/freshness.latest_batter_event_date` is newer than the stale date previously shown in production.
- `/data/freshness.batter_leaderboard_latest_event_date` is newer than the stale date previously shown in production.
- `/batters/leaderboards.latest_event_date` matches the refreshed Batter leaderboard source date.
- Pitcher Recent Outings include recent games for pitchers who appeared.
- `/matchups` still returns canonical `home_win_prob` and `away_win_prob`.
- `/matchup/{game_pk}/competitive` still returns Batter vs Arsenal cards.

If `/data/freshness` still shows the old date after the Statcast scheduled job runs, the issue is in the Railway job execution logs or Statcast fetch result, not in the Batter page frontend.

## Matchup detail pitcher overview

The matchup detail page should show the enriched pitcher overview/profile data when a user clicks into a game. The implementation should not be rebuilt from scratch. The production problem is wiring, deployment, and data freshness: `/matchup/{game_pk}` must return the enriched pitcher fields already supported by the backend, the frontend must read the correct fields, and the nightly refresh must keep the underlying Statcast/PitcherAggregate/PitchArsenal data current.

## Preservation rules

Do not change these contracts as part of refresh work:

- `home_win_prob`
- `away_win_prob`
- canonical matchup probability behavior
- PR #330 DB-first starter overview enrichment
- null/missing handling for FIP, xFIP, SIERA, and xSIERA
- existing matchup detail route shape except for backward-compatible diagnostics
