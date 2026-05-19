# Statcast Refresh and Data Freshness

This app does not refresh Statcast data during normal user requests. The leaderboard, pitcher overview, pitcher recent-outing, rolling-stat, and matchup detail sections are read paths over stored database rows.

## Why this exists

Pitcher overview and batter leaderboard cards can look empty or stale when the production database has not been refreshed. PR #330 enriched the starter overview payload, but that change does not fetch Statcast rows or create database aggregates by itself. It reads existing DB-backed fields first, uses MLB Stats API fallback for standard season pitching stats, and intentionally keeps FIP, xFIP, SIERA, and xSIERA null unless trusted inputs exist.

The shared root table for Batter leaderboards, Batter data-quality latest event dates, Pitcher Recent Outings, pitcher rolling stats, batter rolling stats, and Statcast fallback context is `statcast_events`. Refreshing only the day's probable starters is not enough because it misses relievers, actual non-probable starters, all hitters, and any game rows not represented by probable-pitcher schedule metadata.

## Nightly command

The production refresh command is:

```bash
python scripts/nightly_statcast_refresh.py --days 10 --include-today
```

The script defaults to a 10-day rolling overlap when `--days` is omitted. The overlap is intentional: it handles late Statcast availability, late-night games, missed cron windows, doubleheaders, and rows that become complete after the first ingest.

This command should run as a Railway cron job or Railway scheduled service so it executes inside the Railway environment that already has the production `DATABASE_URL`. Do not use GitHub Actions for this job unless a separate GitHub Actions database secret is intentionally created.

## Railway cron setup

Create a Railway cron or scheduled job attached to the same project/environment as the production backend.

Command:

```bash
python scripts/nightly_statcast_refresh.py --days 10 --include-today
```

Required environment:

```text
DATABASE_URL
```

Railway already provides `DATABASE_URL` to services in the environment when the database is attached. The refresh script refuses to silently use local SQLite unless `--allow-sqlite-local` is passed explicitly for local validation.

## Refresh behavior

The refresh job now uses date-wide Statcast ingestion for each target date. It fetches all pitch-level Statcast rows for the date, not just the probable pitchers from the schedule.

`statcast_events` is updated idempotently:

- primary pitch identity: `game_pk + at_bat_number + pitch_number + pitcher_id + batter_id`
- fallback identity for incomplete rows: `game_date + pitcher_id + batter_id + pitch_type + events + release_speed + launch_speed + launch_angle + balls + strikes + inning + inning_topbot + outs_when_up`
- existing rows are updated with better non-null values
- rerunning the same date should be mostly no-op/update, not duplicate inserts
- the refresh does not delete or rebuild the full season

## Tables refreshed

The refresh job uses the ETL logic in `mlb_app/etl.py` and updates the surfaces the app already reads:

- `statcast_events` for pitch-level Statcast rows used by pitcher/batter rolling views, Batter leaderboards, Pitcher Recent Outings, ordered at-bats, and H2H fallback.
- `pitcher_aggregates` for DB-first pitcher overview, matchup detail pitcher cards, model projections, and starter overview.
- `batter_aggregates` for Batter profile aggregate cards and multi-season Batter views.
- `pitch_arsenal` for current-season pitch mix, usage, whiff, xwOBA, and hard-hit context.
- `team_splits` for matchup offense inputs and split fallback context.
- `batter_pitch_type_matchups` remains the Stored 365 / Batter vs Arsenal table and should be refreshed after base `statcast_events` is current.

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
