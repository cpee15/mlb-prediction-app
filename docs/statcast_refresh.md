# Statcast Refresh and Data Freshness

This app does not refresh Statcast data during normal user requests. The leaderboard, pitcher overview, and matchup detail pitcher sections are read paths over stored database rows.

## Why this exists

Pitcher overview and batter leaderboard cards can look empty or stale when the production database has not been refreshed. PR #330 enriched the starter overview payload, but that change does not fetch Statcast rows or create database aggregates by itself. It reads existing DB-backed fields first, uses MLB Stats API fallback for standard season pitching stats, and intentionally keeps FIP, xFIP, SIERA, and xSIERA null unless trusted inputs exist.

## Nightly command

The production refresh command is:

```bash
python scripts/nightly_statcast_refresh.py --days 2 --include-today
```

This is wired through `.github/workflows/nightly-statcast-refresh.yml` and can also be triggered manually through GitHub Actions.

## Required production secret

The workflow requires:

```text
DATABASE_URL
```

The refresh script refuses to silently use local SQLite unless `--allow-sqlite-local` is passed explicitly for local validation.

## Tables refreshed

The refresh job uses the existing ETL logic in `mlb_app/etl.py` and updates the surfaces the app already reads:

- `statcast_events` for pitch-level Statcast rows used by pitcher/batter rolling views and batter leaderboards.
- `pitcher_aggregates` for DB-first pitcher overview and matchup detail pitcher cards.
- `pitch_arsenal` for current-season pitch mix, usage, whiff, xwOBA, and hard-hit context.
- `team_splits` for matchup offense inputs and split fallback context.

## Read-only surfaces

These endpoints should not run live Statcast pulls:

- `/batters/leaderboards`
- `/batter/{id}/profile`
- `/batter/{id}/rolling/*`
- `/matchups`
- `/matchup/{game_pk}`

The right fix for stale data is the scheduled refresh job, not request-time scraping.

## Freshness endpoint

Use this endpoint to inspect production DB freshness without triggering ETL:

```text
GET /data/freshness
```

It returns:

- latest Statcast event date
- Statcast days stale
- latest pitcher aggregate date
- pitcher aggregate days stale
- latest batter leaderboard source date
- batter leaderboard days stale
- current-season pitch arsenal count
- current-season batter terminal row count
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
