# My Dashboard baseball-state operations

My Dashboard is driven by the existing MLB matchup and lineup lifecycle. It does not require a separate cron-verification workflow to decide whether models are confirmed.

## Authoritative flow

1. The app builds the daily matchup slate from MLB game data and probable starters.
2. Before batting orders post, lineup-sensitive reports are `projected`.
3. As MLB boxscore lineups arrive, the active-lineup index moves to `partial` and verified hitters enter the report universe.
4. When every checked game has a batting order, the report becomes `confirmed`.
5. A changed lineup identity produces a new `lineup_revision`; active-lineup solver caches expire on a 30-second baseball-time window and rebuild the existing model shell automatically.

## What operators should inspect

Use the report response itself:

- `model_state`
- `lineup_revision`
- `lineup_filter.lineup_status`
- `lineup_filter.games_checked`
- `lineup_filter.games_with_lineups`
- `lineup_filter.teams_with_lineups`
- `lineup_filter.confirmed_batter_count`

The dashboard should not present yesterday's lineup as today's confirmation. Yesterday hydration remains historical cache warming only.

## Betting meaning

`projected` is the overnight/early-market board. `lineup_building` is a mixed slate where only posted orders are verified. `confirmed` is the final lineup-aware board for batter props, matchup edges, and final price comparison.
