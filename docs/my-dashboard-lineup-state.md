# My Dashboard lineup-state promotion

My Dashboard follows the baseball slate, not a separate hydration lifecycle.

## State machine

1. `projected` — probable pitchers and matchup context exist, but no confirmed starting lineup is available.
2. `lineup_building` — one or more games have confirmed batting orders and the report is filtered to those verified hitters.
3. `confirmed` — every checked game has a confirmed lineup and the model/report shell represents the confirmed slate.

## Automatic promotion

Today's lineup index and active-lineup report payloads use a maximum 30-second cache window. When MLB boxscore lineup identity changes, the next rebuild produces a new `lineup_revision`, reruns the existing dashboard solver against the updated confirmed hitter universe, and promotes `model_state` automatically.

No separate cron verification is required for this transition. The existing matchup/lineup data path remains authoritative.

## Response fields

Active-lineup reports expose:

- `model_state`
- `lineup_revision`
- `lineup_filter.lineup_status`
- `lineup_filter.confirmed_batter_count`
- `lineup_filter.games_checked`
- `lineup_filter.games_with_lineups`
- `lineup_filter.teams_with_lineups`

These fields let the frontend distinguish an early projected board, a partially posted slate, and the final confirmed board without changing report objects or creating duplicate records.

## Betting interpretation

- `projected`: useful for overnight and early-market pricing, with lineup uncertainty still embedded.
- `lineup_building`: actionable only for games whose batting orders are verified; unposted games remain outside confirmed-hitter reports.
- `confirmed`: the preferred state for final batter matchup rankings, prop screens, and lineup-sensitive model outputs.
