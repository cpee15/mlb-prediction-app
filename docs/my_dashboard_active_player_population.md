# My Dashboard active-player population contract

## Scope

PR 2 populates only `dashboard_players`. It does not create analytical snapshots, promote `dashboard_player_current`, or change either My Dashboard solver route. Those remain PRs 3 and 4.

## Identity rules

- The canonical key is a positive MLBAM player ID from an explicit ID field or MLB Stats `person.id`.
- Names are descriptive attributes and never create or merge identities.
- A new canonical row requires both an MLB ID and a non-empty name from a verified source.
- A tracked-game row with an ID but no name may update an already-resolved canonical row. Otherwise it is returned in `unresolved_identities` and is not silently persisted.
- Multiple source rows with the same MLB ID are merged into one candidate.

## Verified inputs

- Confirmed lineups: existing matchup generation plus `lineup_profile.fetch_boxscore_lineup`.
- Tracked games: grouped `statcast_events` batter and pitcher IDs within the activity window.
- Active rosters: MLB Stats `/teams/{team_id}/roster` with `rosterType=active`.
- Usable analytics gate: existence in `batter_aggregates` or `pitcher_aggregates`.
- Projected lineup rows may be supplied by a later orchestration layer, but must already carry an explicit MLB ID.

## Eligibility

The window defaults to 30 days and is configured by `DASHBOARD_ACTIVE_PLAYER_WINDOW_DAYS`.

Priority-ordered active reasons are:

1. `today_confirmed_or_projected_lineup`
2. `recent_confirmed_lineup`
3. `recent_tracked_game`
4. `active_roster_with_analytics`

All other observed candidates become `no_recent_verified_activity`.

## Refresh safety

`populate_dashboard_players()` is idempotent for the same source facts and preserves earlier identity/activity dates. It does not deactivate canonical players missing from a partial refresh by default. A caller may set `transition_missing_players=True` only after it has verified that the refresh input is complete; missing active rows then transition to `not_observed_in_complete_refresh`.

The service returns resolved, unresolved, created, updated, activated, deactivated, active-hitter, and active-pitcher counts. It raises verified roster-source failures instead of converting them into a deceptive empty roster.
