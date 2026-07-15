# My Dashboard snapshot and current-projection hydration

## Boundary

PR 3 creates immutable `dashboard_player_snapshots` and atomically promotes `dashboard_player_current`. It does not route Report Builder requests to the new projection; that remains PR 4.

## Source model

The canonical population always comes from active, resolved `dashboard_players`. Batter and pitcher aggregates provide baseline scalar metrics. The newest compatible current `my_dashboard_records` row may overlay reportable metrics and score, but it cannot add or remove players from the population.

This preserves useful existing dataset work without mistaking a daily component result for the complete player universe.

## Refresh lifecycle

1. Build and validate all candidate rows before writing.
2. Require unique canonical MLB IDs.
3. For a full refresh, require exact coverage of the active canonical population.
4. Derive a deterministic per-player content version and a deterministic batch projection version.
5. Reuse an identical existing snapshot; create a new snapshot only when that player’s approved reportable content changed.
6. Flush snapshots and current-row promotion in one transaction.
7. Run the optional promotion guard after staging and before commit.
8. Roll back every staged snapshot/current change on any failure.

An empty full refresh is rejected by default. Partial refreshes never delete current rows outside their requested subset. A successful complete refresh removes current rows for players no longer in the active canonical population.

## Failure safety and idempotency

- A builder or source failure occurs before shared rows are changed.
- A validation, database, or promotion-guard failure rolls back the entire date refresh.
- Repeating identical inputs reuses snapshots and leaves the current projection unchanged.
- A one-player metric change creates one new historical snapshot and reuses unchanged player snapshots.
- User filters and weights are not accepted or persisted by this service.

## Backfill evidence

`backfill_player_projection()` processes dates in order, retains a result per successful date, and promotes current rows only for the final requested date. Its response includes requested/successful/failed date counts, per-date projection versions and row counts, total snapshots created, final current row count, historical snapshot count, and explicit failures.

These counts are execution evidence only. They are not a claim that production backfill has run.
