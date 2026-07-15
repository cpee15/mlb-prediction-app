# My Dashboard Workbench acceptance matrix

Tracks the implementation completed under issue #1043.

## Architecture

`analytical solvers -> My Dashboard-owned persisted dataset -> SQL query/describe layer -> existing Report Builder`

## Completed slices

| Requirement | Implementation |
| --- | --- |
| Persist normalized records by date, component, mode, version, and entity | PR #1044 |
| Atomic promotion and previous-version failure safety | PR #1044 |
| SQL filtering, counting, sorting, pagination, and object metadata | PR #1045 |
| Current-date filtered route integration with stale-dataset fallback | PR #1046 |
| Existing Report Builder server pagination and sorting | PR #1047 |
| Query-time weight ranking without persisting user configuration | PR #1050 |
| Scheduled yesterday hydration into persisted Workbench datasets | PR #1051 |
| Route-selection regression coverage | This PR |

## Preserved compatibility boundaries

- Public `/my-dashboard/solver` and `/my-dashboard/solver/active-lineups` routes remain unchanged.
- Historical reports remain on the legacy compatibility path.
- Current-date unfiltered reports remain on the legacy compatibility path.
- Current-date requests with substantive filters or non-default weights use the persisted SQL path.
- Standard and active-lineup datasets remain isolated.
- Saved `report_view`, `workbench_view`, and `dashboard_report` payloads remain readable.
- Shared dataset rows never contain user filters or weights.
- Scoring formulas remain authoritative in the existing solver modules.
- Matchups, Matchup Detail, Daily Odds, Model Projections, and AI Data Assistant are outside this migration and were not changed.

## Production verification checklist

After deployment, verify:

1. A current-date filtered report returns `execution_path = my_dashboard_dataset_sql_query`.
2. Sorting a column changes server ordering across the full filtered result set.
3. Page 2 does not repeat page 1 and `totalSize` remains stable.
4. A weight-only current-date report uses the SQL dataset path and does not alter persisted rows.
5. Confirmed-lineup reports return the active-lineup dataset mode and current lineup revision.
6. Forced `/my-dashboard/solver/hydrate-yesterday` reports `dataset_hydration.dataset_source = my_dashboard_records`.
7. A failed refresh serves the prior valid current dataset only when one exists and emits an explicit warning.
8. Existing saved reports reopen with their prior columns, filters, sort, and snapshot rows.

## Known operational requirement

GitHub Actions have not consistently appeared for these connector-created PR heads. Backend tests, frontend Node tests, and the frontend production build must be executed by the repository CI or deployment pipeline before production verification is marked complete.
