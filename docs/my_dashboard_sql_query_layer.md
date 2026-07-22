# My Dashboard SQL query layer

Issue: #1043

This PR adds the database-backed query boundary for the existing metadata-driven Workbench architecture.

## Preserved behavior

- Existing `/my-dashboard/solver` routes are not changed in this PR.
- `MyDashboardReportBuilderPage.jsx` is not changed.
- Existing in-memory `apply_report_query()` remains available until route integration is proven.
- Saved reports, scoring formulas, solver builders, and other product surfaces remain unchanged.

## New query path

`query_dashboard_dataset()` reads only the current persisted `my_dashboard_records` version for the requested date, component, and dataset mode. It applies relational and registered JSON metric filters in SQL before counting, sorting, and pagination.

The response preserves the current MLBGPT contract:

- `items`
- `records`
- `totalSize`
- `total_count`
- `done`
- `query`
- `page_info`
- `object_info`

It also returns dataset provenance and page-level observability.

## Runtime safeguards

- Only allowlisted relational fields and registered metrics can be sorted.
- Stable secondary ordering uses `entity_key` and row `id`.
- Standard and active-lineup modes are isolated.
- Query-time filters and weights are never persisted.
- Weight-aware SQL ranking is deliberately deferred rather than implemented with a behavior-changing approximation.

The next PR will integrate this query service behind the existing solver route contract for current-date filtered requests after dataset freshness and hydrate-if-needed behavior are added.
