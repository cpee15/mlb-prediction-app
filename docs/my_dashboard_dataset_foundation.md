# My Dashboard dataset foundation

Issue: #1043

## Preserved public contract

This foundation does not change the current frontend or route behavior. The existing Report Builder, `/my-dashboard/solver`, `/my-dashboard/solver/active-lineups`, saved report payloads, scoring formulas, and other application surfaces remain untouched.

## Responsibility boundary

- Existing My Dashboard solvers remain the analytical candidate builders.
- `my_dashboard_records` is the persisted, reportable dataset owned exclusively by My Dashboard.
- A later SQL query PR will read this dataset using the existing metadata-driven `items`, `records`, `totalSize`, `page_info`, and `object_info` response contract.
- User filters and scoring weights are query definitions and are never stored in the shared analytical dataset.

## Dataset identity

A dataset version is isolated by:

- MLB date
- component
- mode (`standard` or `active_lineups`)
- dataset version
- entity key

A new version is written as non-current and promoted only after all rows flush successfully. The previous current version is demoted in the same transaction. A build or write failure rolls back and leaves the previous valid version intact.

## Runtime considerations

The foundation adds indexed relational columns for common report predicates and JSON columns for flexible analytical metrics. This allows the next query-layer PR to push filtering, sorting, counting, and pagination into PostgreSQL without rebuilding analytical contexts for every report interaction.

No process-local cache is treated as the dataset authority.
