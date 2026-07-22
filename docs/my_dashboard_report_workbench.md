# My Dashboard Report Workbench Architecture

## Purpose

My Dashboard should evolve from a curated card surface into the first metadata-driven workbench inside MLBGPT.

The goal is not to replace the model formulas. The goal is to expose the formulas through a faster, mobile-stable, metadata-driven interface that behaves like metadata-driven reports and field management:

- choose an object/component
- inspect available fields
- apply filters
- run the query/formula
- view results in a table
- save or pin the result

## Current repo foundation

The repo is already close enough to build Workbench v0 without starting from raw SQL.

Existing backend foundation:

- `SUPPORTED_COMPONENTS = {"hitters", "pitchers", "teams", "totals", "overall_players"}` in `mlb_app/my_dashboard_solver.py`
- `available_filters_for_component()` returns component-level field/filter metadata
- `normalize_filter_payload()` already supports basic filters, metric min/max filters, and metric weights
- `AppDashboardItem` persists `payload_json`, `filter_json`, and `sort_json`
- active-lineup solving exists as an additive wrapper and must stay additive
- hydration endpoint exists for yesterday confirmed 1-9 lineups

## Report Builder mapping

| Report Builder concept | MLBGPT equivalent |
| --- | --- |
| Object | Dashboard component / future workbench object |
| Field list | Item fields + metrics returned by `available_filters` |
| Filter | `filters` payload |
| Filter operator | `equals`, `contains`, `min`, `max`, `in` |
| Report rows | Solver result `items` |
| Report builder | Workbench UI |
| Dashboard component | Saved `AppDashboardItem` |
| Object Manager | Workbench object/field registry |

## Workbench v0 objects

The first Workbench version should wrap the existing dashboard components:

1. `hitters`
2. `pitchers`
3. `teams`
4. `totals`
5. `overall_players`

Do not start by exposing arbitrary raw SQL. Raw ORM objects can be promoted later after a formal schema registry is built.

## Required mobile behavior

My Dashboard must work at mobile widths before more features are added.

Requirements:

- page shell renders before formula results finish
- controls stack vertically below tablet widths
- filters are reachable with thumb-sized controls
- cards and tables do not overflow the viewport except controlled horizontal result-grid scrolling
- each component has its own loading and error state
- a slow component cannot freeze the entire dashboard
- primary actions remain visible: refresh, filter, save/pin, inspect fields

## Required performance behavior

The formulas are expensive and should not all block initial page load.

Recommended load model:

1. Fetch `/my-dashboard/health` first for supported components and hydration policy.
2. Render the dashboard shell immediately.
3. Load components progressively, one component request at a time or with a low concurrency cap.
4. Cache component payloads by date, component, active-lineup flag, and normalized filters.
5. Keep stale component results visible while refreshing.
6. Avoid running all component formulas inside a single blocking frontend effect.

## Result-size policy

The existing card dashboard can keep a top-10 default.

Workbench/table mode needs an explicit row contract:

- `limit`
- `offset` or `page`
- `page_size`
- `total_count_before_filters`
- `total_count_after_filters`
- `returned_count`

Do not silently cap Workbench results to ten rows. The card UI and table UI should have separate result-size expectations.

## Backend contract proposal

Add optional request fields to dashboard solver requests in a backward-compatible way:

```json
{
  "date": "2026-07-10",
  "component": "hitters",
  "filters": {},
  "view_mode": "cards",
  "limit": 10,
  "offset": 0,
  "include_available_fields": true
}
```

`view_mode = "cards"` keeps current behavior.

`view_mode = "workbench"` returns larger/paginated table-ready results.

## Frontend architecture proposal

The My Dashboard page should be split into smaller units:

- `DashboardShell`
- `DashboardObjectPicker`
- `DashboardFilterPanel`
- `DashboardFieldPanel`
- `DashboardComponentCard`
- `DashboardResultsTable`
- `useDashboardComponentPayload`
- `useProgressiveDashboardHydration`

The shell should not wait for every component payload before rendering.

## AI role

AI should not produce the final answer first.

AI should translate language into a visible query/filter state:

User says:

> Show me left-handed hitters with strong xwOBA and EV today.

Workbench should populate:

- object: `hitters`
- metric filters: `xwOBA` minimum, `EV` minimum if supported
- sort: score or selected metric descending
- active lineups: true where applicable

Then the user can inspect and run the query.

## Acceptance criteria

Phase 1 is complete when:

- mobile dashboard renders cleanly from 320px width upward
- first render does not block on all formulas
- component results load independently
- filters remain usable on mobile
- backend remains backward compatible
- metadata-driven workbench contract is documented and ready for implementation

Phase 2 is complete when:

- Workbench object picker exists
- available fields/metrics are visible per object
- filter builder writes the existing filter contract
- table results are available with larger/paginated row counts
- saved dashboard items can store Workbench filters/sorts
- AI can populate filters instead of merely writing prose

## Immediate next ticket: roles and Workbench language

After the report-shelf polish and rename controls, define a server-owned `user` versus `admin` capability contract before adding an advanced Workbench. Standard users should continue using allowlisted report types, fields, filters, weights, and personal folders. Administrators may receive broader dataset inspection and advanced report tools only through explicit capabilities returned by the authenticated profile.

The advanced Workbench must use a constrained MLBGPT report language compiled into parameterized, allowlisted database operations. Unrestricted raw SQL is out of scope until a separate design covers read-only enforcement, schema allowlists, row limits, timeouts, and audit logging.
