# My Dashboard production operations

## Purpose

My Dashboard builds a complete daily report universe for hitters, pitchers, teams, totals, and overall players. The report query API then filters, sorts, counts, and paginates that universe.

Pagination is a response-size control. It is not a top-player limit.

## Query contract

The solver endpoints return a Salesforce-inspired response:

- `totalSize`: complete filtered result count
- `records`: current page
- `done`: whether the current page is the final page
- `page_info`: page number, size, count, and navigation state
- `object_info.fields`: server-owned field metadata

Primary endpoints:

- `POST /my-dashboard/solver`
- `POST /my-dashboard/solver/active-lineups`
- `POST /my-dashboard/solver/batch`

## Confirmed-lineup policy

For hitter and overall-player reports, the active-lineup endpoint uses the lineup for the exact requested MLB date.

Statuses are explicit:

- `confirmed`: every checked game has a published lineup
- `partial`: at least one lineup is available, but the slate is incomplete
- `unavailable`: no verified lineup was available
- `not_applicable`: the report object is not hitter scoped

An unavailable lineup does not silently fall back to yesterday while presenting the result as today's confirmed lineup. Yesterday hydration is cache warming for yesterday's confirmed slate, not a substitute for today's lineup.

## Hydration endpoint

Recommended production request:

```http
POST /my-dashboard/solver/hydrate-yesterday
Content-Type: application/json

{
  "active_lineups": true,
  "force": true
}
```

`force: true` is recommended for the scheduled production run so the execution performs and records a fresh build instead of returning a previously cached hydration payload.

The response includes an `execution` object with:

- run ID
- target date
- start and completion timestamps
- duration
- requested components
- component result counts
- lineup coverage counts
- warnings
- error status
- cache mode

## Status and health

- `GET /my-dashboard/health`
- `GET /my-dashboard/hydration/status`

The status endpoint reports the latest hydration observed by the current application process and the declared cron configuration.

The latest execution status is process-local. A Railway restart resets it to `never_run_in_this_process`. Railway deployment logs remain the durable source for historical executions unless a persistent job-history table is added later.

## Railway cron configuration

Set these environment variables on the application service:

```text
MY_DASHBOARD_HYDRATION_CRON_SCHEDULE=0 10 * * *
MY_DASHBOARD_HYDRATION_TIMEZONE=America/New_York
MY_DASHBOARD_HYDRATION_PRODUCTION_VERIFIED=false
```

The exact schedule should be selected relative to the app's other daily ingestion jobs. The hydration job should run only after yesterday's matchups, Stored 365 rows, model projections, and boxscore lineups are available.

The cron must call the public Railway service URL. It must not call `127.0.0.1` unless the cron process also starts the API server in the same container.

Example command pattern:

```bash
curl --fail --show-error --silent \
  --request POST \
  --header 'Content-Type: application/json' \
  --data '{"active_lineups":true,"force":true}' \
  "$MY_DASHBOARD_BASE_URL/my-dashboard/solver/hydrate-yesterday"
```

Required variable for that command:

```text
MY_DASHBOARD_BASE_URL=https://<production-domain>
```

## Production verification checklist

1. Deploy the merged revision.
2. Call the hydration endpoint manually with `force: true`.
3. Confirm HTTP 200.
4. Confirm `execution.status` is `success`.
5. Confirm `execution.target_date` is yesterday's MLB date.
6. Confirm all requested components are present.
7. Confirm hitter candidate and deduplicated counts are greater than the displayed page size when data supports it.
8. Confirm lineup status and games-with-lineups counts are plausible.
9. Call `/my-dashboard/hydration/status` and match the run ID.
10. Configure the Railway cron.
11. Observe one scheduled run in Railway logs.
12. Set `MY_DASHBOARD_HYDRATION_PRODUCTION_VERIFIED=true` only after that scheduled run succeeds.

## Failure interpretation

### `never_run_in_this_process`

The service started or restarted and no hydration has run in that process.

### `failed`

Inspect `error`, component warnings, database connectivity, projection generation, and lineup-fetch failures.

### `partial`

This is not necessarily a processing failure. It usually means some MLB lineups were not yet published for the requested date.

### Zero confirmed hitters

Check:

- requested date
- matchup generation
- game PK availability
- boxscore lineup response
- player ID and team matching
- Stored 365 hitter coverage

## Validation commands

```bash
pytest tests/test_my_dashboard_report_query.py
pytest tests/test_my_dashboard_observability.py
pytest tests/test_active_lineup_solver.py
cd frontend && npm test
cd frontend && npm run build
```

## Sprint completion criteria

My Dashboard is complete when:

- no report-layer top-10 cap exists
- the complete daily universe is scored before pagination
- confirmed-lineup filtering uses the complete hitter universe
- server sorting and counts remain stable across pages
- the frontend consumes server field metadata
- current-page and complete-report exports work
- saved definitions restore filters, fields, lineup mode, page size, and sort
- hydration produces an observable execution summary
- one real Railway scheduled execution is verified
