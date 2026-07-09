# Phase 7 Finalization Report

Related issue: #958  
Epic: #944

## Status

Phase 7 is the final EPIC acceptance phase. This report is intentionally checked into the repo so benchmark evidence and probability-source verification have a stable place to land.

Current PR scope:

- Calendar frontend now uses `GET /matchups/calendar/schedule` for initial calendar load.
- Calendar refresh now warms `POST /matchups/calendar/snapshot` and reloads the lightweight schedule snapshot.
- `scripts/phase7_benchmark.py` captures cold/warm route timings, payload sizes, cache headers, probability-source headers, and debug hotspot payloads.

## Merged / active sprint phases

| Phase | Scope | PR / Issue | Status |
|---|---|---:|---|
| Phase 1 | Probability/performance discovery map | #951 | Merged |
| Phase 2 | Instrumentation, payload bytes, debug hotspots | #952 | Merged |
| Phase 3 | Model Projections probability contract | #953 | Merged |
| Phase 4 | Lightweight calendar + warming routes | #954 | Merged |
| Phase 5 | Shared artifact metadata | #955 | Merged |
| Phase 6 | Formula/simulation cache | #957 | Active/merge pending at time this report was created |
| Phase 7 | Request sequencing, verification, benchmark evidence | #958 | Active |

## Installed routes used by Phase 7

| Route | Purpose | Expected cold path | Expected warm path |
|---|---|---|---|
| `GET /matchups/calendar/schedule` | Lightweight calendar/date-selector load | Schedule-only snapshot | Shared calendar artifact/cache |
| `POST /matchups/calendar/snapshot` | Warm lightweight calendar snapshots | Schedule-only snapshot build | Shared calendar artifact/cache |
| `GET /matchups?date=<date>` | Daily Matchups | Matchups date cache or build | Matchups cache |
| `POST /matchups/snapshot/<date>` | Warm Daily Matchups | Matchups build | Matchups cache |
| `GET /matchup/{game_pk}` | Matchup Detail / Overview | Detail build | Detail/cache path if available |
| `GET /models/projections?date=<date>` | Model Projections | Projection artifact or build | Model projection artifact/cache |
| `POST /models/projections/snapshot/{date}` | Warm Model Projections | Projection artifact build | Model projection artifact/cache |
| `GET /debug/performance` | Runtime performance snapshot | Current process diagnostics | Current process diagnostics |
| `GET /debug/performance/hotspots` | Runtime hotspot snapshot | Current process diagnostics | Current process diagnostics |

## Frontend/API request sequencing matrix

| Surface | Previous behavior | Phase 7 behavior | Remaining note |
|---|---|---|---|
| Calendar / date selector | `GET /matchups/calendar`, which could trigger full `generate_matchups_for_date` for yesterday/today/tomorrow | `GET /matchups/calendar/schedule`, schedule-only initial payload | Implemented in `frontend/src/pages/YesterdayTodayPage.jsx` |
| Calendar refresh | `POST /matchups/snapshot/{date}` then heavy calendar reload | `POST /matchups/calendar/snapshot` then lightweight calendar reload | Implemented |
| Daily Matchups / HomePage | Uses `/matchups?date=<date>` | No frontend change in this PR | Should benefit from prior cache/instrumentation; benchmark required |
| Matchup Detail / Overview | Uses `/matchup/{game_pk}` | No frontend change in this PR | Benchmark required; future reuse of projection artifacts may remain |
| Model Projections | Uses `/models/projections?date=<date>` | No frontend change in this PR | Route already uses Phase 3 probability contract and Phase 5 artifact keys; Phase 6 adds internal cache wrapper |
| Daily Odds | Existing Daily Odds API/page flow | No frontend change in this PR | Benchmark/audit required; probability source must be verified |

## Probability-source verification matrix

| Surface | Required source behavior | Phase 7 verification method | Status |
|---|---|---|---|
| `/models/projections` | Displayed/default aliases use Model Projections output when available | `scripts/phase7_benchmark.py` reads `model_projection_probability.source` counts | Script added; run required after deploy |
| Model Projections page | UI uses shared derived simulation / route aliases | Existing frontend already reads shared derived simulation for visible win cards | Needs production verification |
| Daily Matchups / HomePage | Should display Model Projections where available or explicit fallback | Requires route/component verification | Pending benchmark/audit |
| Matchup Detail / Overview | Should display Model Projections where available or explicit fallback | Requires route/component verification | Pending benchmark/audit |
| Daily Odds | Should prefer Model Projection probability where available | Requires route/component verification | Pending benchmark/audit |

## Benchmark command

Run against production or a deployed preview after this PR is deployed:

```bash
python scripts/phase7_benchmark.py \
  --base-url https://mlbgpt.com \
  --date YYYY-MM-DD \
  --output docs/phase7_benchmark_output.json
```

The script records:

- cold/warm elapsed ms
- response payload bytes
- `X-Response-Time-ms`
- `X-Payload-Bytes`
- `X-Cache`
- `X-Probability-Source`
- `/debug/performance`
- `/debug/performance/hotspots`
- Model Projection probability-source counts

## Benchmark evidence

Paste or commit `docs/phase7_benchmark_output.json` after deployment.

| Route/page | Cold ms | Warm ms | Payload bytes | Cache status | Probability source | Notes |
|---|---:|---:|---:|---|---|---|
| `/matchups/calendar/schedule` | Pending | Pending | Pending | Pending | not loaded on initial calendar | Calendar should not trigger heavy matchup build |
| `/matchups?date=<date>` | Pending | Pending | Pending | Pending | Pending | Daily Matchups |
| `/matchup/{game_pk}` | Pending | Pending | Pending | Pending | Pending | Matchup Detail |
| `/models/projections?date=<date>` | Pending | Pending | Pending | Pending | model_projections where available | Model Projections |
| Daily Matchups frontend waterfall | Pending | Pending | Pending | Pending | Pending | Browser/network capture required if not covered by backend script |
| Model Projections frontend waterfall | Pending | Pending | Pending | Pending | Pending | Browser/network capture required if not covered by backend script |
| Daily Odds frontend/API waterfall | Pending | Pending | Pending | Pending | Pending | Browser/network capture required if not covered by backend script |

## EPIC completion checklist

- [x] Phase 7 issue created from final five EPIC acceptance bullets.
- [x] Calendar initial load switched to installed lightweight route.
- [x] Calendar refresh switched to installed lightweight snapshot route.
- [x] Benchmark runner added.
- [ ] Phase 6 merged/deployed if not already merged.
- [ ] Phase 7 deployed.
- [ ] Production or preview benchmark output committed/attached.
- [ ] Probability-source verification completed for all displayed/default surfaces.
- [ ] Remaining exceptions documented, or none confirmed.
- [ ] EPIC #944 closed only after evidence supports completion.

## Final completion statement

Not complete yet. Phase 7 code is in progress; final benchmark output and probability-source verification must be produced after deployment before the EPIC can be closed.
