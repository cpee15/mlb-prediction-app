# Performance + Probability Sprint Notes

Related sprint issues: #944, #945, #946, #947, #948, #949, #950.

## Phase 1 status

Phase 1 is a discovery-only change. It does **not** change runtime behavior, model semantics, route outputs, frontend display logic, cache behavior, or probability values.

The purpose of this document is to map the current probability surface before implementing the migration from canonical displayed/default win probabilities to Model Projections displayed/default win probabilities.

## Search terms covered

The repo was searched for the Phase 1 terms:

- `canonical`
- `canonicalMatchupProbability`
- `canonical_game_context`
- `canonical_matchup_win_probability`
- `canonical_matchup_win_probability_v2`
- `compute_canonical_matchup_probability`
- `home_win_prob`
- `away_win_prob`
- `home_win_probability`
- `away_win_probability`
- `main_matchup_probabilities`
- `probability_components`
- `win probability`
- `Win Probability`
- `/matchups/calendar`
- `/models/projections`
- `matchups?date`
- `Model Projection`

## High-level finding

The current codebase treats canonical matchup probability as the final/default probability in several backend paths, while the Model Projections page visibly displays simulation-derived values from `sharedSimulation.derived_outputs`. This creates a source-of-truth conflict that Sprint 1 must resolve.

The most important current conflict is in `mlb_app/model_projections.py`:

- `_canonical_probability_payload(...)` copies `matchup.home_win_prob` and `matchup.away_win_prob` into `home_win_probability` / `away_win_probability`.
- It sets `source` to `matchups.canonical_matchup_win_probability_v2`.
- It labels `projection_sim.home_win_probability` / `projection_sim.away_win_probability` as `simulation_diagnostic` and `diagnostic_only_not_final_probability`.
- The returned `/models/projections` game object sets `away_win_prob`, `home_win_prob`, `away_win_probability`, and `home_win_probability` from the canonical payload.
- The route source notes explicitly say `home_win_prob and away_win_prob are canonical v2 from /matchups` and simulation outputs are diagnostics.

That conflicts with the product directive for this sprint: displayed/default win probability should come from newest actual Model Projections output percentages, not stale canonical probabilities.

## Chosen authoritative Model Projections field / contract for Sprint 1

Based on the current Model Projections page behavior, the best candidate for the authoritative displayed Model Projections percentage is:

```text
game.sharedSimulation.derived_outputs.bullpen_adjusted_game_simulation.home_win_probability
game.sharedSimulation.derived_outputs.bullpen_adjusted_game_simulation.away_win_probability
```

Fallback within Model Projections should be:

```text
game.sharedSimulation.derived_outputs.game_simulation.home_win_probability
game.sharedSimulation.derived_outputs.game_simulation.away_win_probability
```

Only after those are absent should Sprint 1 use explicitly marked fallback values from existing canonical fields.

Reasoning:

1. `build_model_projection_payload(...)` already selects `projection_sim` as `shared_bullpen_sim or shared_game_sim` after calling `build_shared_game_simulation(...)`.
2. `frontend/src/pages/ModelProjectionsPage.jsx` renders the visible Model Projections win cards from `getSharedDerivedSimulation(game)`, which returns `derived.bullpen_adjusted_game_simulation || derived.game_simulation || {}`.
3. The visible Model Projections overview uses `sharedSim.away_win_probability` and `sharedSim.home_win_probability` for the displayed Away/Home Win cards.
4. Therefore, the UI already behaves as if the shared derived simulation is the Model Projections percentage surface, even though backend top-level aliases still advertise canonical values.

Sprint 1 should define a normalized probability object whose primary values are sourced from that Model Projections derived output and whose legacy aliases are populated from it when available.

Proposed contract:

```json
{
  "home_win_probability": 0.542,
  "away_win_probability": 0.458,
  "home_win_prob": 0.542,
  "away_win_prob": 0.458,
  "source": "model_projections",
  "source_path": "sharedSimulation.derived_outputs.bullpen_adjusted_game_simulation",
  "model_version": "<actual shared/model projection version>",
  "generated_at": "<timestamp>",
  "game_pk": 123456,
  "date": "YYYY-MM-DD",
  "fallback_source": null,
  "is_fallback": false
}
```

If the Model Projections derived output is absent, fallback must be explicit:

```json
{
  "source": "fallback:canonical_matchup_win_probability_v2",
  "is_fallback": true,
  "fallback_source": "canonical_matchup_win_probability_v2",
  "missing_model_projection_reason": "sharedSimulation derived outputs missing"
}
```

## Backend routes that emit or propagate win probabilities

### `GET /matchups`

File: `mlb_app/app.py`

Current behavior:

- `list_matchups(date)` returns `generate_matchups_for_date(session, date)` directly.
- `generate_matchups_for_date(...)` builds canonical matchup outputs in `mlb_app/matchup_generator.py`.
- The output includes `home_win_prob`, `away_win_prob`, `legacy_home_win_prob`, `legacy_away_win_prob`, `model_version`, `probability_components`, `pitcher_overview`, `batter_vs_arsenal_summary`, and `missing_inputs`.

Migration impact:

- This is the primary Daily Matchups/HomePage source.
- Sprint 1 should preserve `home_win_prob` / `away_win_prob` aliases but populate visible/default values from Model Projections when available.
- Sprint 2/3 should prevent this route from cold-building more than necessary on page load.

### `GET /matchups/calendar`

File: `mlb_app/app.py`

Current behavior:

- Builds `yesterday`, `today`, `tomorrow` via `_build_date_window()`.
- For each date, if not in `MATCHUP_SNAPSHOT_CACHE`, it calls `generate_matchups_for_date(session, date_value)`.
- Returns `{ date, count, games }` where `games` is the full matchup payload.

Migration impact:

- Current calendar can synchronously trigger heavy full matchup generation for three dates.
- If the calendar displays probabilities in the future, those probabilities must use Model Projections artifacts when warmed.
- Current frontend calendar page appears to render date/count and first eight game links only, not probabilities.

### `POST /matchups/snapshot/{date_str}`

File: `mlb_app/app.py`

Current behavior:

- Populates `MATCHUP_SNAPSHOT_CACHE[date_str]` with `generate_matchups_for_date(session, date_str)`.
- Returns count.

Migration impact:

- Useful as an explicit warming path, but currently warms canonical matchup payloads.
- Sprint 2/3 should add projection probability warming alongside this.

### `POST /ai/ask`

File: `mlb_app/app.py`

Current behavior:

- For matchup/today/yesterday/weather questions, it calls or reads from `generate_matchups_for_date(...)` and returns up to 8/10 game payloads.

Migration impact:

- It can expose `home_win_prob` / `away_win_prob` inside returned game payloads.
- Should inherit the normalized Model Projection probability contract once `/matchups` output is migrated.

### `GET /matchup/{game_pk}`

File: `mlb_app/app.py`

Current behavior:

- Builds full matchup detail payload.
- Calls `compute_win_probability(...)` from `mlb_app/scoring.py`, assigning `home_win_prob` and `away_win_prob`.
- Also builds lineup offense profiles, PA outcome models, half-inning simulations, bullpen models, game simulations, bullpen-adjusted simulations, and shared game simulation.
- Returns top-level `home_win_prob` and `away_win_prob` from `compute_win_probability(...)`, not Model Projections output.

Migration impact:

- This is the most important page-specific migration target.
- Matchup Detail / Matchup Overview must display the same latest Model Projections percentage as `/models/projections` for the same game/date.
- Sprint 1 should route displayed/default `home_win_prob` / `away_win_prob` aliases through a Model Projection probability resolver.
- Sprint 3/4 should reuse game-level Model Projection probability artifacts instead of recomputing full projections cold.

### `GET /matchup/{game_pk}/competitive`

File: `mlb_app/app.py`

Current behavior:

- Returns lineup matchup matrices and does not appear to emit side win probabilities.

Migration impact:

- No direct probability migration unless frontend adds side probability display to competitive analysis.

### `GET /models/projections`

File: `mlb_app/model_projection_routes.py` and `mlb_app/model_projections.py`

Current behavior:

- Route uses cache key `model_projection:full:{date}` and returns `build_model_projection_payload(session, target_date)`.
- `build_model_projection_payload(...)` calls `generate_matchups_for_date(...)`, builds side contexts, simulation cards, shared simulations, canonical payload, canonical game context, teams, and workspace.
- Top-level `home_win_prob`, `away_win_prob`, `home_win_probability`, and `away_win_probability` are currently copied from `_canonical_probability_payload(...)`.
- The actual visible Model Projections page win cards read from shared derived simulation output.

Migration impact:

- This route must expose a normalized `model_projection_probability` / `probability` contract.
- The existing top-level legacy aliases should be repointed to Model Projections derived output when available.
- `canonicalMatchupProbability` and `canonicalGameContext` may remain as diagnostics/backward compatibility but must not be labeled final/default.

### Daily Odds routes

File: `mlb_app/daily_odds_routes.py`

Current behavior:

- `_load_matchups(...)` calls `generate_matchups_for_date(...)`.
- `_fallback_candidates_from_matchups(...)` and `_models_from_unpriced_matchups(...)` use `home_win_prob` and `away_win_prob` from matchups as internal moneyline probabilities.
- `_summarize_projection_payload(...)` currently names `_canonical_probability_summary(...)`, returns canonical home/away probabilities, includes diagnostic home/away probabilities, and labels the contract `canonical_final_probability_with_simulation_diagnostic_context`.
- `_build_daily_recap(...)` repeats the same canonical-final contract label.

Migration impact:

- Daily Odds is a direct consumer of the old canonical probability contract.
- Sprint 1 should switch its game-level probability resolution to the normalized Model Projection probability contract when available.
- The phrases `canonical_final_probability_with_simulation_diagnostic_context` should be renamed or demoted.

### Model Tracker routes/storage

Files: `mlb_app/model_tracker.py`, `mlb_app/model_tracker_routes.py`, `mlb_app/model_tracker_safe_snapshot.py`

Current behavior observed in `mlb_app/model_tracker.py`:

- `ModelTrackerSnapshot` stores `home_win_probability` and `away_win_probability` columns.
- Constants define `CANONICAL_MODEL_VERSION = "canonical_matchup_win_probability_v2"`.
- `_canonical_probability_bundle(...)` reads `main_matchup_probabilities`, `workspace.canonicalMatchupProbability`, and top-level `home_win_prob` / `away_win_prob`.
- It emits `canonical_home_win_prob`, `canonical_away_win_prob`, `final_probability_source`, and diagnostic simulation metadata.
- Normalization functions label rows/components as `canonical_game_snapshot`, `canonical_moneyline_side`, `canonical_projected_side`, and `canonical_model_projection_win_probability`.

Migration impact:

- Model Tracker is currently canonical-centric.
- Sprint 1 should either add a new Model Projection probability bundle or rename/demote canonical bundle usage where it is used as final/default.
- Snapshot history can preserve canonical diagnostics, but new final/displayed records should identify Model Projections as source.

## Frontend pages/components that display or consume win probabilities

### `frontend/src/pages/HomePage.jsx`

Current route/page:

- Main app route `/` labelled `Matchups` / `Daily Matchups`.
- Fetches `${API}/matchups?date=${date}`.
- Displays `m.away_win_prob` and `m.home_win_prob` as percentage values.
- Passes those fields to `ProbBar`.

Migration impact:

- This page will update automatically if backend legacy aliases are repointed to Model Projections.
- It should eventually prefer a normalized probability object if added, but backward-compatible alias repointing is enough for first migration.

### `frontend/src/pages/MatchupDetailPage.jsx`

Current route/page:

- App route `/matchup/:game_pk`.
- Fetches the matchup detail endpoint and displays top-level `away_win_prob` / `home_win_prob` in its probability section.

Migration impact:

- This is a required Sprint 1 target.
- It must match Model Projections page percentages for the same game/date once a Model Projection probability exists.

### `frontend/src/pages/ModelProjectionsPage.jsx`

Current route/page:

- App route `/models/projections`.
- Fetches `/models/projections?date=...`.
- `getSharedDerivedSimulation(game)` returns `sharedSimulation.derived_outputs.bullpen_adjusted_game_simulation || sharedSimulation.derived_outputs.game_simulation || {}`.
- Visible overview win cards use `sharedSim.away_win_probability` and `sharedSim.home_win_probability`.
- Team projection panel also uses `sim.away_win_probability` / `sim.home_win_probability`, falling back to model inputs.

Migration impact:

- This page is the basis for choosing the authoritative Model Projections probability field.
- Backend aliases should be updated to match this page's visible percentages.

### `frontend/src/pages/DailyOddsPageFixed.jsx`

Current route/page:

- Uses `/daily-odds` style data and joins model/matchup/event objects.
- `RecapCard` derives `awayWin` from `first([sim(model), m, model?.models?.moneyline], ['away_win_probability', 'away_win_prob', 'model_probability'])` and `homeWin` from `first([sim(model), m], ['home_win_probability', 'home_win_prob'])`.
- This can currently prefer simulation/model if present but falls back to matchups canonical fields.

Migration impact:

- Should consume the normalized Model Projection probability contract or backend-repointed aliases.
- Must not surface stale canonical values when Model Projection output exists.

### `frontend/src/pages/YesterdayTodayPage.jsx`

Current route/page:

- App route `/calendar`.
- Fetches `/matchups/calendar`.
- Displays date, count, and first eight game links.
- Does not currently display win probabilities in the inspected portion.

Migration impact:

- Performance-critical for calendar decoupling.
- Probability migration only applies if future calendar cards show probabilities.

### Other frontend search hits

Additional files with probability/search hits that should be reviewed during Sprint 1/5:

- `frontend/src/lib/landing/selectBetOfTheDay.mjs`
- `frontend/src/pages/LandingV2Page.jsx`
- `frontend/src/pages/DailyOddsPage.jsx`
- `frontend/src/pages/NewsPage.jsx`
- `frontend/src/starterOverviewDomPatch.js`
- `frontend/src/pages/ModelTrackerPage.jsx` if it renders model tracker probability columns from API output

## Model/projection functions that compute or alias probabilities

### Canonical probability path

Files:

- `mlb_app/canonical_matchup_probability.py`
- `mlb_app/matchup_generator.py`

Functions/objects:

- `compute_canonical_matchup_probability(...)`
- `_apply_canonical_probability(...)`
- `_add_missing_pitcher_diagnostics(...)`
- `generate_matchups_for_date(...)`
- `_generate_matchups_for_date_uncached(...)`

Current role:

- Produces `home_win_prob`, `away_win_prob`, `model_version`, diagnostics, and probability components.
- Currently considered final/default for matchups and model projection aliases.

Sprint migration role:

- Should become fallback/diagnostic/input unless explicitly needed for old compatibility.
- Should not remain displayed/default when Model Projections output exists.

### Legacy scoring path on matchup detail

Files:

- `mlb_app/app.py`
- `mlb_app/scoring.py`

Functions:

- `compute_win_probability(...)`
- `get_matchup_detail(...)`

Current role:

- `GET /matchup/{game_pk}` top-level `home_win_prob` / `away_win_prob` come from `compute_win_probability(...)`.

Sprint migration role:

- Should become fallback only for Matchup Detail if Model Projection probability is unavailable.

### Model Projections path

Files:

- `mlb_app/model_projection_routes.py`
- `mlb_app/model_projections.py`
- `mlb_app/simulation/game_simulation_builder.py`
- `mlb_app/simulation/game_simulator.py`

Functions:

- `model_projections(...)`
- `build_model_projection_payload(...)`
- `_build_projection_simulation_cards(...)`
- `_canonical_probability_payload(...)`
- `build_shared_game_simulation(...)`
- `simulate_game_with_bullpen(...)`

Current role:

- Builds `/models/projections` output.
- Currently returns top-level canonical aliases while storing shared simulation-derived probabilities under `sharedSimulation.derived_outputs` and workspace diagnostics.

Sprint migration role:

- Should produce the new normalized Model Projection probability object.
- Top-level aliases should match the selected Model Projection derived probability when available.

### Daily Odds probability aliasing

Files:

- `mlb_app/daily_odds_routes.py`
- `mlb_app/daily_odds_models.py`

Functions:

- `_fallback_candidates_from_matchups(...)`
- `_models_from_unpriced_matchups(...)`
- `_canonical_probability_summary(...)`
- `_summarize_projection_payload(...)`
- `_build_daily_recap(...)`
- `build_game_models(...)` in `daily_odds_models.py` should be reviewed for probability consumption.

Current role:

- Uses matchup `home_win_prob` / `away_win_prob` to build moneyline candidates and daily recap probability summaries.

Sprint migration role:

- Should use Model Projection probability resolver when projection output is available.

### Model Tracker probability aliasing

Files:

- `mlb_app/model_tracker.py`
- `mlb_app/model_tracker_routes.py`

Functions:

- `_canonical_probability_bundle(...)`
- `_canonical_features(...)`
- `_canonical_reasoning(...)`
- `_team_pick_from_canonical(...)`
- `normalize_matchup_rows(...)`
- `normalize_daily_odds_rows(...)`
- `normalize_model_projection_rows(...)`

Current role:

- Stores and labels canonical probabilities as final probability snapshots.

Sprint migration role:

- Preserve historical canonical diagnostics, but new snapshot final/default rows should use Model Projection probability resolution when available.

## Routes involved in Matchups, Matchup Detail, Calendar, Daily Matchups, and Model Projections

### Backend

- `GET /matchups`
- `GET /matchups/calendar`
- `POST /matchups/snapshot/{date_str}`
- `GET /matchup/{game_pk}`
- `GET /matchup/{game_pk}/competitive`
- `GET /models/projections`
- Daily Odds / model routes in `mlb_app/daily_odds_routes.py`
- Model Tracker routes included by `model_projection_routes.py` via `model_tracker_router`
- `POST /ai/ask` because it returns matchup payload slices

### Frontend

- `/` -> `HomePage.jsx` / Daily Matchups
- `/matchup/:game_pk` -> `MatchupDetailPage.jsx`
- `/calendar` -> `YesterdayTodayPage.jsx`
- `/models/projections` -> `ModelProjectionsPage.jsx`
- `/daily-odds` -> `DailyOddsPage.jsx` or `DailyOddsPageFixed.jsx` depending current app import/config
- `/model-tracker` -> `ModelTrackerPage.jsx`

## Open questions for Phase 2/3 implementation

1. Whether `sharedSimulation.derived_outputs.bullpen_adjusted_game_simulation` is always present after `/models/projections` warms, or whether `game_simulation` fallback is common.
2. Whether Matchup Detail can cheaply locate date/game projection artifacts without cold-building the entire `/models/projections` payload.
3. Whether Model Tracker should store both canonical and Model Projection probability bundles side-by-side for a transition period.
4. Whether Daily Odds moneyline fallback should use Model Projection probability artifacts directly or continue to consume matchup aliases after those aliases are repointed.
5. Whether existing backend tests depend on `model_version == canonical_matchup_win_probability_v2` for top-level `/models/projections` fields.

## Phase 1 acceptance checklist

- [x] Search terms covered.
- [x] Backend routes that emit/propagate win probabilities mapped.
- [x] Frontend pages/components that display/consume win probabilities mapped.
- [x] Model/projection probability functions mapped.
- [x] Routes involved in Matchups, Matchup Detail, Calendar, Daily Matchups, and Model Projections mapped.
- [x] Authoritative Model Projections field selected for Sprint 1.
- [x] No runtime behavior changed.
