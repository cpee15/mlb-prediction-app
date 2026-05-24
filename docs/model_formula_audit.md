# Model Formula Audit

## Scope of this audit

This audit documents the current formula and route surfaces most relevant to the canonical model engine work. The purpose is to identify where probabilities, edges, confidence, pitcher-vs-hitter logic, and tracker metadata are currently produced so the app can move toward one explainable model contract without breaking working production routes.

This pass intentionally treats Model Projections as a reference surface, not the primary implementation target.

## Audited files

### `mlb_app/scoring.py`

Current role:
- Core matchup scoring engine for win probability and individual pitcher-vs-batter scoring.

Current formulas:
- Pitcher aggregate weighted score using:
  - `k_pct`
  - `bb_pct`
  - `hard_hit_pct`
  - `xwoba`
  - `avg_velocity`
- Batter aggregate weighted score using:
  - `avg_exit_velocity`
  - `hard_hit_pct`
  - `barrel_pct`
  - `k_pct`
  - `bb_pct`
  - `batting_avg`
- Arsenal score using pitch-level:
  - `usage_pct`
  - `whiff_pct`
  - `strikeout_pct`
  - `rv_per_100`
  - `xwoba`
- `compute_win_probability()` converts net score into `home_win_prob` and `away_win_prob` using a logistic transform.
- `score_individual_matchup()` returns:
  - `pitcher_advantage`
  - `batter_advantage`
  - `net_score`
  - `pitcher_win_prob`

Current strengths:
- Already uses pitch usage weighting in the arsenal contribution.
- Already exposes app-facing `home_win_prob` and `away_win_prob`.

Current gaps:
- No explicit separation of season baseline vs recent form.
- No explicit bullpen component.
- No explicit team recent-form windows.
- No explicit data quality score.
- No explicit expected value calculation.
- No confidence tier taxonomy.
- Pitcher-vs-hitter logic is not strict enough about majority arsenal exposure.
- No hard suppression for hitters with poor usage-weighted whiff/strikeout profile.
- No low-usage pitch warning contract.
- No pitch-data quality flag contract.

### `mlb_app/daily_odds_routes.py`

Current role:
- Builds Daily Odds payloads from matchups plus sportsbook events.
- Generates fallback model candidates when sportsbook events are missing.

Current logic:
- Imports `build_game_models()` and `build_prop_models()` from `daily_odds_models.py`.
- Builds fallback moneyline candidates from `home_win_prob` and `away_win_prob`.
- Builds fallback pitcher watchlist candidates from pitcher `k_pct`, `xwoba`, and `hard_hit_pct`.

Current strengths:
- Already reads canonical-looking matchup probability fields.
- Already includes diagnostics like `features_used`, `missing_inputs`, and `drivers`.

Current gaps:
- Still has route-local fallback scoring behavior.
- No single canonical recommendation gate shared with other surfaces.
- No canonical data quality score or confidence tier.
- No canonical expected value or rejection reason contract.

### `mlb_app/daily_odds_models.py`

Current role:
- Converts matchup and sportsbook data into Daily Odds model objects.

Current formulas:
- American odds to implied probability conversion.
- Moneyline model compares canonical matchup probability against sportsbook implied probability.
- Spread model uses projected runs or probability differential proxy.
- Total model uses weather, wind, and offense strength to create total lean.
- Confidence currently depends mostly on number of features present.

Current strengths:
- Moneyline model already references canonical matchup probability fields.
- Already computes edge as model probability minus market implied probability.
- Already stores `features_used`, `missing_inputs`, and `drivers`.

Current gaps:
- Confidence is still shallow and feature-count based.
- No canonical EV framework used across all model families.
- No strict recommendation gate shared with tracker or assistant.
- No pitcher-vs-hitter usage-weighted gate contract.

### `mlb_app/ai_data_assistant_routes.py`

Current role:
- Serves the AI Data Assistant page and query endpoint.

Current logic:
- Calls `build_ai_data_assistant_response()`.
- Accepts message/date/game/player/team context.

Current strengths:
- Already set up to answer against app-owned data.
- Already has deterministic mode by default.

Current gaps:
- The route itself does not define canonical formula access. That work happens deeper in the assistant services.

### `mlb_app/ai_data_assistant_performance.py`

Current role:
- Performance wrapper around AI Data Assistant.
- Adds process-local caching and canonical probability context.

Current logic:
- Treats `home_win_prob` and `away_win_prob` as canonical v2 side probabilities.
- Explicitly marks simulation output as diagnostic only, not final side probability.
- Enriches assistant responses with canonical probability context.

Current strengths:
- Strong starting point for using canonical probability contract in the assistant.
- Already identifies `canonical_matchup_win_probability_v2` and legacy comparison fields.

Current gaps:
- No unified canonical formula object for pitcher model, team recent form, market edge, or usage-weighted hitter logic.
- Assistant can explain probability context, but not yet a full canonical model breakdown.

### `mlb_app/model_projection_routes.py`

Current role:
- Serves `/models/projections`.
- Delegates to `build_model_projection_payload()`.

Current guidance for this initiative:
- Treat as a read-only/reference surface in this pass.
- Do not redesign or rewrite projections workflow here.

### `mlb_app/model_projections.py`

Current role:
- Builds Model Projections payload using matchups, bullpen profile, environment profile, team offense prior, PA outcome probabilities, and simulation tools.

Current strengths:
- Rich projection context already exists.
- Contains several model cards and simulation helpers.
- Uses offense prior, bullpen profile, environment profile, and pitch arsenal inputs.

Current gaps relative to canonical model initiative:
- Not the right place to centralize cross-surface formulas yet.
- Too broad/risky to rewrite in the first pass.
- Should consume canonical model outputs later rather than be rebuilt now.

### `mlb_app/model_tracker_routes.py`

Current role:
- Tracker and dashboard route layer.
- Mostly manages saved workspace/dashboard structures.

Current strengths:
- Existing route surface already exists for tracker features.

Current gaps:
- Route file is not the core formula engine.
- Tracker diagnostics need to be expanded at the snapshot/model layer, not only at the route layer.

### `mlb_app/model_tracker.py`

Current role:
- Persistent snapshot layer for matchups, daily odds, model projections, and dashboard-style model outputs.

Current strengths:
- Already stores:
  - `model_probability`
  - `market_implied_probability`
  - `edge`
  - `score`
  - `confidence`
  - `expected_value`
  - projected totals/runs
  - home/away win probabilities
  - reasoning/features/missing inputs/raw payload JSON
- Already has canonical v2 and legacy model version fields.
- Already has additive JSON-safe snapshot strategy.

Current gaps:
- Does not yet store the richer canonical model metadata requested in Issue #432.
- Missing explicit pick-time storage for:
  - confidence tier
  - data quality score
  - pitcher recent-form components
  - team recent-form components
  - usage-weighted pitcher-vs-hitter diagnostics
  - pitch data quality flags
  - rejection reasons and gating details

## Current formula truth by surface

### Final side probability
Current strongest app-facing source:
- `home_win_prob`
- `away_win_prob`

Primary current producers/consumers:
- `mlb_app/scoring.py`
- matchup payloads
- `mlb_app/daily_odds_models.py`
- `mlb_app/ai_data_assistant_performance.py`
- `mlb_app/model_tracker.py`

### Daily Odds edge
Current source:
- market implied probability from American odds
- compared against matchup-derived canonical probability in Daily Odds models

### Confidence
Current source:
- mostly feature-count based in Daily Odds models
- probability/score proxies in some fallback candidates
- no globally consistent confidence tier contract

### Pitcher-vs-hitter logic
Current source:
- `mlb_app/scoring.py`
- current scoring uses arsenal `usage_pct`, but not strict majority exposure gates

## Main duplicated or conflicting logic risks

1. Probability, confidence, and score are still partially generated per surface instead of from one canonical model engine.
2. Daily Odds has fallback candidate logic that can drift from shared scoring.
3. Tracker stores probabilities and diagnostics but not yet the richer metadata needed to diagnose loss causes.
4. AI Data Assistant knows canonical side probability context but not yet a unified model breakdown.

## Recommended cleanup path

### Phase 1
- Add a pure, shared canonical model engine module.
- Define stable utility formulas for:
  - implied probability
  - expected value
  - confidence tiers
  - usage-weighted pitcher-vs-hitter evaluation
- Keep this phase additive and low-risk.

### Phase 2
- Point Daily Odds and AI Data Assistant diagnostics at the canonical model engine.
- Expand tracker snapshot metadata with canonical breakdown fields.

### Phase 3
- Let Model Projections consume canonical model fields later, without rebuilding the page in the first pass.

## First implementation target

The safest immediate implementation is:

1. `docs/model_formula_audit.md`
2. `mlb_app/modeling/canonical_model_engine.py`
3. Tests for:
   - American odds implied probability
   - EV calculation
   - usage-weighted pitcher-vs-hitter gates
   - low-usage pitch false-positive suppression
   - majority arsenal exposure promotion logic
   - whiff/strikeout suppression

That creates a real shared modeling foundation without breaking production routes.