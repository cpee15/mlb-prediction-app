# Canonical Model Engine v2 Summary

## What changed in this branch

This branch now includes both:

1. the first safe canonical model foundation
2. the first Daily Odds wiring pass into that foundation

It still does **not** rewrite Model Projections or redesign any cards.

### Added files

- `docs/model_formula_audit.md`
- `mlb_app/canonical_model_engine.py`
- `tests/test_canonical_model_engine.py`
- `tests/test_daily_odds_models.py`
- `docs/canonical_model_engine_v2_summary.md`

### Updated files

- `mlb_app/daily_odds_models.py`

## What formulas now exist in the canonical foundation

### 1. American odds to implied probability

Function:
- `american_to_implied_probability()`

Purpose:
- Normalize sportsbook odds into implied probability for cross-surface edge calculations.

### 2. Expected value calculation

Function:
- `calculate_expected_value()`

Purpose:
- Calculate EV using model probability and American odds.

### 3. Confidence tier assignment

Function:
- `assign_confidence_tier()`

Current tiers:
- `NO_BET`
- `MONITOR`
- `LEAN`
- `STRONG`
- `LOCK`

Purpose:
- Provide a stricter shared tiering contract based on:
  - data quality
  - confidence score
  - probability edge
  - expected value
  - missing inputs

### 4. Usage-weighted pitcher-vs-hitter evaluation

Function:
- `evaluate_usage_weighted_pitcher_vs_hitter()`

Purpose:
- Prevent false-positive hitter recommendations driven by low-usage pitch success.
- Enforce majority arsenal support.
- Suppress hitter recommendations when usage-weighted whiff/strikeout risk overwhelms contact quality.
- Emit detailed diagnostic fields that can later flow into Daily Odds, AI Data Assistant, and Model Tracker.

## Daily Odds wiring added in this branch

`mlb_app/daily_odds_models.py` now uses the canonical foundation utilities for:

- implied probability conversion
- expected value calculation
- confidence tier assignment
- recommendation status
- data quality score
- additive batter-prop usage-weighted gating when matchup batter-vs-arsenal data is already present

### New Daily Odds output fields

Daily Odds models now emit additive fields such as:

- `expected_value`
- `data_quality_score`
- `confidence_tier`
- `recommendation_status`
- `rejection_reason`

These are additive and do not require a card redesign.

### Batter prop gate behavior

For batter props, Daily Odds now attempts a safe optional usage-weighted gate only when the matchup payload already contains usable batter-vs-arsenal diagnostics.

If that gate is present:

- positive majority-usage support can help the over case
- weak or `NO_BET`/`MONITOR` gate status suppresses over cases
- low-quality pitch data pushes the candidate toward monitor/no-bet behavior

If matchup batter-vs-arsenal diagnostics are missing, the model does not invent them. It records missing input instead.

## How pitcher-vs-hitter arsenal usage weighting works

The evaluator:

1. Normalizes the pitcher’s arsenal usage.
2. Builds expected exposure share by pitch type.
3. Scores each pitch type using hitter-side:
   - xwOBA
   - on-base percentage
   - hard-hit rate
   - barrel rate
   - whiff rate
   - strikeout rate
4. Produces usage-weighted aggregates for:
   - positive contact
   - whiff/strikeout risk
   - xwOBA/on-base quality
   - hard-hit quality
   - final usage-weighted net score
5. Determines whether the hitter is supported across a majority of projected exposure.

## How low-usage pitch false positives are blocked

The evaluator will not promote a hitter just because one small-usage pitch grades well.

Current blocking logic:

- Tracks `supported_usage_share` across only the pitch types that grade positively.
- Requires majority arsenal support to allow a strong positive recommendation.
- Emits `low_usage_pitch_warnings` when a supportive pitch is too small a share of projected usage.
- Returns `MONITOR` or `NO_BET` when the hitter’s positive edge does not cover enough expected exposure.

## How whiff/strikeout risk suppresses hitter recommendations

The evaluator calculates:

- `usage_weighted_positive_contact_score`
- `usage_weighted_whiff_strikeout_risk`

If the whiff/strikeout risk outweighs positive contact quality, the hitter is downgraded or blocked even if some hard-hit indicators are positive.

## What fields are returned by the usage-weighted evaluator

The current diagnostic contract includes:

- `pitcher_arsenal_usage`
- `expected_pitch_type_exposure`
- `hitter_metrics_by_pitch_type`
- `usage_weighted_positive_contact_score`
- `usage_weighted_whiff_strikeout_risk`
- `usage_weighted_xwOBA_or_on_base_score`
- `usage_weighted_hard_hit_score`
- `pitch_types_supporting_edge`
- `pitch_types_hurting_edge`
- `low_usage_pitch_warnings`
- `pitch_data_quality_flags`
- `majority_usage_supported`
- `supported_usage_share`
- `usage_weighted_pitcher_vs_hitter_score`
- `final_pitcher_vs_hitter_recommendation_status`

## What this branch still does not do yet

This branch still does not fully wire the canonical engine into:

- AI Data Assistant response rendering
- Model Tracker persistence field expansion
- Model Projections route or page behavior
- backtest script generation
- explicit team recent-form component utilities
- explicit pitcher season-vs-recent-form component utilities

## How Daily Odds uses the canonical layer now

Daily Odds now directly uses:

- `american_to_implied_probability()`
- `calculate_expected_value()`
- `assign_confidence_tier()`
- `evaluate_usage_weighted_pitcher_vs_hitter()` when matchup data supports it

This means Daily Odds can now surface richer diagnostics without changing the card layout.

## How AI Data Assistant can use this next

AI Data Assistant should next use the returned usage-weighted diagnostic object to answer:

- why a hitter matchup passed or failed
- whether majority arsenal exposure supports the edge
- which pitch types support the edge
- which pitch types hurt the edge
- whether low-usage pitch warnings blocked the recommendation
- whether pitch data quality weakened confidence
- why a Daily Odds candidate is `recommended`, `monitor`, or `no_bet`

## How Model Tracker can use this next

Model Tracker should next snapshot these pick-time diagnostics inside existing JSON-safe fields:

- usage-weighted pitcher-vs-hitter score
- supported usage share
- pitch data quality flags
- low-usage pitch warnings
- final pitcher-vs-hitter recommendation status
- confidence tier
- expected value
- data quality score
- rejection reason

## Tests added in this branch

### Canonical engine tests

- positive American odds implied probability
- negative American odds implied probability
- positive EV calculation
- `STRONG` confidence tier assignment
- low-usage pitch cannot drive a positive recommendation
- majority usage support can promote a hitter
- whiff/strikeout risk suppresses hitter recommendation
- missing pitch usage blocks recommendation
- low sample pitch data flags `MONITOR` or worse

### Daily Odds wiring tests

- moneyline model includes EV, confidence tier, recommendation status, and data quality score
- batter prop over can be suppressed by a usage-weighted `NO_BET`/`MONITOR` gate
- batter prop candidates emit EV, confidence tier, and data quality score

## Next recommended improvements

1. Expand Model Tracker snapshots with direct canonical confidence tier / rejection fields.
2. Add AI Data Assistant explanation helpers that directly read the canonical Daily Odds diagnostics.
3. Add team recent-form component utilities.
4. Add starting pitcher baseline vs recent-form component utilities.
5. Add backtest script for edge buckets, tier buckets, usage buckets, and pitch-data quality buckets.

## Testing and verification

Expected test entry points:

```bash
pytest tests/test_canonical_model_engine.py
pytest tests/test_daily_odds_models.py
```

This branch is now beyond scaffolding. It establishes the shared modeling foundation and wires Daily Odds to it in an additive, low-risk way while leaving Model Projections untouched.