# Canonical Model Engine v2 Summary

## What changed in this branch

This branch adds the first safe implementation foundation for a shared canonical model layer without rewriting Model Projections or redesigning any cards.

### Added files

- `docs/model_formula_audit.md`
- `mlb_app/canonical_model_engine.py`
- `tests/test_canonical_model_engine.py`
- `docs/canonical_model_engine_v2_summary.md`

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

## What this branch does not do yet

This branch is a foundation layer. It does not yet fully wire the new canonical engine into:

- Daily Odds route outputs
- AI Data Assistant response rendering
- Model Tracker persistence fields
- Model Projections route or page behavior

That wiring should happen in the next implementation pass after this shared formula foundation is reviewed.

## How Daily Odds can use this next

Daily Odds should next consume:

- `american_to_implied_probability()`
- `calculate_expected_value()`
- `assign_confidence_tier()`
- `evaluate_usage_weighted_pitcher_vs_hitter()`

The highest-value next step is replacing local prop gating with canonical usage-weighted hitter diagnostics and shared confidence tiers.

## How AI Data Assistant can use this next

AI Data Assistant should next use the returned usage-weighted diagnostic object to answer:

- why a hitter matchup passed or failed
- whether majority arsenal exposure supports the edge
- which pitch types support the edge
- which pitch types hurt the edge
- whether low-usage pitch warnings blocked the recommendation
- whether pitch data quality weakened confidence

## How Model Tracker can use this next

Model Tracker should next snapshot these pick-time diagnostics inside existing JSON-safe fields:

- usage-weighted pitcher-vs-hitter score
- supported usage share
- pitch data quality flags
- low-usage pitch warnings
- final pitcher-vs-hitter recommendation status
- confidence tier
- expected value

## Tests added in this branch

- positive American odds implied probability
- negative American odds implied probability
- positive EV calculation
- `STRONG` confidence tier assignment
- low-usage pitch cannot drive a positive recommendation
- majority usage support can promote a hitter
- whiff/strikeout risk suppresses hitter recommendation
- missing pitch usage blocks recommendation
- low sample pitch data flags `MONITOR` or worse

## Known limitations

- No live route integration yet
- No tracker persistence wiring yet
- No backtest script yet
- No canonical game-level team/pitcher scoring object yet
- No explicit team recent-form windows yet in this module
- No explicit pitcher season-vs-recent-form decomposition yet in this module

## Next recommended improvements

1. Wire Daily Odds to the canonical utility functions.
2. Add team recent-form component utilities.
3. Add starting pitcher baseline vs recent-form utilities.
4. Expand tracker snapshot metadata.
5. Add backtest script for edge buckets, tier buckets, usage buckets, and pitch-data quality buckets.
6. Add AI Data Assistant explanation helpers that directly read the canonical usage-weighted output.

## Testing and verification

Expected test entry point:

```bash
pytest tests/test_canonical_model_engine.py
```

This branch has not claimed route-level completion. It establishes the shared modeling foundation needed for the stricter cross-surface implementation work in subsequent commits.