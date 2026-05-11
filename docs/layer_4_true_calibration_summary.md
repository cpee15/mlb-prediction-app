
# Layer 4 — True Calibration Framework Summary

## Executive Summary

Layer 4 successfully transformed the MLB simulation project from an exploratory simulator into a statistically grounded calibration and research framework.

The most important conclusion from Layer 4 is:

> Baseline production behavior remains the correct default.

No candidate modifier configuration demonstrated a sufficiently stable statistical edge to justify production integration at this time.

## Completed Layer 4 Milestones

- actual-result backfill infrastructure
- modifier propagation validation
- exact scalar parser extraction
- verified RMSE metric calculations
- full true-calibration evaluation
- bootstrap stability/confidence analysis

## Full Calibration Leaderboard

### Combined Score Ranking

| Rank | Config | Combined Score |
|---|---|---|
| 1 | pitcher_stuff__small | 1.802559 |
| 1 | combined_expected_contact_and_stuff__small | 1.802559 |
| 3 | baseline_current_production | 1.806968 |
| 3 | hitter_expected_contact__small | 1.806968 |
| 5 | hitter_recent__medium | 1.811126 |
| 6 | combined_recent_and_stuff__small | 1.811470 |

### Run MAE Ranking

| Rank | Config | Total Run MAE |
|---|---|---|
| 1 | baseline_current_production | 3.712158 |
| 1 | hitter_expected_contact__small | 3.712158 |
| 3 | pitcher_stuff__small | 3.738648 |
| 3 | combined_expected_contact_and_stuff__small | 3.738648 |
| 5 | combined_recent_and_stuff__small | 3.785031 |
| 6 | hitter_recent__medium | 3.790793 |

## Stability / Confidence Analysis

Final stability verdict:

baseline_preferred

No candidate demonstrated a statistically convincing improvement over baseline after bootstrap confidence analysis.

## Candidate Conclusions

### Pitcher Stuff Modifiers

Promising but inconclusive.

pitcher_stuff__small metrics vs baseline:

- total_error delta: +0.028232
- brier delta: -0.002364
- log_loss delta: -0.004699

Confidence intervals overlapped 0.

### Recent Modifiers

Recent modifiers produced inflated total-run bias and consistently worse total-error calibration.

Verdict:
recent_modifier_overfit_or_bias

### Expected Contact Modifiers

hitter_expected_contact__small produced exact zero paired deltas vs baseline.

Likely causes:
- inactive propagation
- insufficient modifier magnitude
- scalar-output disconnect

## Production Decision

Keep baseline as default.

Do not integrate candidate modifiers into production yet.

## Final Layer 4 Conclusion

Layer 4 successfully established a trustworthy empirical calibration framework.

The framework rejected weak or unstable edges instead of forcing production integration.

The project now has a durable scientific foundation for future baseball simulation research.
