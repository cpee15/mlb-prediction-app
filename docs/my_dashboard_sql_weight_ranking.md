# My Dashboard SQL weight ranking

Issue: #1048

This slice removes the final current-date filtered-report exception that routed scoring-weight overrides through the legacy in-memory solver.

## Preserved formula

The SQL expression reproduces the existing `my_dashboard_solver.apply_weight_overrides` contract:

`adjusted = base_score + normalized_metric * (weight - 1.0) * 0.25`

Metric normalization uses the same EV, launch-angle, sample-size, total, score/edge/diff, walk-rate, allowed-contact, and generic clamp rules.

## Safety boundary

- User weights are normalized per request and never persisted.
- Basic and metric filters execute before weight adjustment, matching the current Python behavior.
- Weighted score is calculated before sorting, offset, and limit.
- Shared `my_dashboard_records` rows remain unchanged.
- Unsupported weight metrics are ignored with explicit warnings.
- Historical and unfiltered reports retain their existing compatibility paths.
