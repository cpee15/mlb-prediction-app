# Layer 5C-M — Arsenal Schema Normalization Design

## Diagnosis

arsenal_schema_normalization_design_ready

## Purpose

This layer defines canonical normalization rules for real generated arsenal payloads before any research-only shadow composite calculations.

This layer does not activate realism, modify production simulation, mutate _build_pa_model, change probabilities, or integrate shadow composites.

## Confirmed Real Payload Fields

Validated in Layer 5C-LA:

- whiff_pct: present/numeric
- usage_pct: present/numeric
- rv_per_100: present/numeric
- strikeout_pct: present/numeric
- xwoba: present/numeric
- xba: present/numeric
- hard_hit_pct: fragmented
- hr_rate: not canonical
- pitch_mix: not canonical

## Canonical Arsenal Schema

Core canonical fields:

- canonical_pitch_type
- canonical_usage_pct
- canonical_whiff_pct
- canonical_strikeout_pct
- canonical_rv_per_100
- canonical_xwoba
- canonical_xba
- canonical_hard_hit_pct
- canonical_pitch_mix_weight
- canonical_damage_proxy

## Canonical Pitch Type Mapping

Normalize pitch labels:

- FF -> four_seam
- FA -> four_seam
- SI -> sinker
- FT -> sinker
- SL -> slider
- CU -> curveball
- CH -> changeup
- KC -> knuckle_curve
- FC -> cutter

Unknown labels should preserve raw label and flag a validation warning.

## Source Mapping Rules

Accepted pitcher arsenal sources:

- home_pitch_arsenal.<pitch>.<metric>
- away_pitch_arsenal.<pitch>.<metric>
- home_pitcher_features.<metric>
- away_pitcher_features.<metric>

Explicitly exclude hitter/offense paths from pitcher arsenal normalization.

## Scale Normalization

Percentage-like fields may arrive in probability scale or percentage scale.

Rules:

- values between 0 and 1 are treated as already normalized
- values greater than 1 and less than or equal to 100 are divided by 100
- values less than 0 or greater than 100 are invalid
- ambiguous scales should be flagged

## Derived Canonical Fields

canonical_pitch_mix_weight should derive from normalized usage_pct because exact pitch_mix is not canonical in real payloads.

canonical_damage_proxy should derive from rv_per_100, xwoba, hard_hit_pct, and/or xba until a true canonical hr_rate source exists.

## Role Normalization

Supported roles:

- home starter
- away starter
- aggregate pitcher features

Deferred roles:

- bullpen
- opener chains
- reliever sequences

## Validation Rules

pit_k_shadow requires:

- whiff_pct
- usage_pct
- strikeout_pct

pit_xwoba_shadow requires:

- xwoba
- rv_per_100

pit_hard_hit_shadow requires:

- hard_hit_pct or xwoba proxy

pit_hr_shadow requires:

- damage proxy or future canonical hr_rate

Minimum standards:

- numeric_rate >= 0.70
- null_rate <= 0.30
- schema fragmentation monitored
- scale ambiguity warning
- invalid ranges fail

## Future Layer Dependency

If accepted, Layer 5C-N may implement research-only canonical extractors.

Still prohibited:

- realism activation
- production simulation mutation
- shadow composite integration
- edge detection

## Final Conclusion

The realism architecture is structurally viable, payload-validated, and now normalization-constrained.
