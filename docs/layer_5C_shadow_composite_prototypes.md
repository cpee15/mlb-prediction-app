# Layer 5C-J — Shadow Pitcher Composite Prototype Design

## Status

Diagnosis:

shadow_composite_designs_ready_for_research_only_phase

This layer defines research-only shadow composite concepts that prepare for future arsenal realism integration without changing production simulation behavior.

IMPORTANT:

This layer does NOT:
- activate arsenal realism
- modify _build_pa_model
- mutate probabilities
- change production calibration behavior
- alter runtime simulation outputs

This layer ONLY:
- defines prototype research composites
- documents realism insertion seams
- defines double-count protections
- establishes future calibration requirements

---

# Current Architecture Recap

The simulator already consumes active aggregate pitcher composites:

- pit_k
- pit_bb
- pit_xba
- pit_xwoba
- pit_hard_hit
- pit_hr

Detailed arsenal fields remain dormant:

- whiff_pct
- usage_pct
- rv_per_100
- arsenal
- pitch_mix

The realism boundary discovered in Layers 5C-H and 5C-I showed that:
- aggregate realism is active
- detailed arsenal realism is dormant
- the correct insertion seam is the composite layer

NOT:
- downstream probability mutation
- post-normalization overrides

---

# Shadow Composite Philosophy

Shadow composites exist to safely prototype future realism systems.

They are:
- research-only abstractions
- non-production structures
- calibration-safe prototypes

Their purpose is to:
- test realism ideas safely
- isolate arsenal concepts
- avoid destabilizing the calibrated probability engine

Shadow composites should remain upstream of probabilities.

Future realism should follow:

arsenal realism
    ↓
shadow composite layer
    ↓
aggregate composite layer
    ↓
stable probability engine

NOT:

arsenal realism
    ↓
late-stage probability mutation

---

# Prototype Shadow Composites

## 1. pit_k_shadow

Purpose:
- prototype arsenal-adjusted strikeout realism

Prototype concept:

pit_k_shadow =
    shrinkage(
        aggregate_k = pit_k,
        arsenal_whiff_signal = whiff_component,
    )

Candidate dormant inputs:
- whiff_pct
- usage_pct
- pitch-level K expectation

Risk:
VERY HIGH double-count risk.

Reason:
pit_k already partially encodes latent whiff ability.

Safe future direction:
- shrinkage only
- residual blending only
- bounded influence only

---

## 2. pit_xwoba_shadow

Purpose:
- prototype arsenal-adjusted contact quality

Prototype concept:

pit_xwoba_shadow =
    residual_blend(
        baseline_xwoba = pit_xwoba,
        arsenal_contact_signal = rv_contact_component,
    )

Candidate dormant inputs:
- rv_per_100
- pitch-type contact suppression
- pitch-mix interaction quality

Risk:
HIGH double-count risk.

Reason:
xwOBA already captures significant contact quality information.

---

## 3. pit_hard_hit_shadow

Purpose:
- prototype arsenal-adjusted hard-contact suppression

Prototype concept:

pit_hard_hit_shadow =
    bounded_blend(
        baseline_hard_hit = pit_hard_hit,
        arsenal_contact_shape = pitch_mix_contact_component,
    )

Candidate dormant inputs:
- pitch_mix
- contact-shape metrics
- pitch-shape realism

Risk:
MODERATE.

---

## 4. pit_hr_shadow

Purpose:
- prototype arsenal-adjusted HR suppression

Prototype concept:

pit_hr_shadow =
    bounded_blend(
        baseline_hr = pit_hr,
        arsenal_damage_component = barrel_suppression_signal,
    )

Candidate dormant inputs:
- barrel suppression
- fly-ball damage shaping
- pitch-type HR suppression

Risk:
MODERATE.

---

# Double-Count Prevention Strategy

This is the most important realism safety problem.

Aggregate pitcher stats already contain latent arsenal information.

Examples:
- strikeout rate already contains bat-missing ability
- xwOBA already reflects contact quality
- HR suppression already reflects pitch quality

Therefore:
naive additive stacking would:
- inflate variance
- destabilize calibration
- create fake edges
- overfit historical noise

Future realism blending should use:
- shrinkage
- residualization
- bounded adjustments
- capped influence

NOT:
- additive stacking
- direct probability overrides

---

# Future Research Path

## Phase 1 — Shadow Mode

Goal:
- construct research-only shadow composites
- no production influence

## Phase 2 — Calibration Harness

Goal:
- compare baseline vs shadow composites
- perform sensitivity testing
- run bootstrap analysis

## Phase 3 — Controlled Activation Research

Goal:
- research-flag-only activation
- no production default
- no market-facing integration

## Phase 4 — Future Layer 8 Integration

Goal:
- eventual arsenal matchup realism
- pitch-level interaction modeling
- matchup explainability

Only after:
- calibration stability
- regime stability
- overfitting protection
- rollback validation

---

# Final Recommendation

The simulator should evolve toward:

detailed arsenal realism
    ↓
shadow composite layer
    ↓
aggregate composite layer
    ↓
stable probability engine

This preserves:
- calibration integrity
- realism modularity
- future matchup realism flexibility
- long-term edge-detection discipline
