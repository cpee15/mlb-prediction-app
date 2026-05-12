# Roadmap to Edge Detection

## Executive Summary

The project has completed the first major empirical calibration cycle. The current model is now scientifically evaluatable, but it should not yet be treated as a complete edge-detection engine.

Layer 4 established a trustworthy calibration framework:

- actual-result backfill
- profile propagation validation
- exact scalar parser extraction
- RMSE verification
- full calibration run
- bootstrap stability analysis

The key conclusion is:

> The calibration framework is ready, but the model realism layers are not complete enough for edge detection.

Current readiness verdict:

```text
calibration_framework_ready_realism_layers_next_edge_detection_later
```

---

## Why Calibration Is Not the Same as Edge Readiness

A model can be useful for empirical calibration and still be incomplete for market edge detection.

Calibration validates whether the model's average predictions and candidate changes can be evaluated against actual outcomes.

Edge detection requires more than calibrated averages. It requires realistic distribution shape, tail behavior, context handling, and market comparison discipline.

Sportsbook markets often price:

- distribution tails
- derivative markets
- variance
- contextual asymmetries
- environment effects
- matchup-specific effects

Therefore, edge detection should come after realism and distribution-validation layers, not immediately after Layer 4.

---

## Completed Layers

## Layer 1 — Baseline App/Data Foundation

### Core Question

Can the system run and produce baseline matchup/simulation outputs?

### Purpose

Layer 1 established the base app and data flow.

It covered:

- core matchup generation
- baseline team/player inputs
- initial simulation outputs
- basic data refresh assumptions
- initial simulation architecture

### Layer Type

```text
infrastructure_foundation
```

---

## Layer 2 — Profile Construction and Source Quality

### Core Question

Are player and team profiles being built from reliable inputs?

### Purpose

Layer 2 audited the source inputs and profile construction process.

It covered:

- hitter profile coverage
- pitcher true-talent construction
- bullpen expected-stat/profile work
- offensive expected-stat profiles
- source input debugging
- deduplication and coverage audits

### Layer Type

```text
input_realism_and_profile_quality
```

---

## Layer 3 — Profile Weighting and Talent-Signal Philosophy

### Core Question

How should season, current, recent, expected, and stuff signals be weighted before simulation?

### Purpose

Layer 3 decided how to combine and interpret baseball talent signals.

It covered:

- profile season weighting simplification
- recomputation weighting audits
- season vs current vs recent weighting
- expected-contact and pitcher-stuff modifier concepts
- recent windows as bounded modifiers instead of primary replacements
- candidate profile pairings for simulation testing

### Layer Type

```text
talent_signal_weighting
```

---

## Layer 4 — Empirical Simulation Calibration

### Core Question

Do candidate profile systems improve real predictive calibration?

### Purpose

Layer 4 turned the simulator into an empirical research framework.

It covered:

- simulation injection seam discovery
- candidate propagation validation
- actual-result backfill
- output schema discovery
- exact scalar parser patch
- RMSE verification
- full 188-game calibration
- bootstrap stability analysis

### Key Result

```text
baseline_preferred
```

Layer 4 showed:

- the calibration framework works
- baseline remains the production default
- no candidate modifier is production-ready yet
- recent modifiers appear overfit/inflationary
- pitcher stuff is promising but inconclusive
- expected-contact-only appears inactive or ineffective

### Layer Type

```text
empirical_calibration_and_validation
```

---

# Corrected Roadmap Before Edge Detection

The remaining roadmap should not place all realism work under one layer. Environment, game mechanics, pitch arsenal logic, distribution validation, and market comparison are separate modeling domains.

---

## Layer 5 — Player/Profile Signal Refinement

### Core Question

Are the talent signals feeding the simulator strong, active, and correctly scaled?

### Focus Areas

- expected-contact propagation and magnitude investigation
- pitcher-stuff modifier tuning
- bullpen profile refinement
- larger profile-window tests
- candidate modifier recalibration
- signal activation diagnostics

### Why This Layer Exists

Layer 4 showed that expected-contact-only produced exact zero paired deltas and that pitcher-stuff signals may help win-probability calibration but are not yet stable enough.

Layer 5 should refine the talent signals before expanding into other realism domains.

### Exit Criteria

Layer 5 is complete when:

- active signal paths are confirmed
- inactive modifiers are explained or removed
- modifier magnitudes are calibrated
- candidate signal changes can be evaluated through the Layer 4 calibration framework
- no candidate is promoted without stability evidence

### Layer Type

```text
player_talent_signal_refinement
```

---

## Layer 6 — Game-State Realism Engine

### Core Question

Does the simulator represent baseball mechanics well enough to model scoring distributions?

### Focus Areas

- extra innings and ghost runner logic
- stolen bases and caught stealing
- wild pitches and passed balls
- balks
- first-to-third advancement
- second-to-home advancement
- sac flies and tagging up
- double plays by base/out state
- pinch hitters and substitutions
- bullpen sequencing and leverage behavior

### Why This Layer Exists

These events do not just change mean runs. They change distribution shape, scoring tails, inning extension probabilities, and derivative markets such as team totals and alternate totals.

### Exit Criteria

Layer 6 is complete when:

- base/out transitions are more realistic
- scoring distribution tails improve
- inning-level run distribution improves
- extra-inning behavior is represented correctly
- team-total and total-run variance improve against actual outcomes

### Layer Type

```text
game_mechanics_realism
```

---

## Layer 7 — Environment Realism Engine

### Core Question

Does the model represent venue, weather, and park physics well enough for run-environment realism?

### Focus Areas

- venue-specific wind geometry
- wind direction relative to field orientation
- roof state and dome state
- temperature
- humidity
- air density
- park-specific carry effects
- wall and field geometry
- weather-to-batted-ball interaction

### Why This Layer Exists

Environment is not just a generic adjustment. Baseball environment is geometric and nonlinear.

Wind at Wrigley Field is not the same as wind at Yankee Stadium. Roof-open and roof-closed states can materially change carry and run environment. Pull-side fly balls and opposite-field fly balls may respond differently to wind orientation.

Environment deserves its own major layer because it is a physics and venue-modeling domain, not a player-profile domain.

### Exit Criteria

Layer 7 is complete when:

- park/weather effects are venue-specific
- roof/dome state is accounted for where available
- wind direction is interpreted relative to field geometry
- run-environment bias improves by park/weather bucket
- HR, XBH, and total-run calibration improve in environment-sensitive games

### Layer Type

```text
environment_and_park_physics
```

---

## Layer 8 — Pitch Arsenal and Matchup Engine

### Core Question

Does the model capture pitcher-hitter 1v1 matchup dynamics?

### Focus Areas

- pitch mix by pitcher handedness and batter handedness
- pitch mix by count
- pitch-specific quality metrics
- hitter pitch-type strengths and weaknesses
- arsenal interaction with handedness and park
- sequencing and count leverage
- matchup preview explainability

### Why This Layer Exists

The app's long-term conceptual edge is baseball as a matchup sport. Pitcher arsenal vs hitter profile is the core 1v1 interaction.

A model can be calibrated at an aggregate level while still failing to explain why a specific pitcher-hitter matchup is favorable or unfavorable.

Layer 8 should improve both PA outcome realism and the app's matchup-preview usefulness.

### Exit Criteria

Layer 8 is complete when:

- pitch arsenal signals propagate into PA outcomes
- hitter pitch-type strengths/weaknesses affect projections
- handedness and count context modify pitch mix meaningfully
- matchup explanations become specific and credible
- hitter/pitcher-specific calibration improves

### Layer Type

```text
pitch_arsenal_matchup_realism
```

---

## Layer 9 — Distribution-Shape Validation and Pricing Readiness

### Core Question

Are the model's distributions realistic enough for pricing derivatives?

### Focus Areas

- run distribution tails
- variance calibration
- alternate totals
- team totals
- favorite/underdog buckets
- high-total vs low-total environments
- inning-level scoring clusters
- calibration by regime

### Why This Layer Exists

Mean calibration is not enough for pricing. Markets often expose errors in the tails and derivative surfaces.

Before edge detection, the model must show that its probability distributions are realistic, not just its expected runs.

### Exit Criteria

Layer 9 is complete when:

- run distribution tails are plausible
- total/team-total variance is calibrated
- derivative probability ladders are credible
- calibration holds across scoring regimes
- model uncertainty is quantified clearly

### Layer Type

```text
distribution_validation_and_pricing_readiness
```

---

## Layer 10 — Market Comparison and Edge Detection

### Core Question

Does the model identify persistent disagreement with sportsbook markets?

### Focus Areas

- odds ingestion
- no-vig implied probabilities
- market-vs-model deltas
- closing-line comparison
- edge persistence tracking
- threshold design
- paper-trading evaluation
- bankroll-safe evaluation metrics

### Why This Layer Exists

Edge detection should be the final step, not the next step.

The model should only be compared to markets after talent signals, game-state realism, environment realism, matchup realism, and distribution realism have been improved and validated.

### Exit Criteria

Layer 10 is complete when:

- market data is ingested reliably
- implied probabilities are normalized correctly
- model-vs-market deltas are measured consistently
- closing-line value is tracked
- apparent edges persist out-of-sample
- edge thresholds are conservative and evidence-based

### Layer Type

```text
market_edge_detection
```

---

# Current Readiness Verdict

```text
calibration_framework_ready_realism_layers_next_edge_detection_later
```

Meaning:

- the framework is ready to evaluate model changes
- the model is not yet ready to be trusted as an edge detector
- edge detection should wait until realism and distribution layers are stronger

---

# Production Discipline

No future layer should trigger production integration unless the candidate change satisfies all of these gates:

- improves over baseline on a full sample
- bootstrap confidence interval excludes 0 on primary metrics
- does not inflate run bias materially
- remains stable across date splits
- remains stable across regime buckets
- has no parser fallback issues
- has no metric integrity issues
- improves or preserves distribution realism

---

# Strategic Principle

The project should continue to prioritize evidence over hypothesis.

Layer 4 was successful partly because it rejected weak edges. That discipline should continue through every realism layer before any market-facing edge detection is attempted.
