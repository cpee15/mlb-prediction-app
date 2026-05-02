# Model Projection Formula Map

This document defines the modeling contract for the production Model Projections workspace. The goal is to make every displayed value traceable to one of four roles:

- **Direct Sim Input**: directly changes PA probabilities, starter/bullpen source selection, environment modifiers, or calibration.
- **Derived Sim Output**: produced by the simulation from direct inputs.
- **Context / Future Input**: useful for explanation or planned model improvements, but not a strong direct driver yet.
- **Diagnostic**: provenance, missing inputs, model versions, source tables, sample windows, and debug details.

The preferred end state is that `/models/projections` and `/matchup/:game_pk` use the same shared simulation builders, so the same game produces the same expected runs, win probabilities, and total probabilities unless intentionally configured with different model versions.

---

## 1. Source-of-Truth Contract

Every simulation payload should expose enough metadata to explain the result:

- `model_version`
- `source_builder`
- `simulation_count`
- `seed`
- `starter_exit_enabled`
- `starter_quality_score`
- `starter_quality_label`
- `calibration_version`
- `offense_source`
- `pitcher_source`
- `bullpen_source`
- `environment_source`

If `/models/projections` and `/matchup/:game_pk` differ, the UI should make the difference explicit rather than presenting both as the same model.

---

## 2. Plate Appearance Outcome Model

Each plate appearance resolves into one normalized outcome bucket:

- `K`
- `BB`
- `HBP`
- `1B`
- `2B`
- `3B`
- `HR`
- `ROE`
- `Contact Out`

Conceptually:

```text
PA probabilities = f(
  offense profile,
  opposing starter or bullpen profile,
  environment modifiers,
  calibration / shrinkage
)
```

### 2.1 Strikeout Probability

**Direct drivers**

- offense `k_rate` / `k_pct`
- opposing starter `k_rate` / `k_pct`
- opposing bullpen `k_rate`
- bullpen `whiff_rate` and `csw_rate` when wired
- future: pitch-level whiff rates and arsenal matchup

**Display in UI**

- Offense K%
- Starter K%
- Bullpen K%
- PA model K probability

### 2.2 Walk Probability

**Direct drivers**

- offense `bb_rate` / `bb_pct`
- opposing starter `bb_rate` / `bb_pct`
- opposing bullpen `bb_rate`
- bullpen `zone_rate` and `first_pitch_strike_rate` when wired

### 2.3 Hit Probability

**Direct drivers**

- offense batting average or hit rate
- starter `xba_allowed`, if verified as pitcher-allowed
- bullpen `xba_allowed`
- starter/bullpen contact quality allowed
- environment `hit_boost_index`

### 2.4 Extra-Base Hit and Home Run Probability

**Direct drivers**

- offense `iso`
- offense `slugging_pct`
- starter `hard_hit_rate_allowed`
- starter `xwoba_allowed`
- bullpen `hard_hit_rate_allowed`
- bullpen `barrel_rate_allowed`
- bullpen `xwoba_allowed`
- environment `hr_boost_index`
- environment `run_scoring_index`

---

## 3. Starter Exit Model

Driven by:

- `starter_quality_score`

Effect:

- determines innings distribution
- starter → bullpen transition

---

## 4. Bullpen Model

Direct drivers:

- bullpen K rate
- bullpen BB rate
- bullpen hard-hit
- bullpen xwOBA

---

## 5. Environment Model

Direct drivers:

- run_scoring_index
- hr_boost_index
- hit_boost_index

---

## 6. Simulation Outputs

Derived outputs:

- expected runs
- win probability
- totals

---

## 7. UI Rules

Direct Input → show
Derived Output → show
Context → compact
Diagnostic → hide from main

---

## 8. Known Issues

- xBA may be misinterpreted
- run creation formatting broken
- pitch JSON not formatted
- sandbox vs main mismatch
