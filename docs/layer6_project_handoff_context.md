# MLB Prediction App — Project Handoff and Layer 6 Context

## Purpose of this document

This document is a handoff guide for continuing `msantoria/mlb-prediction-app` without losing context.

It explains:

1. What the app is trying to become.
2. How the current work fits the larger vision.
3. Why the layer system exists.
4. What has already been proven.
5. What is being worked on right now.
6. How to continue without guessing.
7. The exact process expected for future work.

The goal is a seamless transition so another person or assistant can continue pushing the app forward using the same discipline and standards.

---

# 1. Product vision

The app is being developed toward a strong MLB simulation and projection system.

The vision is not to jump straight into gambling edge detection.

The correct order is:

```text
1. Build a credible simulation/projection engine.
2. Validate that the simulation engine produces coherent win-probability and run-projection surfaces.
3. Validate that game-mechanics realism actually propagates into those surfaces.
4. Only after that, compare model output to market prices.
5. Only after that, discuss edge detection.

The current project philosophy is:

strong simulation model first
edge detection second

That matters because an apparent betting edge is meaningless if the underlying simulation surface is not proven, stable, and measurable.

2. App direction

The app is intended to support MLB game prediction through a layered model/simulation pipeline.

The current Layer 6 work is focused on whether game-mechanics realism can affect the model output path.

Examples of realism mechanics being tracked include:

bullpen_active
double_play_reachable_delta_unproven
sac_fly_reachable_delta_unproven
extras_walkoff_bypassed
steals_inactive
balk_deferred

The current UI/backtest realism labels are:

current_ui_realism_state_label = bullpen_active_partial_realism
backtest_label = current_ui_projection_path_bullpen_active_partial_realism

These labels mean the system has some realism features reachable or partially wired, but Layer 6 has not exited yet.

3. Why we use the layer system

The layer system exists to prevent fuzzy progress.

Each layer has:

one purpose
one diagnosis string
one recommended next layer
one recommended path
hard safety boundaries
explicit artifacts
explicit blockers

The layer system makes it clear whether we are:

planning
implementing
auditing
measuring
activating
exiting

We do not mix these modes.

Examples:

Plan layer:
  defines what should happen next, but does not execute it.

Implementation layer:
  performs exactly the allowed operation.

Audit layer:
  validates what happened and routes the next step.

Measurement layer:
  only happens after a real surface exists and has been audited.

Activation layer:
  only happens after measurement proves the feature is safe and useful.

Exit layer:
  only happens when all required evidence exists.
4. Non-negotiable workflow rules

Future work must follow these rules:

No guessing.
No vague "probably."
No hidden assumptions.
No unplanned adapter calls.
No unplanned metrics.
No production changes unless explicitly planned and audited.
No Layer 6 exit without evidence.

Every layer should produce:

tmp/<layer_slug>.json
tmp/<layer_slug>_checks.csv
tmp/<layer_slug>_predecessor.csv
tmp/<layer_slug>_input_artifacts.csv
tmp/<layer_slug>_decision.csv
tmp/<layer_slug>_safety_boundaries.csv
tmp/<layer_slug>_recommended_path.csv

Additional artifacts should be added when useful, but the layer must remain focused.

Every response after running a layer should classify it as:

PASS
FAIL
PARTIAL / BLOCKED

A valid PASS should identify:

all_checks_passed = true
diagnosis = expected diagnosis string
recommended_next_layer = expected next layer
recommended_path = expected path
5. Git / PR process being used

Each layer gets its own branch.

Branch naming pattern:

<layer>_<short_layer_description>

Example:

6LX_layer6_projection_adapter_probability_alias_normalization_implementation

Commit naming pattern:

Add <layer> Layer 6 <description>

Example:

Add 6LX Layer 6 projection adapter probability alias normalization implementation

PR body should include:

Summary
Validation
Key JSON excerpt
Gate decision
Safety boundaries
Current blockers
Recommended next layer

After a PR is merged, verify it before generating the next layer.

6. Important repository hygiene

There are persistent untracked scripts that should usually be ignored unless directly relevant:

scripts/audit_pitcher_aggregate_rate_provenance.py
scripts/backtest_extras_walkoff_hybrid_pairing.py
scripts/backtest_transition_parameter_sensitivity.py
scripts/debug_extras_walkoff_payload_paths.py

Do not accidentally add those unless a layer explicitly requires them.

Use:

git status

before every commit.

7. What Layer 6 has been doing

Layer 6 is trying to answer:

Can game-mechanics realism be measured through the current UI/projection/backtest path?

To measure realism effects, the project needs a model output surface containing usable prediction fields.

Target fields originally sought:

home_win_probability
away_win_probability
home_expected_runs
away_expected_runs
total_expected_runs
projected_total

Without a prediction surface, we cannot measure whether realism mechanics matter.

8. Major discoveries so far
8.1 The adapter path exists

The target adapter is:

mlb_app.ai_data_assistant_performance::_canonical_games_from_projection_payload

Required contract:

_canonical_games_from_projection_payload(payload, game_pk, limit)

The correct future call shape was established as:

_canonical_games_from_projection_payload(payload, game_pk=824776, limit=1)
8.2 The payload shape was repaired

The adapter expects:

payload = {
    "games": [...]
}

Earlier payload fixtures were not shaped correctly for the adapter.

A non-production payload artifact was created with:

payload["games"]
game_pk = 824776
limit = 1
8.3 The adapter call succeeded

Layer 6LU executed exactly one controlled non-production adapter call.

The call succeeded.

It returned:

return_type = list
return_list_length = 1
first_item_type = dict

Returned first-row keys included:

away_win_prob
canonical_game_context
data_confidence
favorite
favorite_probability
game_pk
home_win_prob
label
lineup_status
missing_inputs
model_version
probability_component_keys
simulation_is_final_probability
simulation_role

This proved:

adapter plumbing is live
payload shape repair worked
the call path is not the current blocker
8.4 The returned surface used probability aliases

The adapter returned:

home_win_prob
away_win_prob
favorite_probability

but did not return:

home_win_probability
away_win_probability

So Layer 6 reclassified the blocker.

Old blocker:

adapter execution unknown

Resolved.

New blocker:

prediction_field_contract_normalization_needed
8.5 Run surface remains absent

The following fields are still absent:

home_expected_runs
away_expected_runs
total_expected_runs
projected_total

This means run metrics remain blocked.

The probability path and run path must be treated separately.

9. Current exact state after PR #699

PR #699 merged Layer 6LX:

6LX_layer_6_projection_adapter_probability_alias_normalization_implementation

6LX created a non-production normalized surface artifact:

tmp/layer6_6lx_projection_adapter_probability_alias_normalization_implementation_normalized_surface.json

6LX applied:

home_win_prob -> home_win_probability
away_win_prob -> away_win_probability

6LX preserved:

home_win_prob
away_win_prob
game_pk

6LX explicitly preserved the run-surface gap:

home_expected_runs = null
away_expected_runs = null
total_expected_runs = null
projected_total = null

6LX did not:

execute adapter calls
compute metrics
run backtests
fetch live data
write databases
modify production code
activate mechanics
grant Layer 6 exit

The 6LX result established:

probability_surface_materialized_after_implementation = true
probability_metric_ready_after_implementation = false
runs_metric_ready_after_implementation = false
any_backtest_metric_ready_after_implementation = false
run_surface_gap_remains = true
layer_6_exit_recommended = false

Important interpretation:

A non-production probability-normalized surface now exists.
It still requires audit.
It is not yet a metric-ready backtest surface.
10. Current next layer

The next layer should be:

6LY_layer_6_projection_adapter_probability_alias_normalization_audit

Recommended path:

audit_probability_alias_normalization_artifact

Purpose:

Audit the 6LX normalized surface artifact.
Confirm the alias mappings were applied.
Confirm canonical probability fields are present.
Confirm original alias fields are preserved.
Confirm the artifact is non-production.
Confirm it is not a backtest surface.
Confirm run surface gap remains.
Confirm no metrics are ready yet.
Route the next layer.

6LY should not:

execute adapter calls
compute metrics
run backtests
fetch data
write databases
modify production code
activate mechanics
grant Layer 6 exit
11. What 6LY should decide

6LY should determine whether the probability side is ready for a future probability-surface measurement plan.

Likely route if 6LY passes:

6LZ_layer_6_projection_adapter_probability_surface_metric_plan

or similar.

But do not guess the name unless the 6LY script explicitly emits it.

The key decision should be:

probability surface normalized and audited: true or false
run surface still blocked: true
metrics still not run: true
Layer 6 exit still blocked: true
12. Current blockers

As of after 6LX / PR #699:

probability_alias_normalization_requires_audit
run_surface_gap_remains
real_backtest_metrics_not_run
layer6_exit_not_allowed

After 6LY, if the audit passes, the first blocker can become resolved or reclassified.

The run-surface blocker should remain unresolved until an expected-runs path is found or explicitly scoped out.

13. How to continue the process

For each next layer:

Start from clean upstream main.
git checkout main
git fetch upstream main
git reset --hard upstream/main
Create a layer branch.
git checkout -b <layer_branch>
Add exactly one script for the layer.
scripts/<layer_script>.py
Script must:
read predecessor JSON and CSV artifacts.
validate predecessor diagnosis.
validate required input artifacts.
write layer JSON and CSV artifacts.
emit a clear diagnosis.
emit recommended next layer and path.
preserve safety boundaries.
Run syntax compile.
export PYTHONPATH=$(pwd)
export PYTHONDONTWRITEBYTECODE=1

python - <<'PY'
from pathlib import Path
failures = []
for root in [Path("mlb_app"), Path("scripts")]:
    if not root.exists():
        continue
    for path in sorted(root.rglob("*.py")):
        try:
            compile(path.read_text(encoding="utf-8"), str(path), "exec")
        except Exception as exc:
            failures.append(f"{path}: {type(exc).__name__}: {exc}")
if failures:
    print("\n".join(failures))
    raise SystemExit(1)
print("syntax compile passed for mlb_app and scripts without writing pyc files")
PY
Run the layer script.
python scripts/<layer_script>.py
Cat the artifacts.
cat tmp/<layer_slug>_checks.csv
cat tmp/<layer_slug>_predecessor.csv
cat tmp/<layer_slug>_decision.csv
cat tmp/<layer_slug>_safety_boundaries.csv
cat tmp/<layer_slug>_recommended_path.csv
cat tmp/<layer_slug>.json
Only commit if PASS.
git status
git diff -- scripts/<layer_script>.py
git add scripts/<layer_script>.py
git commit -m "Add <layer> Layer 6 <description>"
git push -u origin <layer_branch>
Open PR.
Merge PR.
Verify merged PR.
Generate next layer.
14. How to avoid breaking the workflow

Do not combine layers.

Do not jump from planning directly to production change.

Do not compute metrics in audit or implementation layers unless a prior plan explicitly allowed metrics.

Do not run additional adapter calls unless a prior plan explicitly allowed the exact call count.

Do not treat non-production artifacts as backtest-ready.

Do not treat placeholder fields as real predictions.

Do not invent run projections.

Do not call a feature complete just because one probability surface exists.

15. Current mental model of progress

The project has moved through these phases:

1. Identify whether realism mechanics are reachable.
2. Find projection/backtest measurement surface.
3. Trace adapter candidate.
4. Fix import context.
5. Fix call contract.
6. Fix payload shape.
7. Execute one controlled adapter call.
8. Audit returned surface.
9. Reclassify blocker to alias normalization.
10. Plan alias normalization.
11. Implement non-production alias normalization artifact.
12. Next: audit alias normalization artifact.

This is coherent progress.

The project is no longer stuck at:

Can we call the adapter?

That is resolved.

The project is now at:

Can the normalized probability surface become an audited measurement surface?

The current answer is:

Not yet.
6LY must audit the normalized artifact first.
16. What success looks like next

Near-term success:

6LY passes.
Probability normalization artifact is audited.
Canonical probability fields are confirmed present.
Artifact is non-production and not a backtest surface.
Run surface gap remains explicit.
Metrics remain blocked until planned.

Medium-term success:

A probability metric plan is created.
A probability metric implementation is run on an audited surface.
The metric output is audited.

Later success:

A real backtest path is measured.
Realism mechanics show measurable deltas or are proven not to.
Activation is planned only after evidence exists.
Layer 6 exits only when the evidence supports it.
17. Exact current handoff summary

Use this summary when continuing:

We are in Layer 6 of msantoria/mlb-prediction-app.

The vision is to build a credible MLB simulation model before edge detection.

We are using a strict layer system:
plan -> implement -> audit -> measure -> audit -> activate only if supported.

PR #699 merged 6LX:
6LX_layer_6_projection_adapter_probability_alias_normalization_implementation.

6LX wrote a non-production normalized probability surface artifact:
tmp/layer6_6lx_projection_adapter_probability_alias_normalization_implementation_normalized_surface.json

It mapped:
home_win_prob -> home_win_probability
away_win_prob -> away_win_probability

It preserved:
home_win_prob
away_win_prob
game_pk

It preserved run gaps:
home_expected_runs = null
away_expected_runs = null
total_expected_runs = null
projected_total = null

No adapter call, metrics, backtest, production code modification, activation, or Layer 6 exit occurred in 6LX.

Next required layer:
6LY_layer_6_projection_adapter_probability_alias_normalization_audit

6LY should audit the normalized artifact and decide whether a probability metric planning layer can come next.
18. Standard response format for future assistant/operator

When reviewing output, respond like this:

PASS / FAIL.

Key result:
<important JSON fields>

What this proves:
<short explanation>

Still blocked:
<explicit blockers>

Next layer:
<recommended_next_layer>

Commit/push commands:
<exact commands>

When creating the next layer, provide:

1. Short explanation.
2. Prompt.
3. One full executable bash block.

Do not split code across multiple blocks if the user asks for one block.

19. Current recommended next prompt

The next task should ask for 6LY:

Create 6LY_layer_6_projection_adapter_probability_alias_normalization_audit.

Audit 6LX normalized surface artifact.

Confirm:
- 6LX passed.
- normalized surface artifact exists.
- row_count = 1.
- game_pk present.
- home_win_probability present.
- away_win_probability present.
- home_win_prob preserved.
- away_win_prob preserved.
- non_production = true.
- not_a_backtest_surface = true.
- run surface gap remains.
- no metrics are ready yet.
- no adapter calls occurred.
- no production code changed.
- no activation occurred.
- Layer 6 exit remains blocked.

Recommend the next layer only after audit.
20. Final principle

This project should keep moving in small, auditable increments.

Do not optimize for speed.

Optimize for:

traceability
measurement integrity
no guessing
clear blockers
clear next route
simulation credibility before edge detection
