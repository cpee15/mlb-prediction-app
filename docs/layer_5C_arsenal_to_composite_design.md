# Layer 5C — Arsenal-to-Composite Design Proposal

Diagnosis: arsenal_realism_design_ready_for_research_phase

The simulator currently consumes aggregate pitcher realism through active composites:
- pit_k
- pit_bb
- pit_xba
- pit_xwoba
- pit_hard_hit
- pit_hr

Dormant arsenal fields include:
- whiff_pct
- usage_pct
- rv_per_100
- arsenal
- pitch_mix

Key conclusion:
Detailed arsenal realism should blend into composite construction layers, NOT late-stage probability mutation.

Important safety constraints:
- research-flag only
- disabled by default
- no direct probability overrides
- no additive double counting
- recalibration required before activation

Candidate insertion seams:
- whiff_pct -> pit_k
- rv_per_100 -> pit_xwoba
- pitch_mix -> pit_hard_hit
- arsenal quality -> pit_hr
