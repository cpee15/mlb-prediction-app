from pathlib import Path
import csv
import json

OUTDIR = Path("tmp/layer_6NZ_model_projection_realism_gap_analysis_plan")
OUTDIR.mkdir(parents=True, exist_ok=True)

gap_inventory = [
    ["base_out_state","sim_reachable_not_projection_reachable","projection_wiring_gap","yes","critical","6OB"],
    ["base_advancement_transitions","sim_reachable_not_projection_reachable","projection_wiring_gap","yes","critical","6OB"],
    ["opener_bulk_pitcher","absent","missing_feature","yes","critical","6OC"],
    ["ghost_runner_extra_innings","sim_reachable_not_projection_reachable","projection_wiring_gap","yes","critical","6OB"],
    ["double_play_logic","sim_reachable_not_projection_reachable","projection_wiring_gap","yes","high","6OB"],
    ["sac_fly_logic","sim_reachable_not_projection_reachable","projection_wiring_gap","yes","high","6OB"],
    ["steals_caught_stealing","sim_reachable_not_projection_reachable","projection_wiring_gap","yes","high","6OB"],
    ["bullpen_transition","ui_visible_diagnostic_only","projection_visibility_gap","yes","medium","6OD"],
    ["dynamic_starter_exit","ui_visible_diagnostic_only","projection_visibility_gap","yes","medium","6OD"],
    ["balks","absent","missing_feature","yes","low","6OE"],
    ["wild_pitch_passed_ball","absent","missing_feature","yes","low","6OE"],
]

inventory_csv = OUTDIR / "realism_gap_inventory.csv"
with inventory_csv.open("w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "feature_name",
        "classification",
        "gap_category",
        "implementation_needed",
        "priority_level",
        "recommended_future_layer",
    ])
    writer.writerows(gap_inventory)

summary = {}
for row in gap_inventory:
    key = (row[1], row[2])
    summary[key] = summary.get(key, 0) + 1

summary_csv = OUTDIR / "realism_gap_summary.csv"
with summary_csv.open("w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["classification","gap_category","count"])
    for (c,g), count in sorted(summary.items()):
        writer.writerow([c,g,count])

priority_csv = OUTDIR / "realism_gap_priorities.csv"
with priority_csv.open("w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["feature_name","priority_level","recommended_future_layer"])
    for row in gap_inventory:
        writer.writerow([row[0], row[4], row[5]])

diagnosis = {
    "layer_id": "6NZ",
    "layer_name": "layer_6_model_projection_realism_gap_analysis_plan",
    "diagnosis": "layer_6_model_projection_realism_gap_analysis_plan_complete",
    "recommended_next_layer": "6OA_layer6_model_projection_realism_gap_analysis_audit",
    "generated_csv_artifacts": [
        str(inventory_csv),
        str(summary_csv),
        str(priority_csv),
    ],
    "generated_json_artifacts": [
        str(OUTDIR / "diagnosis.json")
    ],
}

(OUTDIR / "diagnosis.json").write_text(
    json.dumps(diagnosis, indent=2) + "\n",
    encoding="utf-8",
)

print(json.dumps(diagnosis, indent=2))
