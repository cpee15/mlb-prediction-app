from __future__ import annotations

import csv
import datetime as dt
import json
import os
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, Iterable, List, Optional, Tuple

from mlb_app.database import create_tables, get_engine, get_session
from mlb_app.lineup_profile import fetch_boxscore_lineup
from mlb_app.matchup_generator import generate_matchups_for_date

from scripts.audit_player_true_talent_profiles import (
    COUNT_METRICS,
    DEFAULT_COMPONENT_WEIGHTS,
    DEFAULT_MIN_PA_BY_COMPONENT,
    METRICS,
    _aggregate_lineup_profiles,
    _build_player_true_talent_profile,
    _date_range,
    _metric_deltas,
    _safe_float,
    _safe_int,
    _side_context,
    _team_inputs,
)
from scripts.audit_offensive_expected_stat_profiles import (
    _build_expected_contact_quality_profile,
)


LEAGUE_XWOBA = float(os.getenv("LEAGUE_XWOBA", "0.320"))

DEFAULT_SCALES = [0.05, 0.10, 0.15, 0.20]
DEFAULT_AVG_CAPS = [0.005, 0.010, 0.015]
DEFAULT_SLG_CAPS = [0.010, 0.020, 0.030]
DEFAULT_ISO_CAPS = [0.005, 0.010, 0.020]

PRESET_SETTINGS = [
    {
        "setting": "observed_true_talent_baseline",
        "scale": 0.0,
        "avg_cap": 0.0,
        "slg_cap": 0.0,
        "iso_cap": 0.0,
        "preset": True,
    },
    {
        "setting": "contact_modifier_conservative",
        "scale": 0.05,
        "avg_cap": 0.005,
        "slg_cap": 0.010,
        "iso_cap": 0.005,
        "preset": True,
    },
    {
        "setting": "contact_modifier_medium",
        "scale": 0.10,
        "avg_cap": 0.010,
        "slg_cap": 0.020,
        "iso_cap": 0.010,
        "preset": True,
    },
    {
        "setting": "contact_modifier_aggressive",
        "scale": 0.20,
        "avg_cap": 0.015,
        "slg_cap": 0.030,
        "iso_cap": 0.020,
        "preset": True,
    },
]


def _distribution(values: List[Any]) -> Dict[str, Optional[float]]:
    clean = sorted(float(v) for v in values if v is not None)
    if not clean:
        return {"count": 0, "min": None, "median": None, "avg": None, "p75": None, "p90": None, "p95": None, "max": None}

    def pct(p: float) -> float:
        if len(clean) == 1:
            return clean[0]
        idx = int(round((len(clean) - 1) * p))
        return clean[idx]

    return {
        "count": len(clean),
        "min": round(clean[0], 4),
        "median": round(float(median(clean)), 4),
        "avg": round(float(mean(clean)), 4),
        "p75": round(pct(0.75), 4),
        "p90": round(pct(0.90), 4),
        "p95": round(pct(0.95), 4),
        "max": round(clean[-1], 4),
    }


def _parse_float_list_env(name: str, default: List[float]) -> List[float]:
    raw = os.getenv(name)
    if not raw:
        return list(default)
    return [float(part.strip()) for part in raw.split(",") if part.strip()]


def _setting_grid() -> List[Dict[str, Any]]:
    scales = _parse_float_list_env("CONTACT_MODIFIER_SCALES", DEFAULT_SCALES)
    avg_caps = _parse_float_list_env("CONTACT_MODIFIER_AVG_CAPS", DEFAULT_AVG_CAPS)
    slg_caps = _parse_float_list_env("CONTACT_MODIFIER_SLG_CAPS", DEFAULT_SLG_CAPS)
    iso_caps = _parse_float_list_env("CONTACT_MODIFIER_ISO_CAPS", DEFAULT_ISO_CAPS)

    settings: List[Dict[str, Any]] = list(PRESET_SETTINGS)
    seen = {row["setting"] for row in settings}

    for scale in scales:
        for avg_cap in avg_caps:
            for slg_cap in slg_caps:
                for iso_cap in iso_caps:
                    setting = f"grid_s{scale:.2f}_avg{avg_cap:.3f}_slg{slg_cap:.3f}_iso{iso_cap:.3f}"
                    if setting in seen:
                        continue
                    seen.add(setting)
                    settings.append(
                        {
                            "setting": setting,
                            "scale": scale,
                            "avg_cap": avg_cap,
                            "slg_cap": slg_cap,
                            "iso_cap": iso_cap,
                            "preset": False,
                        }
                    )

    return settings


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _bounded_correction(observed: Any, expected: Any, scale: float, cap: float) -> Tuple[Optional[float], Optional[float]]:
    observed_value = _safe_float(observed)
    expected_value = _safe_float(expected)

    if observed_value is None:
        return None, None
    if expected_value is None or scale <= 0 or cap <= 0:
        return round(observed_value, 4), 0.0

    correction = _clamp((expected_value - observed_value) * scale, -cap, cap)
    adjusted = observed_value + correction
    return round(adjusted, 4), round(correction, 4)


def _apply_expected_contact_modifier(
    observed: Dict[str, Any],
    expected: Dict[str, Any],
    setting: Dict[str, Any],
) -> Dict[str, Any]:
    scale = float(setting["scale"])
    avg_cap = float(setting["avg_cap"])
    slg_cap = float(setting["slg_cap"])
    iso_cap = float(setting["iso_cap"])

    adjusted = dict(observed or {})
    adjusted["source"] = f"audit_expected_contact_modifier_{setting['setting']}"
    adjusted["profile_variant"] = setting["setting"]
    adjusted["contact_modifier_scale"] = scale
    adjusted["avg_cap"] = avg_cap
    adjusted["slg_cap"] = slg_cap
    adjusted["iso_cap"] = iso_cap

    adjusted_avg, avg_correction = _bounded_correction(
        (observed or {}).get("batting_avg"),
        (expected or {}).get("xba"),
        scale,
        avg_cap,
    )
    adjusted_slg, slg_correction = _bounded_correction(
        (observed or {}).get("slugging_pct"),
        (expected or {}).get("xslg_proxy"),
        scale,
        slg_cap,
    )
    adjusted_iso, iso_correction = _bounded_correction(
        (observed or {}).get("iso"),
        (expected or {}).get("xiso_proxy"),
        scale,
        iso_cap,
    )

    adjusted["batting_avg"] = adjusted_avg
    adjusted["slugging_pct"] = adjusted_slg
    adjusted["iso"] = adjusted_iso

    # Keep discipline from observed true talent. Expected contact quality does not estimate plate discipline.
    adjusted["k_pct"] = (observed or {}).get("k_pct")
    adjusted["bb_pct"] = (observed or {}).get("bb_pct")
    adjusted["on_base_pct"] = (observed or {}).get("on_base_pct")

    adjusted["avg_correction"] = avg_correction
    adjusted["slg_correction"] = slg_correction
    adjusted["iso_correction"] = iso_correction

    adjusted["expected_xba"] = (expected or {}).get("xba")
    adjusted["expected_xwoba"] = (expected or {}).get("xwoba")
    adjusted["expected_xslg_proxy"] = (expected or {}).get("xslg_proxy")
    adjusted["expected_xiso_proxy"] = (expected or {}).get("xiso_proxy")
    adjusted["expected_avg_exit_velocity"] = (expected or {}).get("avg_exit_velocity")
    adjusted["expected_avg_launch_angle"] = (expected or {}).get("avg_launch_angle")
    adjusted["expected_hard_hit_pct"] = (expected or {}).get("hard_hit_pct")
    adjusted["expected_barrel_pct"] = (expected or {}).get("barrel_pct")
    adjusted["expected_sweet_spot_pct"] = (expected or {}).get("sweet_spot_pct")
    adjusted["expected_pa"] = (expected or {}).get("pa")
    adjusted["observed_pa"] = (observed or {}).get("pa")
    adjusted["has_expected_contact_profile"] = bool((expected or {}).get("has_usable_expected_profile"))
    adjusted["has_usable_true_talent_profile"] = bool((observed or {}).get("has_usable_true_talent_profile"))

    return adjusted


def _aggregate_modified_profiles(
    profiles: List[Dict[str, Any]],
    lineup_count: int,
    min_usable_hitters: int,
    setting_name: str,
) -> Dict[str, Any]:
    usable = [profile for profile in profiles if profile.get("has_usable_true_talent_profile")]

    out: Dict[str, Any] = {
        "source": f"audit_expected_contact_modifier_lineup_{setting_name}",
        "profile_variant": setting_name,
        "starting_lineup_count": lineup_count,
        "usable_hitter_profile_count": len(usable),
        "min_usable_hitters": min_usable_hitters,
        "would_activate": len(usable) >= min_usable_hitters,
    }

    for metric in METRICS:
        values = [_safe_float(profile.get(metric)) for profile in usable if _safe_float(profile.get(metric)) is not None]
        out[metric] = round(float(mean(values)), 4) if values else None

    for metric in COUNT_METRICS:
        values = [_safe_int(profile.get(metric)) for profile in usable if _safe_int(profile.get(metric)) is not None]
        out[metric] = int(sum(values)) if values else None

    for metric in (
        "avg_correction",
        "slg_correction",
        "iso_correction",
        "expected_xba",
        "expected_xwoba",
        "expected_xslg_proxy",
        "expected_xiso_proxy",
        "expected_avg_exit_velocity",
        "expected_avg_launch_angle",
        "expected_hard_hit_pct",
        "expected_barrel_pct",
        "expected_sweet_spot_pct",
    ):
        values = [_safe_float(profile.get(metric)) for profile in usable if _safe_float(profile.get(metric)) is not None]
        out[metric] = round(float(mean(values)), 4) if values else None

    out["observed_pa"] = sum(_safe_int(profile.get("observed_pa")) or 0 for profile in usable)
    out["expected_pa"] = sum(_safe_int(profile.get("expected_pa")) or 0 for profile in usable)
    out["expected_contact_hitter_count"] = sum(1 for profile in usable if profile.get("has_expected_contact_profile"))

    return out


def _is_extreme_delta(row: Dict[str, Any]) -> bool:
    return (
        abs(_safe_float(row.get("iso_delta")) or 0.0) > 0.030
        or abs(_safe_float(row.get("slugging_pct_delta")) or 0.0) > 0.050
        or abs(_safe_float(row.get("k_pct_delta")) or 0.0) > 0.050
        or abs(_safe_float(row.get("bb_pct_delta")) or 0.0) > 0.030
    )


def _abs_adjustments(row: Dict[str, Any]) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {}
    for metric in METRICS:
        delta = _safe_float(row.get(f"{metric}_delta"))
        out[f"{metric}_abs_adjustment"] = round(abs(delta), 4) if delta is not None else None
    return out


def _audit_side(
    session,
    date_str: str,
    matchup: Dict[str, Any],
    side: str,
    settings: List[Dict[str, Any]],
    min_usable_hitters: int,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    target_date = dt.date.fromisoformat(date_str)
    season = target_date.year
    ctx = _side_context(matchup, side)

    game_pk = _safe_int(ctx.get("game_pk"))
    team_id = _safe_int(ctx.get("team_id"))
    split = ctx.get("split") or "vsR"

    if not game_pk or not team_id:
        return [], []

    try:
        lineups = fetch_boxscore_lineup(game_pk)
    except Exception:
        return [], []

    lineup = lineups.get(side) or []
    team_inputs = _team_inputs(session, team_id, season, split)

    modified_profiles_by_setting: Dict[str, List[Dict[str, Any]]] = {
        setting["setting"]: [] for setting in settings
    }
    hitter_rows: List[Dict[str, Any]] = []

    for hitter in lineup:
        player_id = _safe_int(hitter.get("batter_id"))
        player_name = hitter.get("name")
        if not player_id:
            continue

        observed = _build_player_true_talent_profile(
            session=session,
            player_id=player_id,
            player_name=player_name,
            target_date=target_date,
            split=split,
            base_weights=dict(DEFAULT_COMPONENT_WEIGHTS),
            min_pa_by_component=dict(DEFAULT_MIN_PA_BY_COMPONENT),
        )
        expected = _build_expected_contact_quality_profile(
            session=session,
            player_id=player_id,
            player_name=player_name,
            target_date=target_date,
            split=split,
        )

        for setting in settings:
            modified = _apply_expected_contact_modifier(observed, expected, setting)
            modified_profiles_by_setting[setting["setting"]].append(modified)

            hitter_rows.append(
                {
                    "setting": setting["setting"],
                    "date": date_str,
                    "game_pk": game_pk,
                    "side": side,
                    "team_id": team_id,
                    "team_name": ctx.get("team_name"),
                    "split": split,
                    "player_id": player_id,
                    "player_name": player_name,
                    "lineup_slot": hitter.get("lineup_slot"),
                    "observed_batting_avg": observed.get("batting_avg"),
                    "observed_slugging_pct": observed.get("slugging_pct"),
                    "observed_iso": observed.get("iso"),
                    "observed_on_base_pct": observed.get("on_base_pct"),
                    "observed_k_pct": observed.get("k_pct"),
                    "observed_bb_pct": observed.get("bb_pct"),
                    "expected_xba": expected.get("xba"),
                    "expected_xwoba": expected.get("xwoba"),
                    "expected_xslg_proxy": expected.get("xslg_proxy"),
                    "expected_xiso_proxy": expected.get("xiso_proxy"),
                    "expected_avg_exit_velocity": expected.get("avg_exit_velocity"),
                    "expected_avg_launch_angle": expected.get("avg_launch_angle"),
                    "expected_hard_hit_pct": expected.get("hard_hit_pct"),
                    "expected_barrel_pct": expected.get("barrel_pct"),
                    "expected_sweet_spot_pct": expected.get("sweet_spot_pct"),
                    "adjusted_batting_avg": modified.get("batting_avg"),
                    "adjusted_slugging_pct": modified.get("slugging_pct"),
                    "adjusted_iso": modified.get("iso"),
                    "avg_correction": modified.get("avg_correction"),
                    "slg_correction": modified.get("slg_correction"),
                    "iso_correction": modified.get("iso_correction"),
                    "has_expected_contact_profile": modified.get("has_expected_contact_profile"),
                    "has_usable_true_talent_profile": modified.get("has_usable_true_talent_profile"),
                    "observed_pa": modified.get("observed_pa"),
                    "expected_pa": modified.get("expected_pa"),
                    "scale": setting["scale"],
                    "avg_cap": setting["avg_cap"],
                    "slg_cap": setting["slg_cap"],
                    "iso_cap": setting["iso_cap"],
                    "preset": setting["preset"],
                }
            )

    side_rows: List[Dict[str, Any]] = []
    for setting in settings:
        setting_name = setting["setting"]
        lineup_inputs = _aggregate_modified_profiles(
            profiles=modified_profiles_by_setting[setting_name],
            lineup_count=len(lineup),
            min_usable_hitters=min_usable_hitters,
            setting_name=setting_name,
        )

        row = {
            "setting": setting_name,
            "date": date_str,
            "game_pk": game_pk,
            "side": side,
            "team_id": team_id,
            "team_name": ctx.get("team_name"),
            "split": split,
            "starting_lineup_count": len(lineup),
            "usable_hitter_profile_count": lineup_inputs.get("usable_hitter_profile_count"),
            "expected_contact_hitter_count": lineup_inputs.get("expected_contact_hitter_count"),
            "min_usable_hitters": min_usable_hitters,
            "would_activate": lineup_inputs.get("would_activate"),
            "team_source": team_inputs.get("source"),
            "profile_source": lineup_inputs.get("source"),
            "scale": setting["scale"],
            "avg_cap": setting["avg_cap"],
            "slg_cap": setting["slg_cap"],
            "iso_cap": setting["iso_cap"],
            "preset": setting["preset"],
            "lineup_avg_correction": lineup_inputs.get("avg_correction"),
            "lineup_slg_correction": lineup_inputs.get("slg_correction"),
            "lineup_iso_correction": lineup_inputs.get("iso_correction"),
            "lineup_expected_xba": lineup_inputs.get("expected_xba"),
            "lineup_expected_xwoba": lineup_inputs.get("expected_xwoba"),
            "lineup_expected_xslg_proxy": lineup_inputs.get("expected_xslg_proxy"),
            "lineup_expected_xiso_proxy": lineup_inputs.get("expected_xiso_proxy"),
            "lineup_hard_hit_pct": lineup_inputs.get("expected_hard_hit_pct"),
            "lineup_barrel_pct": lineup_inputs.get("expected_barrel_pct"),
            "lineup_sweet_spot_pct": lineup_inputs.get("expected_sweet_spot_pct"),
            "observed_pa": lineup_inputs.get("observed_pa"),
            "expected_pa": lineup_inputs.get("expected_pa"),
            **_metric_deltas(team_inputs, lineup_inputs),
        }
        row["extreme_delta"] = _is_extreme_delta(row)
        row.update(_abs_adjustments(row))
        side_rows.append(row)

    return side_rows, hitter_rows


def _summarize_setting(rows: List[Dict[str, Any]], hitter_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    team_sides = len(rows)
    extreme_delta_sides = sum(1 for row in rows if row.get("extreme_delta"))

    summary: Dict[str, Any] = {
        "team_sides": team_sides,
        "lineup_sides_would_activate": sum(1 for row in rows if row.get("would_activate")),
        "extreme_delta_sides": extreme_delta_sides,
        "extreme_delta_rate": round(extreme_delta_sides / team_sides, 4) if team_sides else None,
        "expected_contact_coverage": {
            "hitter_rows": len(hitter_rows),
            "hitter_rows_with_expected_contact": sum(1 for row in hitter_rows if row.get("has_expected_contact_profile")),
            "avg_expected_contact_hitters_per_side": round(
                mean([_safe_float(row.get("expected_contact_hitter_count")) or 0.0 for row in rows]),
                4,
            ) if rows else None,
        },
        "correction_distributions": {
            "avg_correction": _distribution([row.get("lineup_avg_correction") for row in rows]),
            "slg_correction": _distribution([row.get("lineup_slg_correction") for row in rows]),
            "iso_correction": _distribution([row.get("lineup_iso_correction") for row in rows]),
        },
        "delta_distributions": {
            metric: _distribution([row.get(f"{metric}_delta") for row in rows])
            for metric in METRICS
        },
        "avg_abs_adjustment": {},
        "max_abs_adjustment": {},
        "top_positive_avg_corrections": sorted(
            rows,
            key=lambda row: _safe_float(row.get("lineup_avg_correction")) or -999,
            reverse=True,
        )[:10],
        "top_negative_avg_corrections": sorted(
            rows,
            key=lambda row: _safe_float(row.get("lineup_avg_correction")) or 999,
        )[:10],
        "top_positive_iso_corrections": sorted(
            rows,
            key=lambda row: _safe_float(row.get("lineup_iso_correction")) or -999,
            reverse=True,
        )[:10],
        "top_negative_iso_corrections": sorted(
            rows,
            key=lambda row: _safe_float(row.get("lineup_iso_correction")) or 999,
        )[:10],
    }

    for metric in METRICS:
        key = f"{metric}_abs_adjustment"
        values = [_safe_float(row.get(key)) for row in rows if _safe_float(row.get(key)) is not None]
        summary["avg_abs_adjustment"][metric] = round(float(mean(values)), 4) if values else None
        summary["max_abs_adjustment"][metric] = round(max(values), 4) if values else None

    return summary


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        path.write_text("")
        return
    fieldnames = sorted({key for row in rows for key in row.keys()})
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    start = os.getenv("BACKTEST_START", "2026-04-20")
    end = os.getenv("BACKTEST_END", "2026-05-03")
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    min_usable_hitters = int(os.getenv("MIN_USABLE_HITTERS", "7"))

    settings = _setting_grid()

    engine = get_engine(database_url)
    create_tables(engine)
    SessionLocal = get_session(engine)

    side_rows: List[Dict[str, Any]] = []
    hitter_rows: List[Dict[str, Any]] = []

    with SessionLocal() as session:
        for date_str in _date_range(start, end):
            matchups = generate_matchups_for_date(session, date_str)
            for matchup in matchups:
                for side in ("away", "home"):
                    rows, hitters = _audit_side(
                        session=session,
                        date_str=date_str,
                        matchup=matchup,
                        side=side,
                        settings=settings,
                        min_usable_hitters=min_usable_hitters,
                    )
                    side_rows.extend(rows)
                    hitter_rows.extend(hitters)

    rows_by_setting = {
        setting["setting"]: [row for row in side_rows if row.get("setting") == setting["setting"]]
        for setting in settings
    }
    hitter_rows_by_setting = {
        setting["setting"]: [row for row in hitter_rows if row.get("setting") == setting["setting"]]
        for setting in settings
    }

    summary_by_setting = {
        setting: _summarize_setting(rows_by_setting[setting], hitter_rows_by_setting[setting])
        for setting in rows_by_setting.keys()
    }

    ranked_settings = sorted(
        [
            {
                "setting": setting,
                "preset": next((s["preset"] for s in settings if s["setting"] == setting), False),
                "scale": next((s["scale"] for s in settings if s["setting"] == setting), None),
                "avg_cap": next((s["avg_cap"] for s in settings if s["setting"] == setting), None),
                "slg_cap": next((s["slg_cap"] for s in settings if s["setting"] == setting), None),
                "iso_cap": next((s["iso_cap"] for s in settings if s["setting"] == setting), None),
                "extreme_delta_sides": summary.get("extreme_delta_sides"),
                "extreme_delta_rate": summary.get("extreme_delta_rate"),
                "avg_abs_batting_avg_adjustment": summary.get("avg_abs_adjustment", {}).get("batting_avg"),
                "avg_abs_slugging_pct_adjustment": summary.get("avg_abs_adjustment", {}).get("slugging_pct"),
                "avg_abs_iso_adjustment": summary.get("avg_abs_adjustment", {}).get("iso"),
                "lineup_avg_correction_median": summary.get("correction_distributions", {}).get("avg_correction", {}).get("median"),
                "lineup_slg_correction_median": summary.get("correction_distributions", {}).get("slg_correction", {}).get("median"),
                "lineup_iso_correction_median": summary.get("correction_distributions", {}).get("iso_correction", {}).get("median"),
            }
            for setting, summary in summary_by_setting.items()
        ],
        key=lambda row: (
            row["extreme_delta_sides"] if row["extreme_delta_sides"] is not None else 999999,
            row["avg_abs_iso_adjustment"] if row["avg_abs_iso_adjustment"] is not None else 999999,
            row["avg_abs_slugging_pct_adjustment"] if row["avg_abs_slugging_pct_adjustment"] is not None else 999999,
        ),
    )

    baseline = summary_by_setting.get("observed_true_talent_baseline", {})
    candidate_settings = [
        row for row in ranked_settings
        if row["setting"] != "observed_true_talent_baseline"
        and row.get("extreme_delta_sides") is not None
        and baseline.get("extreme_delta_sides") is not None
        and row["extreme_delta_sides"] <= baseline["extreme_delta_sides"] + 5
    ]

    payload = {
        "start": start,
        "end": end,
        "database_url": database_url,
        "settings_count": len(settings),
        "baseline_extreme_delta_sides": baseline.get("extreme_delta_sides"),
        "candidate_settings_near_baseline": candidate_settings[:20],
        "ranked_settings": ranked_settings,
        "summary_by_setting": summary_by_setting,
        "recommendation": (
            "Audit only. Expected contact quality is applied as bounded corrections to observed true talent, "
            "not as direct stat replacement. Carry stable settings into a later simulation backtest grid only."
        ),
    }

    output_dir = Path("tmp")
    output_dir.mkdir(parents=True, exist_ok=True)
    prefix = f"expected_contact_modifier_grid_{start}_to_{end}"

    json_path = output_dir / f"{prefix}.json"
    team_sides_path = output_dir / f"{prefix}_team_sides.csv"
    hitters_path = output_dir / f"{prefix}_hitters.csv"
    ranked_path = output_dir / f"{prefix}_ranked_settings.csv"

    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str))
    _write_csv(team_sides_path, side_rows)
    _write_csv(hitters_path, hitter_rows)
    _write_csv(ranked_path, ranked_settings)

    print(json.dumps(
        {
            "baseline_extreme_delta_sides": payload["baseline_extreme_delta_sides"],
            "top_ranked_settings": ranked_settings[:15],
            "candidate_settings_near_baseline": candidate_settings[:15],
            "recommendation": payload["recommendation"],
        },
        indent=2,
        sort_keys=True,
        default=str,
    ))
    print(f"Wrote {json_path}")
    print(f"Wrote {team_sides_path}")
    print(f"Wrote {hitters_path}")
    print(f"Wrote {ranked_path}")


if __name__ == "__main__":
    main()
