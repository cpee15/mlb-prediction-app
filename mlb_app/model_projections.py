from __future__ import annotations

import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import inspect, text
from sqlalchemy.orm import Session

from .db_utils import get_pitch_arsenal_with_fallback, get_team_split
from .matchup_generator import generate_matchups_for_date
from .model_projection_formulas import bullpen_collapse_index, offensive_firepower_score, pitch_identity_disruption_score, pitching_volatility_score, safe_float

from .bullpen_profile import build_bullpen_profile
from .environment_profile import compute_environment_profile
from .team_offense_prior import build_team_offense_prior
from .simulation.pa_outcome_model import build_pa_outcome_probabilities
from .simulation.game_simulator import simulate_game_with_bullpen


def _obj_to_dict(obj: Any, fields: List[str]) -> Dict[str, Any]:
    return {field: getattr(obj, field, None) for field in fields} if obj is not None else {}


def _arsenal_records_to_dict(records: List[Any]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for record in records or []:
        pitch_type = getattr(record, "pitch_type", None) or getattr(record, "pitch_name", None) or "unknown"
        out[pitch_type] = {
            "pitch_type": getattr(record, "pitch_type", None),
            "pitch_name": getattr(record, "pitch_name", None),
            "pitch_count": getattr(record, "pitch_count", None),
            "usage_pct": getattr(record, "usage_pct", None),
            "whiff_pct": getattr(record, "whiff_pct", None),
            "strikeout_pct": getattr(record, "strikeout_pct", None),
            "rv_per_100": getattr(record, "rv_per_100", None),
            "xwoba": getattr(record, "xwoba", None),
            "hard_hit_pct": getattr(record, "hard_hit_pct", None),
        }
    return out


def _team_split_inputs(session: Session, team_id: Optional[int], season: int) -> Dict[str, Any]:
    if not team_id:
        return {"source": "missing_team_id"}
    row = get_team_split(session, int(team_id), season, "vsR") or get_team_split(session, int(team_id), season, "vsL")
    data = _obj_to_dict(row, ["pa", "hits", "doubles", "triples", "home_runs", "walks", "strikeouts", "batting_avg", "on_base_pct", "slugging_pct", "iso", "k_pct", "bb_pct"])
    data.update({
        "team_id": team_id,
        "split": getattr(row, "split", None) if row else None,
        "lineup_source": "team_splits_fallback_not_confirmed_lineup" if row else None,
        "player_count_used": None,
        "sample_blend": {"type": "team_split", "season": season, "split": getattr(row, "split", None)} if row else None,
        "source": "team_splits" if row else "missing_team_splits",
    })
    return data


def _find_column(columns: List[str], aliases: List[str]) -> Optional[str]:
    normalized = {col.lower().replace("_", "").replace("/", ""): col for col in columns}
    for alias in aliases:
        key = alias.lower().replace("_", "").replace("/", "")
        if key in normalized:
            return normalized[key]
    return None


def _bullpen_inputs(session: Session, team_id: Optional[int], team_name: Optional[str]) -> Dict[str, Any]:
    try:
        inspector = inspect(session.bind)
        table_names = set(inspector.get_table_names())
    except Exception:
        return {"source_table": None}
    table = next((name for name in ["bullpen_stats", "team_bullpen_stats", "table_layerseven", "layerseven", "team_pitching_bullpen", "team_pitching_stats"] if name in table_names), None)
    if not table:
        return {"source_table": None}
    try:
        columns = [col["name"] for col in inspector.get_columns(table)]
        team_id_col = _find_column(columns, ["team_id", "teamid", "mlb_team_id"])
        team_name_col = _find_column(columns, ["team_name", "team", "name"])
        era_col = _find_column(columns, ["era", "bullpen_era"])
        bb9_col = _find_column(columns, ["bb_per_9", "bb9", "bb_9", "bb_per_nine", "walks_per_9"])
        whip_col = _find_column(columns, ["whip", "bullpen_whip"])
        if not all([era_col, bb9_col, whip_col]):
            return {"source_table": table}
        where = None
        params: Dict[str, Any] = {}
        if team_id_col and team_id is not None:
            where = f"{team_id_col} = :team_id"
            params["team_id"] = int(team_id)
        elif team_name_col and team_name:
            where = f"lower({team_name_col}) = lower(:team_name)"
            params["team_name"] = team_name
        if not where:
            return {"source_table": table}
        row = session.execute(text(f"SELECT {era_col} AS era, {bb9_col} AS bb_per_9, {whip_col} AS whip FROM {table} WHERE {where} LIMIT 1"), params).mappings().first()
        return {"era": row.get("era"), "bb_per_9": row.get("bb_per_9"), "whip": row.get("whip"), "source_table": table} if row else {"source_table": table}
    except Exception as exc:
        return {"source_table": table, "error": str(exc)}


def _probability_model_card(
    model_name: str,
    score: Optional[float],
    inputs: Dict[str, Any],
    formula: str,
    steps: List[str],
    notes: List[str],
    confidence: str = "low",
) -> Dict[str, Any]:
    missing = [key for key, value in inputs.items() if value is None]
    return {
        "model_name": model_name,
        "status": "calculated" if score is not None and not missing else "partial" if score is not None else "missing_inputs",
        "score": round(float(score), 3) if score is not None else None,
        "formula": formula,
        "inputs": inputs,
        "calculation_steps": steps,
        "missing_inputs": missing,
        "data_confidence": confidence,
        "source_notes": notes,
    }


def _team_offense_prior_pa_model(
    team_id: Optional[int],
    team_name: Optional[str],
    opposing_pitcher_profile: Optional[Dict[str, Any]],
    environment_profile: Dict[str, Any],
) -> Dict[str, Any]:
    offense_profile = build_team_offense_prior(team_id=team_id, team_name=team_name)
    return build_pa_outcome_probabilities(
        batter_profile=offense_profile,
        pitcher_profile=opposing_pitcher_profile or {},
        environment_profile=environment_profile,
    )


def _weather_context(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        return {"wind": value}
    return {}


def _pitcher_workspace_profile(team: Dict[str, Any]) -> Dict[str, Any]:
    features = team.get("pitcher_features") or {}
    arsenal = team.get("pitch_arsenal") or {}

    k_rate = safe_float(features.get("k_pct"))
    bb_rate = safe_float(features.get("bb_pct"))
    hard_hit = safe_float(features.get("hard_hit_pct"))
    xwoba = safe_float(features.get("xwoba"))
    xba = safe_float(features.get("xba"))

    return {
        "metadata": {
            "source_type": "model_projection_pitcher_features",
            "generated_from": "model_projections._pitcher_workspace_profile",
            "data_confidence": "medium" if features else "low",
            "pitcher_id": team.get("pitcher_id"),
            "pitcher_name": team.get("pitcher_name"),
            "pitch_arsenal_source": team.get("pitch_arsenal_source"),
            "profile_granularity": "probable_pitcher",
        },
        "bat_missing": {
            "k_rate": k_rate,
            "whiff_rate": None,
            "csw_rate": None,
        },
        "command_control": {
            "bb_rate": bb_rate,
            "zone_rate": None,
            "first_pitch_strike_rate": None,
        },
        "contact_management": {
            "hard_hit_rate_allowed": hard_hit,
            "xwoba_allowed": xwoba,
            "xba_allowed": xba,
            "avg_exit_velocity_allowed": safe_float(features.get("avg_exit_velocity")),
            "avg_launch_angle_allowed": safe_float(features.get("avg_launch_angle")),
        },
        "arsenal": {
            "pitch_mix": arsenal,
            "avg_velocity": safe_float(features.get("avg_velocity")),
            "avg_spin_rate": safe_float(features.get("avg_spin_rate")),
        },
    }


def _offense_workspace_profile(team: Dict[str, Any]) -> Dict[str, Any]:
    inputs = team.get("offense_inputs") or {}
    return {
        "metadata": {
            "source_type": inputs.get("source") or "team_split_or_prior",
            "generated_from": "model_projections._offense_workspace_profile",
            "data_confidence": "low",
            "team_id": team.get("team_id"),
            "team_name": team.get("team_name"),
            "lineup_source": inputs.get("lineup_source"),
            "profile_granularity": "team_offense",
            "sample_blend": inputs.get("sample_blend"),
        },
        "contact_skill": {
            "k_rate": safe_float(inputs.get("k_pct")),
            "batting_avg": safe_float(inputs.get("batting_avg")),
            "contact_rate": None,
        },
        "plate_discipline": {
            "bb_rate": safe_float(inputs.get("bb_pct")),
            "on_base_pct": safe_float(inputs.get("on_base_pct")),
        },
        "power": {
            "iso": safe_float(inputs.get("iso")),
            "slugging_pct": safe_float(inputs.get("slugging_pct")),
            "home_runs": safe_float(inputs.get("home_runs")),
            "doubles": safe_float(inputs.get("doubles")),
            "triples": safe_float(inputs.get("triples")),
        },
        "run_creation": {
            "pa": safe_float(inputs.get("pa")),
            "hits": safe_float(inputs.get("hits")),
            "walks": safe_float(inputs.get("walks")),
            "strikeouts": safe_float(inputs.get("strikeouts")),
        },
    }


def _matchup_workspace_analysis(offense_team: Dict[str, Any], opposing_pitcher: Dict[str, Any]) -> Dict[str, Any]:
    offense_inputs = offense_team.get("offense_inputs") or {}
    pitcher_features = opposing_pitcher.get("pitcher_features") or {}
    arsenal = opposing_pitcher.get("pitch_arsenal") or {}

    offense_k = safe_float(offense_inputs.get("k_pct"))
    offense_bb = safe_float(offense_inputs.get("bb_pct"))
    pitcher_k = safe_float(pitcher_features.get("k_pct"))
    pitcher_bb = safe_float(pitcher_features.get("bb_pct"))

    pitch_edges = []
    for pitch_type, row in (arsenal or {}).items():
        if not isinstance(row, dict):
            continue
        pitch_edges.append({
            "pitch_type": pitch_type,
            "usage_pct": safe_float(row.get("usage_pct")),
            "whiff_pct": safe_float(row.get("whiff_pct")),
            "xwoba": safe_float(row.get("xwoba")),
            "hard_hit_pct": safe_float(row.get("hard_hit_pct")),
        })

    biggest_edge = None
    if pitch_edges:
        biggest_edge = max(
            pitch_edges,
            key=lambda row: (row.get("usage_pct") or 0) + (row.get("whiff_pct") or 0),
        ).get("pitch_type")

    return {
        "metadata": {
            "source_type": "model_projection_workspace_matchup",
            "generated_from": "model_projections._matchup_workspace_analysis",
            "data_confidence": "medium" if arsenal else "low",
            "offense_team_id": offense_team.get("team_id"),
            "offense_team_name": offense_team.get("team_name"),
            "opposing_pitcher_id": opposing_pitcher.get("pitcher_id"),
            "opposing_pitcher_name": opposing_pitcher.get("pitcher_name"),
        },
        "summary": {
            "status": "partial",
            "note": "Model Projections workspace uses production pitcher/team inputs and conservative offense priors.",
            "biggest_edge": biggest_edge,
            "confidence": 0.5 if arsenal else 0.25,
        },
        "plate_discipline_matchup": {
            "offense_k_rate": offense_k,
            "offense_bb_rate": offense_bb,
            "pitcher_k_rate": pitcher_k,
            "pitcher_bb_rate": pitcher_bb,
        },
        "arsenal_matchup": {
            "pitch_edges": pitch_edges,
            "biggest_edge": biggest_edge,
            "pitch_count_used": len(pitch_edges),
        },
    }


def _build_projection_simulation_cards(
    matchup: Dict[str, Any],
    away: Dict[str, Any],
    home: Dict[str, Any],
) -> Dict[str, List[Dict[str, Any]]]:
    away_team_id = away.get("team_id")
    home_team_id = home.get("team_id")
    away_team_name = away.get("team_name")
    home_team_name = home.get("team_name")

    environment_profile = compute_environment_profile({
        "game_pk": matchup.get("game_pk"),
        "game_date": matchup.get("game_date"),
        "venue_name": matchup.get("venue"),
        "weather": _weather_context(matchup.get("weather")),
        "park_factor": matchup.get("park_factor"),
        "home_team": home_team_name,
        "away_team": away_team_name,
    })

    away_pitcher_profile = {}
    home_pitcher_profile = {}

    away_bullpen_profile = build_bullpen_profile(team_id=away_team_id, team_name=away_team_name)
    home_bullpen_profile = build_bullpen_profile(team_id=home_team_id, team_name=home_team_name)

    away_vs_home_starter_pa = _team_offense_prior_pa_model(
        team_id=away_team_id,
        team_name=away_team_name,
        opposing_pitcher_profile=home_pitcher_profile,
        environment_profile=environment_profile,
    )
    home_vs_away_starter_pa = _team_offense_prior_pa_model(
        team_id=home_team_id,
        team_name=home_team_name,
        opposing_pitcher_profile=away_pitcher_profile,
        environment_profile=environment_profile,
    )
    away_vs_home_bullpen_pa = _team_offense_prior_pa_model(
        team_id=away_team_id,
        team_name=away_team_name,
        opposing_pitcher_profile=home_bullpen_profile,
        environment_profile=environment_profile,
    )
    home_vs_away_bullpen_pa = _team_offense_prior_pa_model(
        team_id=home_team_id,
        team_name=home_team_name,
        opposing_pitcher_profile=away_bullpen_profile,
        environment_profile=environment_profile,
    )

    sim = simulate_game_with_bullpen(
        away_starter_probabilities=away_vs_home_starter_pa.get("probabilities") or {},
        home_starter_probabilities=home_vs_away_starter_pa.get("probabilities") or {},
        away_bullpen_probabilities=away_vs_home_bullpen_pa.get("probabilities") or {},
        home_bullpen_probabilities=home_vs_away_bullpen_pa.get("probabilities") or {},
        simulations=3000,
        seed=42,
        innings=9,
        starter_innings=5,
        away_starter_quality=0.0,
        home_starter_quality=0.0,
        dynamic_starter_exit=True,
    )

    total_probs = sim.get("calibrated_total_probabilities") or sim.get("total_probabilities") or {}
    team_total_probs = sim.get("calibrated_team_total_probabilities") or sim.get("team_total_probabilities") or {}

    away_card = _probability_model_card(
        model_name="Simulation: Away Team Run/Win Projection",
        score=sim.get("away_expected_runs"),
        formula="Team offense prior PA probabilities + opponent starter/bullpen profiles + environment + dynamic starter exit",
        inputs={
            "expected_runs": sim.get("away_expected_runs"),
            "raw_expected_runs": sim.get("raw_away_expected_runs"),
            "win_probability": sim.get("away_win_probability"),
            "team_3_plus_runs": team_total_probs.get("away_3_plus"),
            "team_4_plus_runs": team_total_probs.get("away_4_plus"),
            "team_5_plus_runs": team_total_probs.get("away_5_plus"),
            "offense_source": "team_offense_prior",
            "opposing_bullpen_quality": (home_bullpen_profile.get("metadata") or {}).get("bullpen_quality_label"),
            "run_environment_index": (environment_profile.get("run_environment") or {}).get("run_scoring_index"),
        },
        steps=[
            "Build conservative team offense prior because Model Projections is game-level.",
            "Convert offense, opponent starter prior, bullpen prior, and environment into PA probabilities.",
            "Simulate regulation games with starter-to-bullpen transition and calibrated run distribution.",
        ],
        notes=[
            "Uses low-confidence team priors until confirmed lineups and player-level projections are wired into this endpoint.",
            "Raw and calibrated outputs are both retained in the simulation object; this card displays calibrated probabilities where available.",
        ],
        confidence="low",
    )

    home_card = _probability_model_card(
        model_name="Simulation: Home Team Run/Win Projection",
        score=sim.get("home_expected_runs"),
        formula="Team offense prior PA probabilities + opponent starter/bullpen profiles + environment + dynamic starter exit",
        inputs={
            "expected_runs": sim.get("home_expected_runs"),
            "raw_expected_runs": sim.get("raw_home_expected_runs"),
            "win_probability": sim.get("home_win_probability"),
            "team_3_plus_runs": team_total_probs.get("home_3_plus"),
            "team_4_plus_runs": team_total_probs.get("home_4_plus"),
            "team_5_plus_runs": team_total_probs.get("home_5_plus"),
            "offense_source": "team_offense_prior",
            "opposing_bullpen_quality": (away_bullpen_profile.get("metadata") or {}).get("bullpen_quality_label"),
            "run_environment_index": (environment_profile.get("run_environment") or {}).get("run_scoring_index"),
        },
        steps=[
            "Build conservative team offense prior because Model Projections is game-level.",
            "Convert offense, opponent starter prior, bullpen prior, and environment into PA probabilities.",
            "Simulate regulation games with starter-to-bullpen transition and calibrated run distribution.",
        ],
        notes=[
            "Uses low-confidence team priors until confirmed lineups and player-level projections are wired into this endpoint.",
            "Raw and calibrated outputs are both retained in the simulation object; this card displays calibrated probabilities where available.",
        ],
        confidence="low",
    )

    game_total_card = _probability_model_card(
        model_name="Simulation: Game Total Projection",
        score=sim.get("total_expected_runs"),
        formula="Monte Carlo total runs from away/home PA distributions, bullpen priors, environment, and calibrated distribution",
        inputs={
            "total_expected_runs": sim.get("total_expected_runs"),
            "raw_total_expected_runs": sim.get("raw_total_expected_runs"),
            "over_6_5": total_probs.get("over_6.5"),
            "over_7_5": total_probs.get("over_7.5"),
            "over_8_5": total_probs.get("over_8.5"),
            "over_9_5": total_probs.get("over_9.5"),
            "under_7_5": total_probs.get("under_7.5"),
            "under_8_5": total_probs.get("under_8.5"),
            "under_9_5": total_probs.get("under_9.5"),
            "tie_after_regulation": sim.get("tie_after_regulation_probability"),
            "environment_label": (environment_profile.get("run_environment") or {}).get("scoring_environment_label"),
        },
        steps=[
            "Generate PA outcome probabilities for each offense against starter and bullpen contexts.",
            "Run full-game simulation with dynamic starter exit and bullpen transition.",
            "Apply existing game-simulation calibration to expected runs and probability distribution.",
        ],
        notes=[
            "This is the first Model Projections integration of the sandbox simulation engine.",
            "Confidence is low until lineup-level and starter-profile inputs are connected directly into this endpoint.",
        ],
        confidence="low",
    )

    workspace = {
        "environmentProfile": environment_profile,
        "awayPitcherProfile": _pitcher_workspace_profile(away),
        "homePitcherProfile": _pitcher_workspace_profile(home),
        "awayOffenseProfile": _offense_workspace_profile(away),
        "homeOffenseProfile": _offense_workspace_profile(home),
        "awayBullpenProfile": away_bullpen_profile,
        "homeBullpenProfile": home_bullpen_profile,
        "awayPAOutcomeModel": away_vs_home_starter_pa,
        "homePAOutcomeModel": home_vs_away_starter_pa,
        "awayVsHomeBullpenPAOutcomeModel": away_vs_home_bullpen_pa,
        "homeVsAwayBullpenPAOutcomeModel": home_vs_away_bullpen_pa,
        "awayMatchupAnalysis": _matchup_workspace_analysis(away, home),
        "homeMatchupAnalysis": _matchup_workspace_analysis(home, away),
        "bullpenAdjustedGameSimulation": sim,
        "metadata": {
            "workspace_version": "model_projection_workspace_v1",
            "generated_from": "model_projections._build_projection_simulation_cards",
            "data_confidence": "low",
            "notes": [
                "Workspace is generated from production model projection inputs.",
                "Lineup-level detail is not fully wired here yet; team offense priors are used where necessary.",
                "This object is intended to power the full Model Projections workspace UI.",
            ],
        },
    }

    return {
        "away": [away_card, game_total_card],
        "home": [home_card],
        "workspace": workspace,
    }


def _side_context(matchup: Dict[str, Any], side: str, session: Session, season: int) -> Dict[str, Any]:
    pitcher_id = matchup.get(f"{side}_pitcher_id")
    arsenal = matchup.get(f"{side}_pitch_arsenal") or {}
    arsenal_source = "matchup_generator" if arsenal else "missing_pitch_arsenal"
    if not arsenal and pitcher_id:
        records, arsenal_season = get_pitch_arsenal_with_fallback(session, int(pitcher_id), season)
        arsenal = _arsenal_records_to_dict(records)
        arsenal_source = f"pitch_arsenal_fallback_{arsenal_season}" if arsenal else "missing_pitch_arsenal"
    team_id = matchup.get(f"{side}_team_id")
    team_name = matchup.get(f"{side}_team_name")
    ctx = {
        "side": side,
        "team_id": team_id,
        "team_name": team_name,
        "pitcher_id": pitcher_id,
        "pitcher_name": matchup.get(f"{side}_pitcher_name"),
        "pitcher_features": matchup.get(f"{side}_pitcher_features") or {},
        "pitch_arsenal": arsenal,
        "pitch_arsenal_source": arsenal_source,
        "offense_inputs": _team_split_inputs(session, team_id, season),
        "bullpen_inputs": _bullpen_inputs(session, team_id, team_name),
    }
    ctx["models"] = [
        pitching_volatility_score(ctx["pitcher_features"], ctx["pitch_arsenal"]),
        offensive_firepower_score(ctx["offense_inputs"]),
        bullpen_collapse_index(ctx["bullpen_inputs"]),
        pitch_identity_disruption_score(ctx["pitch_arsenal"], hitter_pitch_rows=[]),
    ]
    return ctx


def build_model_projection_payload(session: Session, target_date: str) -> Dict[str, Any]:
    try:
        date_obj = datetime.datetime.strptime(target_date, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError("date must be YYYY-MM-DD") from exc
    matchups = generate_matchups_for_date(session, target_date)
    games: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    for matchup in matchups:
        try:
            away = _side_context(matchup, "away", session, date_obj.year)
            home = _side_context(matchup, "home", session, date_obj.year)

            simulation_cards = _build_projection_simulation_cards(matchup, away, home)
            away["models"].extend(simulation_cards.get("away", []))
            home["models"].extend(simulation_cards.get("home", []))
            workspace = simulation_cards.get("workspace") or {}

            games.append({
                "game_pk": matchup.get("game_pk"),
                "game_date": matchup.get("game_date") or target_date,
                "game_time": matchup.get("game_time"),
                "status": matchup.get("status"),
                "venue": matchup.get("venue"),
                "weather": matchup.get("weather"),
                "away_team": {"id": away.get("team_id"), "name": away.get("team_name")},
                "home_team": {"id": home.get("team_id"), "name": home.get("team_name")},
                "away_pitcher": {"id": away.get("pitcher_id"), "name": away.get("pitcher_name")},
                "home_pitcher": {"id": home.get("pitcher_id"), "name": home.get("pitcher_name")},
                "main_matchup_probabilities": {"away_win_prob": safe_float(matchup.get("away_win_prob")), "home_win_prob": safe_float(matchup.get("home_win_prob"))},
                "teams": {"away": away, "home": home},
                "workspace": workspace,
            })
        except Exception as exc:
            errors.append({"game_pk": matchup.get("game_pk"), "error": str(exc)})
    return {"date": target_date, "count": len(games), "games": games, "errors": errors, "source_notes": ["Daily games are loaded through main generate_matchups_for_date.", "Scores use available real production inputs only.", "Missing inputs are returned explicitly and are not fabricated."]}
