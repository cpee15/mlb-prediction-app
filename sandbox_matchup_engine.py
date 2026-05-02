            home_live_statcast_source = "official_lineup" if home_lineup else "roster_fallback"
            away_live_statcast_source = "official_lineup" if away_lineup else "roster_fallback"

            home_projected_lineup_offense_profile = _enrich_lineup_offense_profile_with_live_statcast(
                home_projected_lineup_offense_profile,
                home_live_statcast_candidates,
                home_live_statcast_source,
            )
            away_projected_lineup_offense_profile = _enrich_lineup_offense_profile_with_live_statcast(
                away_projected_lineup_offense_profile,
                away_live_statcast_candidates,
                away_live_statcast_source,
            )

            def _average_probability_dict(models):
                totals = {}
                count = 0
                for model in models or []:
                    probs = model.get("probabilities") or {}
                    if not probs:
                        continue
                    count += 1
                    for key, value in probs.items():
                        if value is None:
                            continue
                        totals[key] = totals.get(key, 0.0) + float(value)
                if not count:
                    return {}
                return {key: round(value / count, 4) for key, value in sorted(totals.items())}

            def _average_summary_dict(models):
                totals = {}
                count = 0
                for model in models or []:
                    summary = model.get("summary") or {}
                    if not summary:
                        continue
                    count += 1
                    for key, value in summary.items():
                        if value is None:
                            continue
                        totals[key] = totals.get(key, 0.0) + float(value)
                if not count:
                    return {}
                return {key: round(value / count, 4) for key, value in sorted(totals.items())}

            def _build_lineup_pa_outcome_model(lineup, lineup_profile, opposing_pitcher_profile, environment_profile, side_label):
                candidates = lineup or []
                player_models = []

                # V1 uses the lineup-level offense profile for each hitter because
                # the current matchup endpoint exposes team/lineup aggregate profiles.
                # Later versions can replace this with true player-level profiles.
                for player in candidates:
                    model = build_pa_outcome_probabilities(
                        batter_profile=lineup_profile,
                        pitcher_profile=opposing_pitcher_profile,
                        environment_profile=environment_profile,
                    )
                    player_models.append({
                        "player_id": player.get("id"),
                        "player_name": player.get("name"),
                        "probabilities": model.get("probabilities"),
                        "summary": model.get("summary"),
                    })

                lineup_average = _average_probability_dict(player_models)
                lineup_summary = _average_summary_dict(player_models)
                return {
                    "model_version": "lineup_pa_outcome_v1",
                    "side": side_label,
                    "player_count_used": len(player_models),
                    "lineup_average_probabilities": lineup_average,
                    "lineup_average_summary": lineup_summary,
                    "player_outcomes": player_models,
                    "metadata": {
                        "generated_from": "matchup_detail.pa_outcome_integration",
                        "batter_profile_granularity": "lineup_aggregate",
                        "pitcher_profile_granularity": "starter_profile",
                        "environment_profile_used": bool(environment_profile),
                    },
                }

            def _build_half_inning_simulation(pa_model, side_label):
                probabilities = (pa_model or {}).get("lineup_average_probabilities") or {}
                if not probabilities:
                    return {
                        "model_version": "half_inning_sim_v1",
                        "side": side_label,
                        "status": "missing_pa_probabilities",
                    }

                result = simulate_half_innings(
                    probabilities=probabilities,
                    simulations=5000,
                    seed=42,
                )
                result["side"] = side_label
                result["metadata"] = {
                    "generated_from": "matchup_detail.half_inning_simulation",
                    "pa_model_version": (pa_model or {}).get("model_version"),
                    "simulation_seed": 42,
                    "simulation_count": 5000,
                }
                return result

            def _build_bullpen_pa_outcome_model(lineup_profile, opposing_bullpen_profile, environment_profile, side_label):
                model = build_pa_outcome_probabilities(
                    batter_profile=lineup_profile,
                    pitcher_profile=opposing_bullpen_profile,
                    environment_profile=environment_profile,
                )
                return {
                    "model_version": "bullpen_pa_outcome_v1",
                    "side": side_label,
                    "lineup_average_probabilities": model.get("probabilities"),
                    "lineup_average_summary": model.get("summary"),
                    "metadata": {
                        "generated_from": "matchup_detail.bullpen_pa_outcome_model",
                        "batter_profile_granularity": "lineup_aggregate",
                        "pitcher_profile_granularity": "team_bullpen_profile",
                        "environment_profile_used": bool(environment_profile),
                    },
                }

            def _build_game_simulation(away_pa_model, home_pa_model):
                away_probabilities = (away_pa_model or {}).get("lineup_average_probabilities") or {}
                home_probabilities = (home_pa_model or {}).get("lineup_average_probabilities") or {}

                if not away_probabilities or not home_probabilities:
                    return {
                        "model_version": "full_game_sim_v1",
                        "status": "missing_pa_probabilities",
                    }

                result = simulate_game(
                    away_probabilities=away_probabilities,
                    home_probabilities=home_probabilities,
                    simulations=5000,
                    seed=42,
                    innings=9,
                )
                result["metadata"] = {
                    **(result.get("metadata") or {}),
                    "generated_from": "matchup_detail.full_game_simulation",
                    "away_pa_model_version": (away_pa_model or {}).get("model_version"),
                    "home_pa_model_version": (home_pa_model or {}).get("model_version"),
                    "simulation_seed": 42,
                    "simulation_count": 5000,
                }
                return result

            def _build_bullpen_adjusted_game_simulation(away_starter_pa_model, home_starter_pa_model, away_bullpen_pa_model, home_bullpen_pa_model):
                away_starter_probabilities = (away_starter_pa_model or {}).get("lineup_average_probabilities") or {}
                home_starter_probabilities = (home_starter_pa_model or {}).get("lineup_average_probabilities") or {}
                away_bullpen_probabilities = (away_bullpen_pa_model or {}).get("lineup_average_probabilities") or {}
                home_bullpen_probabilities = (home_bullpen_pa_model or {}).get("lineup_average_probabilities") or {}

                if not away_starter_probabilities or not home_starter_probabilities or not away_bullpen_probabilities or not home_bullpen_probabilities:
                    return {
                        "model_version": "full_game_sim_with_bullpen_v1",
                        "status": "missing_pa_probabilities",
                    }

                result = simulate_game_with_bullpen(
                    away_starter_probabilities=away_starter_probabilities,
                    home_starter_probabilities=home_starter_probabilities,
                    away_bullpen_probabilities=away_bullpen_probabilities,
                    home_bullpen_probabilities=home_bullpen_probabilities,
                    simulations=5000,
                    seed=42,
                    innings=9,
                    starter_innings=5,
                )
                result["metadata"] = {
                    **(result.get("metadata") or {}),
                    "generated_from": "matchup_detail.bullpen_adjusted_game_simulation",
                    "away_starter_pa_model_version": (away_starter_pa_model or {}).get("model_version"),
                    "home_starter_pa_model_version": (home_starter_pa_model or {}).get("model_version"),
                    "away_bullpen_pa_model_version": (away_bullpen_pa_model or {}).get("model_version"),
                    "home_bullpen_pa_model_version": (home_bullpen_pa_model or {}).get("model_version"),
                }
                return result

            def _profile_has_useful_offense_metrics(profile):
                for section in ["contact_skill", "plate_discipline", "power", "platoon_profile"]:
                    values = (profile.get(section) or {}).values()
                    if any(value is not None for value in values):
                        return True
                return False

            def _team_split_for_pitcher_hand(team_splits_payload, pitcher_hand):
                team_splits_payload = team_splits_payload or {}
                if pitcher_hand == "L":
                    return team_splits_payload.get("vsL")
                if pitcher_hand == "R":
                    return team_splits_payload.get("vsR")

                # If probable pitcher handedness is unavailable, use the most
                # common/default split as a pragmatic fallback rather than
                # leaving the Batter tab empty.
                return team_splits_payload.get("vsR") or team_splits_payload.get("vsL")

            def _team_split_offense_fallback_profile(
                existing_profile,
                team_splits_payload,
                pitcher_hand,
                lineup_source,
                player_count_used,
            ):
                if _profile_has_useful_offense_metrics(existing_profile):
                    return existing_profile

                split = _team_split_for_pitcher_hand(team_splits_payload, pitcher_hand)
                if not split:
                    return existing_profile

                avg = split.get("batting_avg")
                slg = split.get("slugging_pct")
                iso = slg - avg if slg is not None and avg is not None else None
                selected_split = "vsL" if pitcher_hand == "L" else "vsR" if pitcher_hand == "R" else "vsR_default"

                return {
                    "metadata": {
                        **(existing_profile.get("metadata") or {}),
                        "source_type": "team_split_fallback",
                        "source_fields_used": sorted(list(split.keys())),
                        "data_confidence": "low",
                        "generated_from": "matchup_detail.team_splits_fallback",
                        "profile_granularity": "team_split_proxy",
                        "is_projected_lineup_derived": False,
                        "lineup_source": lineup_source,
                        "opposing_pitcher_hand": pitcher_hand if pitcher_hand in {"L", "R"} else "unknown",
                        "player_count_used": player_count_used,
                        "selected_team_split": selected_split,
                        "sample_window": "current_season",
                        "sample_family": "team_split",
                        "sample_description": "Team split fallback used because player-level lineup splits were unavailable",
                        "sample_size": split.get("pa"),
                        "sample_blend_policy": "team_split_fallback_v1",
                        "stabilizer_window": "current_season",
                    },
                    "contact_skill": {
                        "k_rate": split.get("k_pct"),
                        "whiff_rate": None,
                        "contact_rate": None,
                    },
                    "plate_discipline": {
                        "bb_rate": split.get("bb_pct"),
                        "chase_rate": None,
                        "swing_rate": None,
                    },
                    "power": {
                        "iso": iso,
                        "barrel_rate": None,
                        "hard_hit_rate": None,
                    },
                    "batted_ball_quality": {
                        "avg_exit_velocity": None,
                        "avg_launch_angle": None,
                    },
                    "platoon_profile": {
                        "vs_lhp_woba": None,
                        "vs_rhp_woba": None,
                        "vs_lhp_iso": iso if pitcher_hand == "L" else None,
                        "vs_rhp_iso": iso if pitcher_hand == "R" else None,
                    },
                }

            home_projected_lineup_offense_profile = _team_split_offense_fallback_profile(
                existing_profile=home_projected_lineup_offense_profile,
                team_splits_payload=home_team_splits,
                pitcher_hand=away_pitcher_hand,
                lineup_source="official" if home_lineup else "missing",
                player_count_used=len(home_lineup),
            )
            away_projected_lineup_offense_profile = _team_split_offense_fallback_profile(
                existing_profile=away_projected_lineup_offense_profile,
                team_splits_payload=away_team_splits,
                pitcher_hand=home_pitcher_hand,
                lineup_source="official" if away_lineup else "missing",
                player_count_used=len(away_lineup),
            )

            environment_profile = compute_environment_profile(
                {
                    "game_pk": game_pk,
                    "game_date": game_date_iso,
                    "venue_name": venue_name,
                    "weather": _extract_weather(game),
                    "park_factor": get_park_factor(venue_name),
                    "home_team": home.get("team", {}).get("name"),
                    "away_team": away.get("team", {}).get("name"),
                }
            )

            home_matchup_analysis = build_matchup_analysis(
                pitcher_id=away_pitcher_id,
                pitcher_name=away.get("probablePitcher", {}).get("fullName"),
                pitcher_hand=away_pitcher_hand,
                lineup=home_lineup,
                lineup_source="official" if home_lineup else "missing",
                arsenal_rows=away_pitcher_detail.get("arsenal") or [],
            )
            away_matchup_analysis = build_matchup_analysis(
                pitcher_id=home_pitcher_id,
                pitcher_name=home.get("probablePitcher", {}).get("fullName"),
                pitcher_hand=home_pitcher_hand,
                lineup=away_lineup,
                lineup_source="official" if away_lineup else "missing",
                arsenal_rows=home_pitcher_detail.get("arsenal") or [],
            )

            home_pa_outcome_model = _build_lineup_pa_outcome_model(
                lineup=home_lineup,
                lineup_profile=home_projected_lineup_offense_profile,
                opposing_pitcher_profile=away_pitcher_profile,
                environment_profile=environment_profile,
                side_label="home_offense",
            )
            away_pa_outcome_model = _build_lineup_pa_outcome_model(
                lineup=away_lineup,
                lineup_profile=away_projected_lineup_offense_profile,
                opposing_pitcher_profile=home_pitcher_profile,
                environment_profile=environment_profile,
                side_label="away_offense",
            )

            home_half_inning_simulation = _build_half_inning_simulation(
                home_pa_outcome_model,
                side_label="home_offense",
            )
            away_half_inning_simulation = _build_half_inning_simulation(
                away_pa_outcome_model,
                side_label="away_offense",
            )

            home_bullpen_profile = build_bullpen_profile(
                team_id=home.get("id"),
                team_name=home.get("name"),
            )
            away_bullpen_profile = build_bullpen_profile(
                team_id=away.get("id"),
                team_name=away.get("name"),
            )

            away_vs_home_bullpen_pa_outcome_model = _build_bullpen_pa_outcome_model(
                lineup_profile=away_projected_lineup_offense_profile,
                opposing_bullpen_profile=home_bullpen_profile,
                environment_profile=environment_profile,
                side_label="away_offense_vs_home_bullpen",
            )
            home_vs_away_bullpen_pa_outcome_model = _build_bullpen_pa_outcome_model(
                lineup_profile=home_projected_lineup_offense_profile,
                opposing_bullpen_profile=away_bullpen_profile,
                environment_profile=environment_profile,
                side_label="home_offense_vs_away_bullpen",
            )

            game_simulation = _build_game_simulation(
                away_pa_model=away_pa_outcome_model,
                home_pa_model=home_pa_outcome_model,
            )
            bullpen_adjusted_game_simulation = _build_bullpen_adjusted_game_simulation(
                away_starter_pa_model=away_pa_outcome_model,
                home_starter_pa_model=home_pa_outcome_model,
                away_bullpen_pa_model=away_vs_home_bullpen_pa_outcome_model,
                home_bullpen_pa_model=home_vs_away_bullpen_pa_outcome_model,
            )

            return {
                "game_pk": game_pk,
                "game_date": game_date_iso,
                "venue": venue_name,
                "status": game.get("status", {}).get("detailedState"),
                "weather": _extract_weather(game),
                "park_factor": get_park_factor(venue_name),
                "home_win_prob": home_win_prob,
                "away_win_prob": away_win_prob,
                "homePitcherProfile": home_pitcher_profile,
                "awayPitcherProfile": away_pitcher_profile,
                "homeProjectedLineupOffenseProfile": home_projected_lineup_offense_profile,
                "awayProjectedLineupOffenseProfile": away_projected_lineup_offense_profile,
                "environmentProfile": environment_profile,
                "homeMatchupAnalysis": home_matchup_analysis,
                "awayMatchupAnalysis": away_matchup_analysis,
                "homePAOutcomeModel": home_pa_outcome_model,
                "awayPAOutcomeModel": away_pa_outcome_model,
                "homeHalfInningSimulation": home_half_inning_simulation,
                "awayHalfInningSimulation": away_half_inning_simulation,
                "gameSimulation": game_simulation,
                "bullpenAdjustedGameSimulation": bullpen_adjusted_game_simulation,
                "awayVsHomeBullpenPAOutcomeModel": away_vs_home_bullpen_pa_outcome_model,
                "homeVsAwayBullpenPAOutcomeModel": home_vs_away_bullpen_pa_outcome_model,
                "homeBullpenProfile": home_bullpen_profile,
                "awayBullpenProfile": away_bullpen_profile,
                "home_team": {
                    "id": home_team_id,
                    "name": home.get("team", {}).get("name"),
                    "record": f"{home_record.get('wins',0)}-{home_record.get('losses',0)}" if home_record else None,
                    "pitcher_id": home_pitcher_id,
                    "pitcher_name": home.get("probablePitcher", {}).get("fullName"),
                    **home_pitcher_detail,
                    "splits": home_team_splits,
                    "lineup": home_lineup,
                },
                "away_team": {
                    "id": away_team_id,
                    "name": away.get("team", {}).get("name"),
                    "record": f"{away_record.get('wins',0)}-{away_record.get('losses',0)}" if away_record else None,
                    "pitcher_id": away_pitcher_id,
                    "pitcher_name": away.get("probablePitcher", {}).get("fullName"),
                    **away_pitcher_detail,
                    "splits": away_team_splits,
                    "lineup": away_lineup,
                },
            }

    @app.get("/matchup/{game_pk}/competitive")
    def get_competitive_analysis(game_pk: int) -> Dict[str, Any]:
        url = f"{MLB_STATS_BASE}/schedule"
        params = {
            "gamePk": game_pk,
            "hydrate": "probablePitcher,team,lineups,weather",
        }
        try:
