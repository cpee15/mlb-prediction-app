import pytest

from mlb_app.simulation.shadow import (
    CANONICAL_CATCHER_BASERUNNING_MATERIALIZATION_VERSION,
    CANONICAL_RUNNER_BASERUNNING_MATERIALIZATION_VERSION,
    CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION,
    CanonicalCatcherBaserunningContext,
    CanonicalRunnerBaserunningContext,
    CanonicalStatcastRunnerBaserunningCounts,
    CanonicalStatcastPitcherBaserunningCounts,
    CanonicalStatcastCatcherBaserunningCounts,
    aggregate_statcast_runner_baserunning_counts,
    aggregate_statcast_pitcher_baserunning_counts,
    aggregate_statcast_catcher_baserunning_counts,
    decode_statcast_baserunning_outcomes,
    materialize_statcast_catcher_observations,
    materialize_statcast_runner_observations,
)


def row(**overrides):
    value = {
        "game_pk": 778462,
        "at_bat_number": 17,
        "pitch_number": 5,
        "pitcher": 686613,
        "fielder_2": 605170,
        "on_1b": 650489,
        "on_2b": None,
        "on_3b": None,
        "des": (
            "Ryan Jeffers strikes out swinging. "
            "Willi Castro steals (1) 2nd base."
        ),
    }
    value.update(overrides)
    return value


def test_decodes_stolen_base_with_exact_identities():
    outcomes = decode_statcast_baserunning_outcomes(
        row()
    )

    assert len(outcomes) == 1

    outcome = outcomes[0]

    assert outcome.runner_id == "650489"
    assert outcome.event_type == "stolen_base"
    assert outcome.origin_base == "first"
    assert outcome.target_base == "second"
    assert outcome.game_pk == 778462
    assert outcome.at_bat_number == 17
    assert outcome.pitch_number == 5
    assert outcome.pitcher_id == "686613"
    assert outcome.catcher_id == "605170"


def test_decodes_caught_stealing_from_first():
    outcomes = decode_statcast_baserunning_outcomes(
        row(
            on_1b=663898,
            des=(
                "Cam Smith strikes out swinging and "
                "Brendan Rodgers caught stealing 2nd, "
                "catcher Ryan Jeffers."
            ),
        )
    )

    assert len(outcomes) == 1
    assert outcomes[0].runner_id == "663898"
    assert (
        outcomes[0].event_type
        == "caught_stealing"
    )
    assert outcomes[0].origin_base == "first"
    assert outcomes[0].target_base == "second"


def test_decodes_steal_of_third_from_second():
    outcomes = decode_statcast_baserunning_outcomes(
        row(
            on_1b=None,
            on_2b=592885,
            des=(
                "William Contreras walks. "
                "Christian Yelich steals (2) "
                "3rd base."
            ),
        )
    )

    assert len(outcomes) == 1
    assert outcomes[0].runner_id == "592885"
    assert outcomes[0].origin_base == "second"
    assert outcomes[0].target_base == "third"


def test_decodes_double_steal_as_two_outcomes():
    outcomes = decode_statcast_baserunning_outcomes(
        row(
            on_1b=687363,
            on_2b=691023,
            des=(
                "Masyn Winn strikes out swinging. "
                "Jordan Walker steals (1) 3rd base. "
                "Victor Scott II steals (4) 2nd base."
            ),
        )
    )

    assert tuple(
        (
            outcome.runner_id,
            outcome.origin_base,
            outcome.target_base,
        )
        for outcome in outcomes
    ) == (
        (
            "691023",
            "second",
            "third",
        ),
        (
            "687363",
            "first",
            "second",
        ),
    )


def test_decodes_home_attempt_from_third():
    outcomes = decode_statcast_baserunning_outcomes(
        row(
            on_1b=None,
            on_3b=123456,
            des=(
                "Runner caught stealing home, "
                "catcher to pitcher."
            ),
        )
    )

    assert len(outcomes) == 1
    assert outcomes[0].runner_id == "123456"
    assert (
        outcomes[0].event_type
        == "caught_stealing"
    )
    assert outcomes[0].origin_base == "third"
    assert outcomes[0].target_base == "home"


def test_missing_matching_runner_is_not_fabricated():
    outcomes = decode_statcast_baserunning_outcomes(
        row(
            on_1b=None,
        )
    )

    assert outcomes == ()


def test_non_baserunning_description_returns_empty():
    outcomes = decode_statcast_baserunning_outcomes(
        row(
            des="Batter strikes out swinging.",
        )
    )

    assert outcomes == ()


def test_duplicate_text_is_deduplicated():
    outcomes = decode_statcast_baserunning_outcomes(
        row(
            des=(
                "Runner steals (1) 2nd base. "
                "Runner steals (1) 2nd base."
            ),
        )
    )

    assert len(outcomes) == 1


def test_pandas_missing_value_text_is_not_identity():
    outcomes = decode_statcast_baserunning_outcomes(
        row(
            on_1b="<NA>",
        )
    )

    assert outcomes == ()


def test_non_mapping_input_is_rejected():
    with pytest.raises(
        TypeError,
        match="row must be a mapping",
    ):
        decode_statcast_baserunning_outcomes(
            object()
        )


def test_source_version_is_explicit():
    outcome = decode_statcast_baserunning_outcomes(
        row()
    )[0]

    assert outcome.source_version == (
        CANONICAL_STATCAST_BASERUNNING_SOURCE_VERSION
    )



def test_aggregates_pitch_opportunities_and_outcomes():
    counts = aggregate_statcast_runner_baserunning_counts(
        (
            row(
                game_pk=1,
                at_bat_number=1,
                pitch_number=1,
                on_1b=650489,
                des="Called strike.",
            ),
            row(
                game_pk=1,
                at_bat_number=1,
                pitch_number=2,
                on_1b=650489,
                des=(
                    "Willi Castro steals (1) "
                    "2nd base."
                ),
            ),
            row(
                game_pk=2,
                at_bat_number=1,
                pitch_number=1,
                on_1b=650489,
                des=(
                    "Willi Castro caught stealing "
                    "2nd."
                ),
            ),
        )
    )

    assert len(counts) == 1
    assert counts[0].runner_id == "650489"
    assert counts[0].eligible_opportunities == 3
    assert counts[0].stolen_bases == 1
    assert counts[0].caught_stealing == 1


def test_occupied_target_is_not_eligible():
    counts = aggregate_statcast_runner_baserunning_counts(
        (
            row(
                game_pk=1,
                at_bat_number=1,
                pitch_number=1,
                on_1b=111,
                on_2b=222,
                on_3b=None,
                des="Called strike.",
            ),
        )
    )

    assert tuple(
        (
            value.runner_id,
            value.eligible_opportunities,
        )
        for value in counts
    ) == (
        (
            "222",
            1,
        ),
    )


def test_double_steal_credits_both_runners():
    counts = aggregate_statcast_runner_baserunning_counts(
        (
            row(
                game_pk=1,
                at_bat_number=1,
                pitch_number=1,
                on_1b=111,
                on_2b=222,
                on_3b=None,
                des=(
                    "Lead Runner steals (1) "
                    "3rd base. Trail Runner steals "
                    "(1) 2nd base."
                ),
            ),
        )
    )

    assert tuple(
        (
            value.runner_id,
            value.eligible_opportunities,
            value.stolen_bases,
        )
        for value in counts
    ) == (
        (
            "111",
            1,
            1,
        ),
        (
            "222",
            1,
            1,
        ),
    )


def test_duplicate_pitch_rows_are_ignored():
    duplicate = row(
        game_pk=1,
        at_bat_number=1,
        pitch_number=1,
        on_1b=650489,
        des=(
            "Willi Castro steals (1) "
            "2nd base."
        ),
    )

    counts = aggregate_statcast_runner_baserunning_counts(
        (
            duplicate,
            dict(duplicate),
        )
    )

    assert counts[0].eligible_opportunities == 1
    assert counts[0].stolen_bases == 1


def test_home_attempt_is_not_in_supported_counts():
    counts = aggregate_statcast_runner_baserunning_counts(
        (
            row(
                game_pk=1,
                at_bat_number=1,
                pitch_number=1,
                on_1b=None,
                on_2b=None,
                on_3b=333,
                des=(
                    "Runner caught stealing home."
                ),
            ),
        )
    )

    assert counts == ()


def test_aggregate_order_is_deterministic():
    counts = aggregate_statcast_runner_baserunning_counts(
        (
            row(
                game_pk=1,
                at_bat_number=1,
                pitch_number=1,
                on_1b=222,
                des="Called strike.",
            ),
            row(
                game_pk=2,
                at_bat_number=1,
                pitch_number=1,
                on_1b=111,
                des="Called strike.",
            ),
        )
    )

    assert tuple(
        value.runner_id
        for value in counts
    ) == (
        "111",
        "222",
    )


def test_aggregate_requires_stable_pitch_identity():
    with pytest.raises(
        ValueError,
        match=(
            "Statcast row requires game_pk, "
            "at_bat_number, and pitch_number"
        ),
    ):
        aggregate_statcast_runner_baserunning_counts(
            (
                row(
                    game_pk=None,
                ),
            )
        )


def test_aggregate_rejects_non_mapping_rows():
    with pytest.raises(
        TypeError,
        match=(
            "each Statcast row must be a mapping"
        ),
    ):
        aggregate_statcast_runner_baserunning_counts(
            (
                object(),
            )
        )



def runner_counts(
    runner_id="650489",
):
    return CanonicalStatcastRunnerBaserunningCounts(
        runner_id=runner_id,
        eligible_opportunities=20,
        stolen_bases=6,
        caught_stealing=2,
    )


def runner_context(
    runner_id="650489",
):
    return CanonicalRunnerBaserunningContext(
        runner_id=runner_id,
        speed_score=0.90,
        lead_quality=0.80,
        fatigue_index=0.10,
        injury_limit_flag=False,
        context_source_version=(
            "sprint_speed_and_availability_v1"
        ),
    )


def test_materializes_complete_runner_observation():
    observations = (
        materialize_statcast_runner_observations(
            counts=(runner_counts(),),
            contexts=(runner_context(),),
        )
    )

    assert len(observations) == 1

    observation = observations[0]

    assert observation.runner_id == "650489"
    assert observation.eligible_opportunities == 20
    assert observation.stolen_bases == 6
    assert observation.caught_stealing == 2
    assert observation.speed_score == 0.90
    assert observation.lead_quality == 0.80
    assert observation.fatigue_index == 0.10
    assert observation.injury_limit_flag is False
    assert observation.source_version == (
        "canonical_statcast_baserunning_source_v1+"
        "sprint_speed_and_availability_v1"
    )


def test_missing_context_does_not_fabricate_observation():
    observations = (
        materialize_statcast_runner_observations(
            counts=(runner_counts(),),
            contexts=(),
        )
    )

    assert observations == ()


def test_context_without_counts_is_ignored():
    observations = (
        materialize_statcast_runner_observations(
            counts=(),
            contexts=(runner_context(),),
        )
    )

    assert observations == ()


def test_materialization_preserves_count_order():
    observations = (
        materialize_statcast_runner_observations(
            counts=(
                runner_counts("runner-2"),
                runner_counts("runner-1"),
            ),
            contexts=(
                runner_context("runner-1"),
                runner_context("runner-2"),
            ),
        )
    )

    assert tuple(
        value.runner_id
        for value in observations
    ) == (
        "runner-2",
        "runner-1",
    )


def test_duplicate_count_identity_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "runner count identifiers must be unique"
        ),
    ):
        materialize_statcast_runner_observations(
            counts=(
                runner_counts(),
                runner_counts(),
            ),
            contexts=(runner_context(),),
        )


def test_duplicate_context_identity_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "runner context identifiers must be unique"
        ),
    ):
        materialize_statcast_runner_observations(
            counts=(runner_counts(),),
            contexts=(
                runner_context(),
                runner_context(),
            ),
        )


def test_unavailable_context_source_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "context_source_version must identify "
            "an available source"
        ),
    ):
        CanonicalRunnerBaserunningContext(
            runner_id="runner",
            speed_score=0.50,
            lead_quality=0.50,
            fatigue_index=0.50,
        )


def test_context_rate_outside_unit_interval_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "speed_score must be between 0 and 1"
        ),
    ):
        runner_context().__class__(
            runner_id="runner",
            speed_score=1.01,
            lead_quality=0.50,
            fatigue_index=0.10,
            context_source_version="test-v1",
        )


def test_materialization_version_is_explicit():
    assert (
        runner_context().materialization_version
        == CANONICAL_RUNNER_BASERUNNING_MATERIALIZATION_VERSION
    )


def defender_rows():
    return (
        row(
            game_pk=1,
            at_bat_number=1,
            pitch_number=1,
            pitcher=900,
            fielder_2=800,
            on_1b=111,
            on_2b=None,
            des="Called strike.",
        ),
        row(
            game_pk=1,
            at_bat_number=1,
            pitch_number=2,
            pitcher=900,
            fielder_2=800,
            on_1b=111,
            on_2b=None,
            des="Runner steals (1) 2nd base.",
        ),
        row(
            game_pk=2,
            at_bat_number=1,
            pitch_number=1,
            pitcher=900,
            fielder_2=800,
            on_1b=111,
            on_2b=None,
            des="Runner caught stealing 2nd.",
        ),
    )


def test_aggregates_exact_pitcher_exposure():
    counts = (
        aggregate_statcast_pitcher_baserunning_counts(
            defender_rows()
        )
    )

    assert counts == (
        CanonicalStatcastPitcherBaserunningCounts(
            pitcher_id="900",
            eligible_opportunities=3,
            stolen_bases_allowed=1,
            caught_stealing=1,
        ),
    )


def test_aggregates_exact_catcher_exposure():
    counts = (
        aggregate_statcast_catcher_baserunning_counts(
            defender_rows()
        )
    )

    assert counts == (
        CanonicalStatcastCatcherBaserunningCounts(
            catcher_id="800",
            eligible_opportunities=3,
            stolen_bases_allowed=1,
            caught_stealing=1,
        ),
    )


def test_double_steal_counts_two_defender_opportunities():
    value = row(
        game_pk=1,
        at_bat_number=1,
        pitch_number=1,
        pitcher=900,
        fielder_2=800,
        on_1b=111,
        on_2b=222,
        on_3b=None,
        des=(
            "Lead Runner steals (1) 3rd base. "
            "Trail Runner steals (1) 2nd base."
        ),
    )

    pitcher = (
        aggregate_statcast_pitcher_baserunning_counts(
            (value,)
        )[0]
    )
    catcher = (
        aggregate_statcast_catcher_baserunning_counts(
            (value,)
        )[0]
    )

    assert pitcher.eligible_opportunities == 2
    assert pitcher.stolen_bases_allowed == 2
    assert catcher.eligible_opportunities == 2
    assert catcher.stolen_bases_allowed == 2


def test_occupied_target_is_not_defender_opportunity():
    value = row(
        game_pk=1,
        at_bat_number=1,
        pitch_number=1,
        pitcher=900,
        fielder_2=800,
        on_1b=111,
        on_2b=222,
        on_3b=333,
        des="Called strike.",
    )

    assert (
        aggregate_statcast_pitcher_baserunning_counts(
            (value,)
        )[0].eligible_opportunities
        == 0
    )
    assert (
        aggregate_statcast_catcher_baserunning_counts(
            (value,)
        )[0].eligible_opportunities
        == 0
    )


def test_missing_pitcher_identity_is_not_fabricated():
    assert (
        aggregate_statcast_pitcher_baserunning_counts(
            (
                row(
                    pitcher=None,
                    fielder_2=800,
                ),
            )
        )
        == ()
    )


def test_missing_catcher_identity_is_not_fabricated():
    assert (
        aggregate_statcast_catcher_baserunning_counts(
            (
                row(
                    pitcher=900,
                    fielder_2=None,
                ),
            )
        )
        == ()
    )


def test_duplicate_defender_pitch_rows_are_ignored():
    value = row(
        game_pk=1,
        at_bat_number=1,
        pitch_number=1,
        pitcher=900,
        fielder_2=800,
        on_1b=111,
        des="Runner steals (1) 2nd base.",
    )

    pitcher = (
        aggregate_statcast_pitcher_baserunning_counts(
            (
                value,
                dict(value),
            )
        )[0]
    )
    catcher = (
        aggregate_statcast_catcher_baserunning_counts(
            (
                value,
                dict(value),
            )
        )[0]
    )

    assert pitcher.eligible_opportunities == 1
    assert pitcher.stolen_bases_allowed == 1
    assert catcher.eligible_opportunities == 1
    assert catcher.stolen_bases_allowed == 1


def test_defender_aggregate_order_is_deterministic():
    values = (
        row(
            game_pk=1,
            pitcher=999,
            fielder_2=777,
        ),
        row(
            game_pk=2,
            pitcher=111,
            fielder_2=222,
        ),
    )

    assert tuple(
        value.pitcher_id
        for value in (
            aggregate_statcast_pitcher_baserunning_counts(
                values
            )
        )
    ) == (
        "111",
        "999",
    )

    assert tuple(
        value.catcher_id
        for value in (
            aggregate_statcast_catcher_baserunning_counts(
                values
            )
        )
    ) == (
        "222",
        "777",
    )


def test_defender_counts_reject_impossible_attempts():
    with pytest.raises(
        ValueError,
        match=(
            "attempts cannot exceed eligible opportunities"
        ),
    ):
        CanonicalStatcastPitcherBaserunningCounts(
            pitcher_id="pitcher",
            eligible_opportunities=1,
            stolen_bases_allowed=1,
            caught_stealing=1,
        )

    with pytest.raises(
        ValueError,
        match=(
            "attempts cannot exceed eligible opportunities"
        ),
    ):
        CanonicalStatcastCatcherBaserunningCounts(
            catcher_id="catcher",
            eligible_opportunities=1,
            stolen_bases_allowed=1,
            caught_stealing=1,
        )


def catcher_counts(
    catcher_id="800",
):
    return CanonicalStatcastCatcherBaserunningCounts(
        catcher_id=catcher_id,
        eligible_opportunities=20,
        stolen_bases_allowed=6,
        caught_stealing=2,
    )


def catcher_context(
    catcher_id="800",
):
    return CanonicalCatcherBaserunningContext(
        catcher_id=catcher_id,
        team_side="home",
        pop_time_score=0.75,
        context_source_version=(
            "baseball_savant_pop_time_v1"
        ),
    )


def test_materializes_complete_catcher_observation():
    observations = (
        materialize_statcast_catcher_observations(
            counts=(catcher_counts(),),
            contexts=(catcher_context(),),
        )
    )

    assert len(observations) == 1

    observation = observations[0]

    assert observation.catcher_id == "800"
    assert observation.team_side == "home"
    assert observation.steal_attempts_against == 8
    assert observation.caught_stealing == 2
    assert observation.throwing_score == 0.25
    assert observation.pop_time_score == 0.75
    assert observation.source_version == (
        "canonical_statcast_baserunning_source_v1+"
        "baseball_savant_pop_time_v1"
    )


def test_missing_catcher_context_is_not_fabricated():
    assert (
        materialize_statcast_catcher_observations(
            counts=(catcher_counts(),),
            contexts=(),
        )
        == ()
    )


def test_context_without_catcher_counts_is_ignored():
    assert (
        materialize_statcast_catcher_observations(
            counts=(),
            contexts=(catcher_context(),),
        )
        == ()
    )


def test_catcher_materialization_preserves_count_order():
    observations = (
        materialize_statcast_catcher_observations(
            counts=(
                catcher_counts("catcher-2"),
                catcher_counts("catcher-1"),
            ),
            contexts=(
                catcher_context("catcher-1"),
                catcher_context("catcher-2"),
            ),
        )
    )

    assert tuple(
        value.catcher_id
        for value in observations
    ) == (
        "catcher-2",
        "catcher-1",
    )


def test_duplicate_catcher_count_identity_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "catcher count identifiers must be unique"
        ),
    ):
        materialize_statcast_catcher_observations(
            counts=(
                catcher_counts(),
                catcher_counts(),
            ),
            contexts=(catcher_context(),),
        )


def test_duplicate_catcher_context_identity_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "catcher context identifiers must be unique"
        ),
    ):
        materialize_statcast_catcher_observations(
            counts=(catcher_counts(),),
            contexts=(
                catcher_context(),
                catcher_context(),
            ),
        )


def test_unavailable_catcher_context_is_rejected():
    with pytest.raises(
        ValueError,
        match=(
            "context_source_version must identify "
            "an available source"
        ),
    ):
        CanonicalCatcherBaserunningContext(
            catcher_id="catcher",
            team_side="away",
            pop_time_score=0.50,
        )


def test_invalid_catcher_team_side_is_rejected():
    with pytest.raises(
        ValueError,
        match="team_side must be away or home",
    ):
        CanonicalCatcherBaserunningContext(
            catcher_id="catcher",
            team_side="invalid",
            pop_time_score=0.50,
            context_source_version="test-v1",
        )


def test_catcher_pop_time_score_is_validated():
    with pytest.raises(
        ValueError,
        match=(
            "pop_time_score must be between 0 and 1"
        ),
    ):
        CanonicalCatcherBaserunningContext(
            catcher_id="catcher",
            team_side="away",
            pop_time_score=1.01,
            context_source_version="test-v1",
        )


def test_catcher_materialization_version_is_explicit():
    assert (
        catcher_context().materialization_version
        == CANONICAL_CATCHER_BASERUNNING_MATERIALIZATION_VERSION
    )
