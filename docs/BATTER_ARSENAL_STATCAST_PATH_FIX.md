# Batter vs Arsenal DB Aggregation Path Fix

Issue: #124

## Product truth

The Batter vs Arsenal cards must display database-backed Statcast aggregations.

Correct pipeline:

```text
statcast_events
  -> hitter Statcast backfill writes raw batter pitch events
  -> pitcher ETL writes raw pitcher pitch events
  -> scripts/run_hitting_matchups_refresh.py aggregates batter plus pitch_type rows
  -> batter_pitch_type_matchups stores the card-ready aggregation
  -> /matchup/{game_pk}/competitive reads that aggregation
  -> frontend renders Batter vs Arsenal cards
```

## Do not change app.py automatically

`mlb_app/app.py` should be changed manually and surgically only.

## Manual app.py change 1

Inside `_build_competitive_matchup(...)`, the `for pitch in arsenal_list:` loop should not use `_hitter_pitch_type_statcast_summary(...)` as a normal fallback, because that hides whether `batter_pitch_type_matchups` exists.

Replace the current `batter_vs_type` block with this:

```python
        batter_vs_type = _stored_batter_pitch_type_summary(
            session=session,
            batter_id=batter_id,
            opposing_pitcher_id=opposing_pitcher_id,
            pitch_type=pitch_type,
            target_date=target_date,
        )

        if batter_vs_type is None:
            batter_vs_type = {
                "source": "missing_batter_pitch_type_matchups",
                "aggregation_source": "raw_statcast_events",
                "lookup_level": None,
                "requested_opposing_pitcher_id": opposing_pitcher_id,
                "stored_opposing_pitcher_id": None,
                "pitch_type": pitch_type,
                "pitches_seen": 0,
                "swings": 0,
                "whiffs": 0,
                "strikeouts": 0,
                "pa": 0,
                "pa_ended": 0,
                "ab": 0,
                "hits": 0,
                "batting_avg": None,
                "xwoba": None,
                "xba": None,
                "avg_exit_velocity": None,
                "avg_launch_angle": None,
                "whiff_pct": None,
                "k_pct": None,
                "putaway_pct": None,
                "hard_hit_pct": None,
                "sample_size": 0,
            }
```

## Manual app.py change 2

The return dict from `_stored_batter_pitch_type_summary(...)` should include these fields so the API proves it is returning the DB-backed aggregation built from raw Statcast events:

```python
        "source": "batter_pitch_type_matchups",
        "aggregation_source": "raw_statcast_events",
        "lookup_level": "exact_batter_pitcher_pitch_type_date",
        "requested_opposing_pitcher_id": opposing_pitcher_id,
        "stored_opposing_pitcher_id": record.opposing_pitcher_id,
```

## Verification

After deploy, run:

```bash
curl -s "https://YOUR_BACKEND_URL/matchup/824929/competitive" \
  | jq '.. | objects | select(has("batter_vs_type")) | .batter_vs_type | {source, aggregation_source, lookup_level, pitches_seen, swings, whiffs, xwoba, xba}' | head -80
```

Expected source for populated rows:

```text
batter_pitch_type_matchups
```

Expected source for missing aggregation rows:

```text
missing_batter_pitch_type_matchups
```
