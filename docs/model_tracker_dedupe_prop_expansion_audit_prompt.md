# Model Tracker duplicate prevention and individual/prop expansion audit prompt

Issue: #795

## Goal

Run a required audit area for the Model Tracker: duplicate prevention plus individual/player/prop expansion.

The Model Tracker must avoid duplicate model outputs while producing more unique individual/player/prop betting rows. Do not fake props. Do not rewrite existing production model formulas. This is an audit/reporting task first.

## Scope

Primary files:

- `frontend/src/pages/ModelTrackerPage.jsx`
- `mlb_app/model_tracker.py`
- `mlb_app/model_tracker_safe_snapshot.py`
- `mlb_app/model_tracker_routes.py`
- `tests/test_model_tracker_daily_odds.py`

Related sources to inspect only as needed:

- Daily Odds `top_prop_model_candidates`
- Batter vs Arsenal / LayerSix matchup outputs
- Pitcher advanced profile / strikeout lean outputs
- My Dashboard solver top hitters/top pitchers
- Daily odds prop markets if available
- Bet105/DraftKings normalized prop markets if available
- Existing best plays payloads, if any

## Hard rules

1. Do not fake model rows.
2. Do not fake player props.
3. Do not create placeholder grades.
4. Do not reduce the current Table View data surface.
5. Do not rewrite existing model formulas.
6. Do not push directly to `main`.
7. Use a branch and PR.
8. Audit/report first; code implementation only after the audit identifies safe additive changes.

## Required audit areas

### 1. Database-level duplicates

Inspect `ModelTrackerSnapshot.tracker_key` generation.

Confirm:

- `tracker_key` is stable across repeated snapshot refreshes for the same date.
- Running `POST /model-tracker/snapshot` twice for the same date does not create duplicate rows.
- `tracker_key` is not too broad, causing different player props/picks to collapse into one row.
- `tracker_key` is not too narrow, causing the same pick to be inserted multiple times because of tiny raw payload differences.
- The unique constraint on `tracker_key` is actually enforced in production.
- Upsert behavior updates existing rows instead of inserting duplicates.

### 2. Logical duplicate picks

Check for duplicate rows that are technically different `tracker_key`s but represent the same betting idea.

Detect duplicates by these signatures:

- `snapshot_date + source + game_pk + market_type + pick_type + pick_label`
- `snapshot_date + game_pk + player_id + market_type + line`
- `snapshot_date + game_pk + player_name + market_type + line`
- `snapshot_date + game_pk + team_name + market_type + pick_label`
- `snapshot_date + event_id + market_type + selection + line`
- `snapshot_date + source_component + player_id + pick_type`
- `snapshot_date + normalized player name + normalized market + normalized line`

Report:

- Exact duplicate groups.
- Near-duplicate groups.
- Which source created them.
- Whether duplicates come from matchups, daily odds, model projections, my-dashboard solver, AI prompts, or prop candidate extraction.
- Whether frontend Game Grouped Tracker shows the same idea multiple times.
- Whether Table View shows duplicate logical picks.
- Whether duplicate cards differ only by `source_component`/`model_name` but should be grouped.
- Whether duplicate rows should be merged, ranked, or preserved as separate source votes.

### 3. Frontend duplicate handling

Audit `ModelTrackerPage.jsx` and report:

- Whether the UI dedupes cards or simply renders every row.
- Whether game cards can show repeated picks for the same player/market.
- Whether table rows can repeat identical pick ideas.
- Whether filters make duplicates easier or harder to spot.
- Whether the UI should add:
  - duplicate warning badge
  - grouped duplicate count
  - source votes panel
  - best unique props section
  - unique individuals section
  - row hash/debug key column
  - sort by unique player/prop score

### 4. Individual/player prop coverage

We want the tracker to produce more unique individual players and prop bets somewhere.

Audit current row sources and report:

- Which sources currently generate player-level rows.
- Which sources generate team/game-only rows.
- Which sources generate prop/watchlist rows.
- Whether `top_prop_model_candidates` are populated.
- Whether hitter props are captured.
- Whether pitcher props are captured.
- Whether batter-vs-arsenal outputs are converted into trackable player prop candidates.
- Whether pitcher strikeout/outs/earned-runs style candidates are produced.
- Whether total bases, hits, RBI, HR, runs, walks, strikeouts, stolen bases, and pitcher Ks are available from existing model data.
- Whether `player_id` is consistently present.
- Whether player-name-only rows need stable identity resolution.
- Whether sportsbook line/price is present for prop rows.
- Whether prop rows are gradeable today or watchlist-only.
- Whether missing sportsbook line prevents grading.

### 5. Prop generation opportunities

Do not fake props. Only use existing model/source data.

Find safe places to create additive prop candidates from existing data:

- Daily Odds `top_prop_model_candidates`
- Batter vs Arsenal / LayerSix matchup outputs
- Pitcher advanced profile / strikeout lean outputs
- My Dashboard solver top hitters/top pitchers
- Daily odds prop markets if available
- Bet105/DraftKings normalized prop markets if available
- Existing best plays payloads, if any

For each candidate source, report:

- File/function that owns the data.
- Fields available.
- Whether `player_id` exists.
- Whether market type exists.
- Whether line and sportsbook price exist.
- Whether model probability exists.
- Whether confidence/score exists.
- Whether actual result mapping exists.
- Whether it should be gradeable or watchlist-only.
- What tracker row shape should be emitted.

### 6. Required new tracker sections

Recommend whether the frontend should add these sections.

#### A. Unique Individual Watchlist

Purpose: show one row per player per date, deduped across all model sources.

Suggested grouping:

- `snapshot_date + player_id`
- fallback `normalized player_name + team_name`

Fields:

- player
- team
- game
- best source
- top market candidate
- score
- confidence
- reason
- number of source votes
- missing inputs
- gradeability status

#### B. Unique Prop Candidates

Purpose: show one row per player-market-line candidate, deduped and ranked.

Suggested grouping:

- `snapshot_date + game_pk + player_id + market_type + line`

Fields:

- player
- market
- line
- sportsbook price if available
- model probability if available
- market implied probability if available
- edge if available
- expected value if available
- confidence
- source votes
- grade status
- gradeability reason

#### C. Duplicate Review

Purpose: debug and prevent repeated cards/picks.

Suggested grouping:

- logical duplicate signatures above

Fields:

- duplicate signature
- row count
- sources
- row IDs/tracker_keys
- pick labels
- recommendation: merge / preserve / fix key / ignore

### 7. Acceptance criteria

The audit is not complete unless it answers:

- Are duplicate rows currently possible?
- Are duplicate rows currently happening?
- Can repeated snapshot refreshes create duplicates?
- Are duplicate `tracker_key`s prevented at the DB layer?
- Are logical duplicate picks visible in the UI?
- Which source creates the most duplicates?
- Which player/prop rows are currently being produced?
- Why are there not more individual/player props?
- What is the safest additive place to create more individual/player prop rows?
- Which prop rows can be graded now?
- Which prop rows must remain watchlist-only until result mapping exists?

### 8. Tests to add or recommend

- Snapshot idempotency: same date snapshot twice does not increase row count.
- Tracker key stability: same source payload creates same `tracker_key`.
- Logical dedupe utility: duplicate player-market-line rows are detected.
- Prop candidate normalization: player prop candidate includes `player_id`, `player_name`, `market_type`, `pick_label`, `line`, `score`, `confidence`.
- Watchlist-only prop rows do not get fake won/lost grades.
- Gradeable moneyline side rows still grade correctly.
- Frontend duplicate grouping does not hide unique rows.
- Frontend unique prop section renders one row per player-market-line.

### 9. Required final recommendations

Include a specific implementation plan for:

- Preventing duplicate inserts.
- Detecting logical duplicate picks.
- Showing duplicate warnings in the UI.
- Producing more unique individual/player rows.
- Producing more prop candidate rows.
- Keeping non-gradeable props as watchlist-only until actual result mapping exists.

Also include:

- Whether duplicates exist today.
- Whether duplicates are DB duplicates, logical duplicates, or UI duplicates.
- The exact duplicate signatures used to test.
- How many unique individuals are produced.
- How many unique prop candidates are produced.
- Why individual/player prop volume is low.
- The safest next implementation to increase prop volume without faking data.

## Endpoint checks

Use these checks during the audit against local and production as available:

```bash
curl -sS https://mlbgpt.com/model-tracker/health | jq .
curl -sS "https://mlbgpt.com/model-tracker?date=YYYY-MM-DD" | jq '.rows | length'
curl -sS -X POST "https://mlbgpt.com/model-tracker/snapshot?date=YYYY-MM-DD" | jq .
curl -sS "https://mlbgpt.com/model-tracker?date=YYYY-MM-DD" | jq '{rows: (.rows | length), games: (.games | length)}'
curl -sS -X POST "https://mlbgpt.com/model-tracker/results/refresh?date=YYYY-MM-DD" | jq .
```

## Final output format

Produce a markdown audit report with:

1. Executive summary
2. Duplicate findings
3. Current player/prop coverage
4. Source-by-source prop opportunity table
5. Frontend duplicate/visibility findings
6. Database duplicate/key findings
7. Recommended unique individual section
8. Recommended unique prop candidate section
9. Recommended duplicate review section
10. Tests to add
11. Files to change later
12. Safe implementation sequence
13. Final verdict
