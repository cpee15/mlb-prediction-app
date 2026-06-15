# Model Tracker UI recommendations and table visibility prompt

Issue: #801

## Goal

Build the next Model Tracker UI PR.

The audit page is directionally fine, but the current experience is too raw. Too many rejected/no-bet/ungraded rows with little useful data are mixed into the same card stream as actionable outputs. Users should not have to dig through horizontal cards to find useful plays, confidence, score, edge, price, and model context.

Make the page more human-friendly and decision-oriented while preserving the full audit data surface.

## Primary file

- `frontend/src/pages/ModelTrackerPage.jsx`

## Current behavior to preserve

The page currently:

- calls `GET /model-tracker?date=YYYY-MM-DD`
- supports Refresh Snapshot and Refresh Results
- parses JSON fields from rows
- filters by source, grade, game, and search
- renders summary stat cards
- renders Game Grouped Tracker cards
- keeps the flip-to-analyst-view card behavior
- renders a full raw table with these fields:
  - date
  - source
  - game
  - type
  - pick
  - player/team
  - model
  - score
  - confidence
  - line
  - price
  - status
  - grade
  - reason

Do not remove the raw audit capability.

## Core product problem

The issue is not that the page lacks data. The issue is that the useful data is buried.

The UI must answer quickly:

1. What are the best plays or leans?
2. Which rows have confidence above `.50` and `.55`?
3. Which rows have price/line/edge/EV?
4. Which rows are rejected/no-bet and why?
5. Which rows are missing data or need result mapping?
6. What does the analyst view say in plain English?
7. Can I still audit every raw model row if needed?

## Required UI changes

### 1. Add an actionable recommendations layer

Create a top-level section before the Game Grouped Tracker:

`Recommended / High Confidence Plays`

This section should include rows/cards where at least one of these is true:

- `confidence >= 0.55`
- `score >= 0.55`
- `model_probability >= 0.55`
- positive `edge`
- positive `expected_value`
- daily odds status is recommended / strong / actionable if available

Also support a secondary threshold view for:

- `confidence >= 0.50`
- `score >= 0.50`
- `model_probability >= 0.50`

Make the output human-readable:

- Primary badge: `Recommended`, `Lean`, `Watchlist`, `Rejected`, `Needs price`, `Needs result mapping`
- Confidence shown consistently as either `.55` style or percentage, but do not mix formats
- Edge and EV shown clearly when present
- Price and line shown clearly when present
- Missing price shown as `Price unavailable`, not blank or generic `Unavailable`

### 2. Separate rejected / no-data rows from actionable rows

Do not delete rejected rows. Do not fake data. But rejected/no-data rows should not dominate the primary experience.

Create buckets:

- `Recommended Plays`
- `Lean / Watchlist`
- `Rejected / No Bet`
- `Missing Data / Ungraded`

Rules:

- Rows with recommendation status like `no_bet`, `rejected`, `suppressed`, or `non_positive_edge` should not appear in the primary recommended section unless they also have a clearly positive score/confidence/edge that meets threshold.
- Rows with no pick, no price, no confidence, no score, and no player/team should go into `Missing Data / Ungraded`.
- Rows with `grade='ungraded'` or `watchlist_only` can appear in Watchlist when they have useful player/prop/model metrics.
- Rejected rows should be accessible lower on the page or behind a collapsed section, not hidden.

### 3. Keep analyst flip but make the card front useful

Keep the flip.

But the front of each card must expose the important data without requiring the flip:

- Pick / player / team
- Game
- Market
- Recommendation status
- Confidence
- Score
- Model probability
- Market implied probability
- Edge
- EV
- Price
- Line
- Grade/result status
- Primary reason summarized in one readable sentence

The card back/analyst view should be organized as:

- Why this row exists
- Source/model/component that created it
- Data supporting the row
- Missing data
- Gradeability status
- Next action: price needed, result mapping needed, wait for final, etc.

### 4. Add deterministic frontend helpers

Add pure helpers inside the page or a small local helper module if cleaner:

- `numericScore(row)`
- `recommendationBucket(row)`
- `recommendationLabel(row)`
- `hasUsefulMetrics(row)`
- `formatProbability(value)`
- `formatAmericanPrice(value)`
- `formatEdge(value)`
- `rowHasPrice(row)`
- `missingDataReasons(row)`
- `analystSummary(row)`
- `isRejectedNoBet(row)`
- `isHighConfidence(row)`
- `isLean(row)`

These helpers must be deterministic and easy to unit test later.

### 5. Improve filters

Keep existing filters:

- source
- grade
- game
- search

Add filters:

- Bucket: all / recommended / lean / watchlist / rejected / missing data
- Minimum confidence: all / 0.50+ / 0.55+ / 0.60+
- Has price: all / has price / missing price
- Has edge: all / positive edge / no edge

### 6. Table View implementation plan

Do not reduce table data. Improve it by splitting the table into human-friendly views while keeping raw audit access.

Replace the single overwhelming table area with three organized tables or tabbed sections.

#### A. Play Summary Table

Purpose: fast decision view.

Columns:

- Bucket
- Pick / Player / Team
- Game
- Market
- Confidence
- Score
- Model Prob
- Market Prob
- Edge
- EV
- Line
- Price
- Status
- Grade
- Reason summary

#### B. Data Quality / Rejected Table

Purpose: show why rows are missing, rejected, or not actionable.

Columns:

- Bucket
- Pick / Player / Team
- Source
- Component
- Game
- Recommendation status
- Rejection reason
- Missing fields
- Gradeability status
- Missing inputs
- Primary reason

#### C. Raw Audit Table

Purpose: preserve all existing data and avoid hiding anything.

Keep every existing raw field and add useful debug columns:

- tracker_key short hash
- source_component
- model_version
- raw price fields if present
- daily odds diagnostics status if present

The raw audit table can be collapsed by default or placed behind a `Raw Audit` tab, but it must remain available.

### 7. Price action compatibility

If PR #800 or later backend price fields are available, support them gracefully without breaking when absent.

Show, when present:

- current/latest provider price
- Bet105 price
- first-seen price
- best price seen
- CLV / price move placeholder
- price snapshot count

When absent, show explicit text:

- `Price unavailable`
- `Price snapshots not loaded`
- `No provider match yet`

Do not leave price cells blank.

This UI PR should not require PR #800 to be merged, but it should be compatible with the eventual fields.

### 8. Duplicate/source-vote compatibility

Support future duplicate/source-vote fields gracefully if present:

- source vote count
- duplicate warning badge
- grouped duplicate count

Do not hide unique rows because they look similar.

## Suggested implementation sequence

1. Add helper functions for row quality, recommendation buckets, and formatting.
2. Compute enriched row objects in `useMemo`, adding:
   - `ui_bucket`
   - `ui_label`
   - `ui_score`
   - `ui_missing_reasons`
   - `ui_has_price`
   - `ui_is_rejected`
   - `ui_is_high_confidence`
3. Add new filter states for bucket, confidence threshold, price, and edge.
4. Add a `Recommended / High Confidence Plays` section before Game Grouped Tracker.
5. Update `TrackerRowCard` front side to show the important metrics directly.
6. Rewrite the back side as an organized analyst readout.
7. Replace the single Table View with:
   - Play Summary Table
   - Data Quality / Rejected Table
   - Raw Audit Table
8. Keep horizontal scroll where necessary, but use readable wrapped cells for reasons and missing-data text.
9. Add empty states explaining why no rows match a bucket/filter.

## Tests / verification

Add or recommend tests for:

- recommendation bucket classification
- `.50` and `.55` threshold classification
- rejected/no-bet rows do not dominate recommended section
- missing price rows show explicit missing-price text
- table split preserves every row in at least one table
- raw audit table still exposes all existing fields
- card front shows useful metrics without flipping
- analyst flip renders organized explanatory text

## Hard rules

- Do not fake picks.
- Do not fake prices.
- Do not delete rejected rows.
- Do not reduce total available table data.
- Do not push directly to `main`.
- Use a branch and PR.
- Keep this frontend-only unless a tiny backend field exposure is absolutely necessary and safe.

## Acceptance criteria

- The page immediately shows recommended/high-confidence plays without making the user dig through every card.
- Confidence >= `.50` and >= `.55` rows are easy to find.
- Rejected/no-data rows are still accessible but visually separated.
- Card fronts are useful without flipping.
- Analyst flip is organized and human-readable.
- Table View is split into summary/data-quality/raw-audit views without hiding existing information.
- Missing price/data is explicit.
- UI is ready to consume Bet105/CLV fields when present.

## Final PR body should include

- Exact UI sections added
- Exact filters added
- Table View split description
- How rejected/no-bet rows are handled
- How `.50` and `.55` thresholds are handled
- How missing prices are displayed
- Confirmation that raw audit data remains available
