# Model Tracker production pass

## Purpose

This pass turns `/model-tracker` from a long development-style tracker into a production-facing game dashboard.

## UX changes

- Games are now the primary unit on the Plays tab.
- Each game renders as a compact accordion row.
- Opening a game splits outputs into:
  - Recommendations
  - Leans
  - Low Confidence / No Play
- The global recommended and lean card sections were removed from the top of the page to prevent duplicated rows and endless scrolling.
- Highest Confidence Plays of the Day now appears below the game accordions.
- The Details tab replaces the old audit-style language with a production table view.

## Results analytics

The Results tab now uses period windows instead of comparing current rows to an empty previous set.

Supported periods:

- DoD
- WoW
- MoM
- Rolling 7
- Rolling 30
- Season

The selected period drives:

- Summary cards
- Current vs Previous P&L chart
- Period cumulative trend chart with previous-period overlay
- Win Rate / ROI comparison chart
- Period-scoped breakdown charts by output type, market, confidence, and edge
- Compact period detail ledger

## Helper coverage

`frontend/src/lib/modelTrackerPnl.mjs` now owns reusable logic for:

- Production bucket labels
- Game grouping
- Highest-confidence ranking
- Period windows and date keys
- Period comparison construction

`frontend/src/lib/modelTrackerPnl.test.mjs` covers the new grouping, ranking, and period window behavior.

## Production copy rules

The UI avoids user-facing sandbox/testing/debug language. Controls are written in user terms such as `Refresh Model Plays`, `Results`, `P&L`, `Quality`, and `Details`.

## Verification

Run from `frontend/`:

```bash
npm test
npm run build
```

After merge and deploy, verify:

- `/model-tracker` loads the selected date.
- Games render as compact accordions.
- DoD/WoW/MoM selection changes summary cards and charts.
- Previous-period chart overlays appear when prior rows exist.
- The page does not expose development copy to production users.
