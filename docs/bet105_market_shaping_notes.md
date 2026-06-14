# Bet105 MLB market shaping notes

This document records the current Bet105/KIBL integration boundary.

## Confirmed

- MLB fixtures are discovered from `info/fixtures` using `sport_id=2`, `league_id=7`.
- Fixture calls must not include `feed_source_id` or `betting_type_id`.
- Selected MLB game fixtures are `fixture_type_id=1` and must be filtered to the selected Eastern slate date.
- KIBL market rows, when present, can be normalized generically using:
  - `fixture_id`
  - `market_id`
  - `market_type_id`
  - `fixture_participant_id`
  - `participant_id`
  - `price_american`
  - `price_decimal`
  - `price_fraction`
  - `info.side`
  - `info.line_id`
  - `info.contestant_id`

## Market classification

- `market_type_id=1` -> Moneyline / `h2h`
- `market_type_id=2` -> Run Line / `spreads`
- `market_type_id=3` -> Total Runs / `totals`
- `market_type_id=0` -> Other Market / `other`

## Current remaining unknown

The remaining unknown is the exact KIBL `info/markets` request body that returns Bet105 MLB market rows for the selected MLB fixture IDs under:

```text
sport_id=2
league_id=7
feed_source_id=171
betting_type_id=1
```

The debug script `scripts/debug_bet105_mlb_markets.py` probes only selected MLB fixture IDs with strict request caps. Once it identifies a winner request body, that body should be promoted into `KiblBet105Repository.market_request_bodies`.
