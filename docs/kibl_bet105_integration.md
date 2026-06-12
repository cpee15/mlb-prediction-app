# KIBL Bet105 odds integration

This app treats Bet105 as a real sportsbook odds provider, not as a mock book. Bet105 data is normalized into the same odds contract already used by the app, then surfaced in two places:

- `/sportsbook/bet105` for the dedicated Bet105 sportsbook-style board and informational slip calculator.
- `/daily-odds` for the model/edge intelligence layer.

## Railway variables

Set these in Railway environment variables. Do not commit real usernames, passwords, tokens, or Cognito responses.

```env
ODDS_PROVIDER=kibl_bet105
KIBL_USERNAME=...
KIBL_PASSWORD=...
KIBL_COGNITO_REGION=us-west-2
KIBL_COGNITO_CLIENT_ID=3udv7qsqgju8c4riqvk72bqcl
KIBL_BASE_URL=https://api.kibl.io/sports/get
KIBL_FEED_SOURCE_ID=171
KIBL_PREMATCH_BETTING_TYPE_ID=1
KIBL_LIVE_BETTING_TYPE_ID=3
KIBL_TIMEZONE=America/New_York
KIBL_CACHE_TTL_SECONDS=120
KIBL_TIMEOUT_SECONDS=20
```

If KIBL uses a path other than the provider's default candidates, set:

```env
KIBL_ODDS_PATH=odds
```

The provider tries `odds`, `events`, then `lines` under `KIBL_BASE_URL` unless `KIBL_ODDS_PATH` is set.

## Explicit sportsbook endpoints

The feature branch adds a standalone router module at `mlb_app/sportsbook_routes.py`. Wire it in `app.py` with:

```python
from .sportsbook_routes import router as sportsbook_router
app.include_router(sportsbook_router)
```

Routes provided by the router:

```text
GET  /odds/bet105/events?date=YYYY-MM-DD&live=false
GET  /odds/bet105/event/{event_id}/markets
GET  /odds/bet105/event/{event_id}/props
GET  /odds/bet105/debug?date=YYYY-MM-DD
GET  /odds/compare/events?date=YYYY-MM-DD&books=bet105,draftkings
POST /odds/parlay/calculate
```

The explicit Bet105 endpoints do not need `ODDS_PROVIDER=kibl_bet105`. That keeps Bet105 and DraftKings available side-by-side.

## Frontend sportsbook wrapper

Route:

```text
/sportsbook/bet105
```

Page:

```text
frontend/src/pages/Bet105SportsbookPage.jsx
```

The page includes:

- premium dark sportsbook layout
- date picker
- prematch/live toggle
- left game rail
- selected game market board
- grouped markets: Featured, Game Lines, Batter Props, Pitcher Props, Team Props, Innings / Periods, Other Markets
- Bet105 odds buttons
- informational slip calculator
- Bet105 vs DraftKings comparison tab
- mobile responsive shell

The slip is an informational calculator only. It does not execute wagers or send transactions.

## Normalized odds contract

All sportsbook UI expects the normalized shape:

- `provider`
- `book`
- `status`
- `events[]`
- `markets[]`
- `selections[]`
- `price`
- `odds.american`
- `odds.decimal`
- `odds.implied_probability`
- `last_updated`

## Smoke tests

Provider smoke test:

```bash
python - <<'PY'
from mlb_app.kibl_bet105_provider import fetch_kibl_bet105_events
print(fetch_kibl_bet105_events(date="2026-06-12", raw=False))
PY
```

Router smoke test after app wiring:

```bash
curl "$API_BASE/odds/bet105/events?date=2026-06-12"
curl "$API_BASE/odds/compare/events?date=2026-06-12&books=bet105,draftkings"
```

Expected result:

- `status` is `ok` or `empty`.
- `provider` is `kibl_bet105` for Bet105 payloads.
- No credentials or bearer tokens appear in `request_params`, `errors`, API responses, or logs.

## Product decision

Do not create a fake/mock sportsbook from Bet105 data. Normalize Bet105 as a real provider and wrap it in a premium in-app sportsbook page. Use DraftKings as a comparison book and MLBGPT model outputs as the intelligence layer for edge detection.
