# KIBL Bet105 odds integration

This app treats Bet105 as a real sportsbook odds provider, not as a mock book. The existing Daily Odds API continues to use the normalized odds contract already consumed by the frontend and modeling layer. When `ODDS_PROVIDER=kibl_bet105`, `fetch_draftkings_events()` delegates to the KIBL provider so the current `/daily-odds/models` endpoint receives Bet105 prices without a frontend rewrite.

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

## Main endpoint

Use the existing endpoint:

```text
GET /daily-odds/models?date=YYYY-MM-DD&include_unified=true
```

With `ODDS_PROVIDER=kibl_bet105`, this endpoint returns Bet105 odds in the same normalized shape used by the existing DraftKings/Odds API flow:

- `provider: "kibl_bet105"`
- `book: "Bet105"`
- `events[]`
- `markets[]`
- `selections[]`
- `odds.american`
- `odds.decimal`
- `odds.implied_probability`

## Smoke test in Railway shell

```bash
python - <<'PY'
from mlb_app.kibl_bet105_provider import fetch_kibl_bet105_events
print(fetch_kibl_bet105_events(date="2026-06-12", raw=False))
PY
```

Expected result:

- `status` is `ok` or `empty`.
- `provider` is `kibl_bet105`.
- No credentials or bearer tokens appear in `request_params`, `errors`, or logs.

## Product decision

Do not create a fake/mock sportsbook from Bet105 data. Normalize Bet105 as a real provider and overlay it beside the model output. That lets the app compare model probability against Bet105 implied probability and later compare Bet105 against DraftKings or other books for market-shopping and edge detection.
