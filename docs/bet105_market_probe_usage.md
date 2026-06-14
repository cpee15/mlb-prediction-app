# Bet105 MLB market probe usage

Run this only from a credentialed runtime such as Railway shell:

```bash
python scripts/debug_bet105_mlb_markets.py --date 2026-06-14 --max-fixtures 3 > /tmp/bet105_mlb_markets.json
python - <<'PY'
import json
p=json.load(open('/tmp/bet105_mlb_markets.json'))
print('fixture_count:', p.get('fixture_count'))
print('fixture_ids_probed:', p.get('fixture_ids_probed'))
print('request_count:', p.get('request_count'))
print('winner_count:', p.get('winner_count'))
print(json.dumps(p.get('winners', []), indent=2))
PY
```

If a winner returns rows for selected MLB fixture IDs, promote that exact request body into `KiblBet105Repository.market_request_bodies`.
