from __future__ import annotations

import argparse
import datetime
import json
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional, Tuple


def _request(method: str, url: str, timeout: int = 60) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    started = time.perf_counter()
    req = urllib.request.Request(url=url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as res:
            body = res.read()
            elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
            content_type = res.headers.get("content-type") or ""
            payload: Dict[str, Any]
            if "application/json" in content_type:
                payload = json.loads(body.decode("utf-8")) if body else {}
            else:
                payload = {"raw_preview": body.decode("utf-8", errors="replace")[:500]}
            return payload, {
                "ok": True,
                "status": res.status,
                "elapsed_ms": elapsed_ms,
                "payload_bytes": len(body),
                "x_response_time_ms": res.headers.get("x-response-time-ms"),
                "x_payload_bytes": res.headers.get("x-payload-bytes"),
                "x_cache": res.headers.get("x-cache"),
                "x_probability_source": res.headers.get("x-probability-source"),
            }
    except urllib.error.HTTPError as exc:
        body = exc.read()
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        return {"error_body": body.decode("utf-8", errors="replace")[:1000]}, {
            "ok": False,
            "status": exc.code,
            "elapsed_ms": elapsed_ms,
            "payload_bytes": len(body),
            "error": f"HTTP {exc.code}",
        }
    except Exception as exc:
        elapsed_ms = round((time.perf_counter() - started) * 1000, 3)
        return {}, {
            "ok": False,
            "status": None,
            "elapsed_ms": elapsed_ms,
            "payload_bytes": None,
            "error": f"{exc.__class__.__name__}: {exc}",
        }


def _first_game_pk(calendar_payload: Dict[str, Any]) -> Optional[Any]:
    for key in ("today", "tomorrow", "yesterday"):
        games = calendar_payload.get(key, {}).get("games") if isinstance(calendar_payload.get(key), dict) else None
        if isinstance(games, list) and games:
            return games[0].get("game_pk")
    return None


def _projection_probability_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    games = payload.get("games") if isinstance(payload.get("games"), list) else []
    total = len(games)
    model_projection = 0
    fallback = 0
    missing = 0
    samples: List[Dict[str, Any]] = []
    for game in games:
        if not isinstance(game, dict):
            continue
        probability = game.get("model_projection_probability") or game.get("probability") or {}
        source = probability.get("source") or game.get("probability_source")
        if source == "model_projections":
            model_projection += 1
        elif source:
            fallback += 1
        else:
            missing += 1
        if len(samples) < 5:
            samples.append({
                "game_pk": game.get("game_pk"),
                "source": source,
                "source_path": probability.get("source_path") or game.get("probability_source_path"),
                "home_win_prob": game.get("home_win_prob"),
                "away_win_prob": game.get("away_win_prob"),
                "is_fallback": probability.get("is_fallback") or game.get("probability_is_fallback"),
            })
    return {
        "games": total,
        "model_projection_source_count": model_projection,
        "fallback_or_other_source_count": fallback,
        "missing_source_count": missing,
        "samples": samples,
    }


def _measure_twice(name: str, method: str, url: str, timeout: int) -> Dict[str, Any]:
    cold_payload, cold = _request(method, url, timeout)
    warm_payload, warm = _request(method, url, timeout)
    return {
        "name": name,
        "method": method,
        "url": url,
        "cold": cold,
        "warm": warm,
        "payload_summary": {
            "cold_keys": sorted(list(cold_payload.keys()))[:20] if isinstance(cold_payload, dict) else [],
            "warm_keys": sorted(list(warm_payload.keys()))[:20] if isinstance(warm_payload, dict) else [],
        },
    }


def run(base_url: str, date: Optional[str] = None, timeout: int = 60) -> Dict[str, Any]:
    target_date = date or datetime.date.today().isoformat()
    base = base_url.rstrip("/")
    results: Dict[str, Any] = {
        "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "base_url": base,
        "date": target_date,
        "routes": [],
        "probability_verification": {},
        "frontend_route_stack": {
            "home_page": ["/matchups?date=<date>", "/matchups/calendar/schedule fallback", "/odds/draftkings/events?date=<date>"],
            "calendar_page": ["/matchups/calendar/schedule", "/matchups/calendar/snapshot"],
            "daily_odds_page": ["/matchups?date=<date>", "/matchups/calendar/schedule fallback", "/odds/draftkings/events?date=<date>", "/daily-odds/models?date=<date>", "/models/projections?date=<date>"],
            "model_projections_page": ["/models/projections?date=<date>"],
        },
        "debug": {},
    }

    calendar_url = f"{base}/matchups/calendar/schedule"
    calendar_payload, calendar_meta = _request("GET", calendar_url, timeout)
    calendar_warm_payload, calendar_warm_meta = _request("GET", calendar_url, timeout)
    results["routes"].append({
        "name": "lightweight_calendar",
        "method": "GET",
        "url": calendar_url,
        "cold": calendar_meta,
        "warm": calendar_warm_meta,
        "heavy_matchup_generation": {
            key: value.get("heavy_matchup_generation")
            for key, value in calendar_payload.items()
            if isinstance(value, dict)
        } if isinstance(calendar_payload, dict) else {},
    })

    results["routes"].append(_measure_twice("daily_matchups", "GET", f"{base}/matchups?date={target_date}", timeout))
    results["routes"].append(_measure_twice("draftkings_events", "GET", f"{base}/odds/draftkings/events?date={target_date}", timeout))
    results["routes"].append(_measure_twice("daily_odds_models", "GET", f"{base}/daily-odds/models?date={target_date}", timeout))

    game_pk = _first_game_pk(calendar_payload)
    if game_pk:
        results["routes"].append(_measure_twice("matchup_detail", "GET", f"{base}/matchup/{game_pk}", timeout))
    else:
        results["routes"].append({"name": "matchup_detail", "skipped": True, "reason": "No game_pk found from lightweight calendar"})

    projection_url = f"{base}/models/projections?date={target_date}"
    projection_cold_payload, projection_cold = _request("GET", projection_url, timeout)
    projection_warm_payload, projection_warm = _request("GET", projection_url, timeout)
    results["routes"].append({
        "name": "model_projections",
        "method": "GET",
        "url": projection_url,
        "cold": projection_cold,
        "warm": projection_warm,
    })
    results["probability_verification"] = _projection_probability_summary(projection_warm_payload)

    for endpoint in ("/debug/performance", "/debug/performance/hotspots"):
        payload, meta = _request("GET", f"{base}{endpoint}", timeout)
        results["debug"][endpoint] = {"meta": meta, "payload": payload}

    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Run Phase 7 MLBGPT cold/warm route benchmarks and probability-source checks.")
    parser.add_argument("--base-url", required=True, help="Base URL such as https://mlbgpt.com")
    parser.add_argument("--date", default=None, help="YYYY-MM-DD. Defaults to today.")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--output", default=None, help="Optional JSON output path.")
    args = parser.parse_args()

    report = run(args.base_url, date=args.date, timeout=args.timeout)
    text = json.dumps(report, indent=2, sort_keys=True)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(text + "\n")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
