from __future__ import annotations

import datetime as dt
import re
from typing import Any, Dict, Iterable, List, Optional

TAG_PATTERNS = {
    "injury": ["injury", "injured", "out", "day-to-day", "day to day", "hurt", "sore", "strain", "sprain", "IL", "injured list"],
    "lineup": ["lineup", "batting order", "not in lineup", "starting lineup", "rest day", "sitting"],
    "starter": ["starter", "probable", "opener", "starting pitcher", "scratched", "rotation"],
    "bullpen": ["bullpen", "reliever", "closer", "setup man", "available", "unavailable", "worked yesterday"],
    "transaction": ["transaction", "optioned", "recalled", "activated", "designated", "DFA", "called up", "call-up", "roster move"],
    "weather": ["weather", "rain", "delay", "delayed", "postponed", "wind", "roof", "suspended"],
    "odds": ["odds", "moneyline", "run line", "total", "prop", "price", "line move"],
    "betting": ["betting", "steam", "sharp", "public", "handle", "tickets", "sportsbook", "DraftKings", "FanDuel"],
    "clubhouse": ["clubhouse", "manager said", "told reporters", "pre-game", "pregame", "availability"],
    "manager_quote": ["manager", "said", "quote", "told reporters"],
    "roster": ["roster", "active roster", "40-man"],
    "callup": ["called up", "call-up", "promoted", "recalled"],
    "suspension": ["suspended", "suspension", "appeal"],
    "hot_streak": ["hot streak", "surging", "red-hot", "heater"],
    "cold_streak": ["slump", "cold streak", "struggling", "ice cold"],
}

HIGH_VALUE_TERMS = ["scratched", "out", "activated", "IL", "delayed", "postponed", "lineup", "not in lineup", "bullpen", "closer", "steam", "line move", "sharp", "opener", "pitch count"]


def classify_text(title: str = "", summary: str = "", source: str = "") -> List[str]:
    text = f"{title} {summary} {source}"
    low = text.lower()
    tags: List[str] = []
    for tag, patterns in TAG_PATTERNS.items():
        if any(pattern.lower() in low for pattern in patterns):
            tags.append(tag)
    if re.search(r"\b[A-Z][a-z]+\s[A-Z][a-z]+\b", text):
        tags.append("player")
    if "mlb" in low or "baseball" in low:
        tags.append("team")
    return sorted(set(tags))


def parse_datetime(value: Any) -> Optional[dt.datetime]:
    if not value:
        return None
    try:
        raw = str(value).replace("Z", "+00:00")
        parsed = dt.datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(dt.timezone.utc)
    except Exception:
        return None


def score_importance(item: Dict[str, Any], today_terms: Optional[Iterable[str]] = None, source_priority: int = 5) -> float:
    published = parse_datetime(item.get("published_at"))
    now = dt.datetime.now(dt.timezone.utc)
    recency = 0.15
    if published:
        hours = max((now - published).total_seconds() / 3600.0, 0.0)
        recency = max(0.05, 1.0 - min(hours, 168.0) / 168.0)
    tags = set(item.get("tags") or [])
    score = recency * 35.0
    score += max(0, 10 - source_priority) * 2.0
    if item.get("is_local") or item.get("is_beat_report"):
        score += 16.0
    if tags.intersection({"injury", "lineup", "starter", "weather", "bullpen"}):
        score += 22.0
    if tags.intersection({"odds", "betting"}) or item.get("is_betting_relevant"):
        score += 12.0
    text = f"{item.get('title', '')} {item.get('summary', '')}".lower()
    for term in HIGH_VALUE_TERMS:
        if term.lower() in text:
            score += 5.0
    for term in today_terms or []:
        if term and str(term).lower() in text:
            score += 4.0
    return round(min(score, 100.0), 2)
