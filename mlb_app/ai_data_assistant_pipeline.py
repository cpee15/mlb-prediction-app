from __future__ import annotations

import copy
import json
import os
from typing import Any, Dict, List, Optional

from . import ai_data_assistant as core


PACKET_KEYS = [
    "intent",
    "date",
    "game_pk",
    "player_id",
    "team_id",
    "sources_used",
    "games",
    "primary_recommendations",
    "watchlist",
    "projection_edges",
    "prop_watchlist",
    "pitcher_leans",
    "odds_summary",
    "data_quality",
    "missing_data",
    "warnings",
]

SCALAR_KEYS = {"game_pk", "player_id", "team_id"}


def _ensure_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _ensure_warning_list(value: Any) -> List[str]:
    warnings: List[str] = []
    for item in _ensure_list(value):
        if item in (None, "", [], {}):
            continue
        if isinstance(item, dict):
            label = item.get("label") or item.get("game_pk") or item.get("pitcher_name") or item.get("player_name") or "item"
            detail = item.get("missing") or item.get("missing_inputs") or item.get("rejection_reason") or item
            warnings.append(f"{label}: {detail}")
        else:
            warnings.append(str(item))
    return warnings


def _base_packet(
    *,
    intent: str,
    date: str,
    game_pk: Optional[int] = None,
    player_id: Optional[int] = None,
    team_id: Optional[int] = None,
    sources_used: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return {
        "intent": intent,
        "date": date,
        "game_pk": game_pk,
        "player_id": player_id,
        "team_id": team_id,
        "sources_used": list(sources_used or []),
        "games": [],
        "primary_recommendations": [],
        "watchlist": [],
        "projection_edges": [],
        "prop_watchlist": [],
        "pitcher_leans": [],
        "odds_summary": {},
        "data_quality": {},
        "missing_data": [],
        "warnings": [],
    }


def _recommendation_from_projection_edge(edge: Dict[str, Any]) -> Dict[str, Any]:
    total = edge.get("total_projection") or {}
    return {
        "type": "model_edge",
        "game_pk": edge.get("game_pk"),
        "label": edge.get("label"),
        "selection": edge.get("model_favorite"),
        "confidence_tier": edge.get("confidence_tier"),
        "confidence": edge.get("confidence"),
        "score": edge.get("score"),
        "win_probability_edge": edge.get("win_probability_edge"),
        "expected_run_differential": edge.get("expected_run_differential"),
        "projected_total_runs": total.get("total_expected_runs"),
        "reasons": edge.get("why") or [],
        "missing_inputs": edge.get("missing_inputs") or [],
        "actionability": "model_supported_side_lean",
    }


def _recommendation_from_pitcher_lean(lean: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": "pitcher_watchlist",
        "game_pk": lean.get("game_pk"),
        "label": lean.get("label"),
        "selection": lean.get("pitcher_name"),
        "team": lean.get("team"),
        "opponent": lean.get("opponent"),
        "category": lean.get("category"),
        "confidence_tier": lean.get("confidence_tier"),
        "confidence": lean.get("confidence"),
        "score": lean.get("score"),
        "reasons": lean.get("reasons") or [],
        "missing_inputs": lean.get("missing_inputs") or [],
        "actionability": "watchlist_only",
    }


def _watchlist_from_projection_signal(item: Dict[str, Any]) -> Dict[str, Any]:
    label = item.get("player_name") or item.get("team") or item.get("label")
    return {
        "type": item.get("type") or "watchlist",
        "label": label,
        "selection": item.get("angle") or item.get("pick") or label,
        "team": item.get("team"),
        "opponent": item.get("opponent"),
        "market": item.get("market"),
        "confidence_tier": item.get("confidence_tier"),
        "score": item.get("score"),
        "expected_value": item.get("expected_value"),
        "reasons": item.get("reasons") or item.get("drivers") or [],
        "actionability": "watchlist_only" if not item.get("market_price_available", True) else "priced_candidate",
    }


def _recommendation_from_odds_candidate(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": "priced_candidate",
        "label": item.get("player_name") or item.get("selection") or item.get("pick") or item.get("market_name"),
        "market": item.get("market") or item.get("market_name"),
        "selection": item.get("selection") or item.get("pick"),
        "line": item.get("line"),
        "price": item.get("price"),
        "score": item.get("score"),
        "confidence": item.get("confidence"),
        "model_probability": item.get("model_probability"),
        "market_implied_probability": item.get("market_implied_probability"),
        "edge": item.get("edge"),
        "expected_value": item.get("expected_value"),
        "game_pk": item.get("game_pk"),
        "away_team": item.get("away_team"),
        "home_team": item.get("home_team"),
        "drivers": item.get("drivers") or [],
        "missing_inputs": item.get("missing_inputs") or [],
        "actionability": "priced_candidate" if item.get("price") not in (None, "") else "watchlist_only",
    }


def _recommendation_from_stored_matchup(row: Dict[str, Any]) -> Dict[str, Any]:
    hitter = row.get("batter_name") or f"Batter {row.get('batter_id')}"
    pitcher = row.get("opposing_pitcher_name") or f"Pitcher {row.get('opposing_pitcher_id')}"
    pitch_name = row.get("pitch_name") or row.get("pitch_type")
    return {
        "type": "stored_365_matchup",
        "game_pk": row.get("game_pk"),
        "label": f"{hitter} vs {pitcher}",
        "selection": hitter,
        "pitch": pitch_name,
        "score": row.get("rank_score"),
        "confidence": row.get("confidence"),
        "confidence_tier": row.get("confidence_tier"),
        "reasons": row.get("reasons") or [],
        "missing_inputs": row.get("missing_inputs") or [],
        "actionability": "watchlist_only",
    }


def _normalize_packet(packet: Dict[str, Any]) -> Dict[str, Any]:
    normalized = copy.deepcopy(packet)
    for key in PACKET_KEYS:
        if key in {"odds_summary", "data_quality"}:
            normalized.setdefault(key, {})
        elif key in {"intent", "date"}:
            normalized.setdefault(key, "")
        elif key in SCALAR_KEYS:
            normalized.setdefault(key, None)
        else:
            normalized.setdefault(key, [])
    if not isinstance(normalized.get("sources_used"), list):
        normalized["sources_used"] = _ensure_list(normalized.get("sources_used"))
    if not isinstance(normalized.get("missing_data"), list):
        normalized["missing_data"] = _ensure_list(normalized.get("missing_data"))
    warnings = _ensure_warning_list(normalized.get("warnings"))
    warnings.extend(_ensure_warning_list(normalized.get("missing_data")))
    deduped: List[str] = []
    seen = set()
    for warning in warnings:
        if warning not in seen:
            seen.add(warning)
            deduped.append(warning)
    normalized["warnings"] = deduped[:20]
    normalized["context_preview"] = compact_packet_preview(normalized)
    return normalized


def build_best_model_edges_packet(
    session,
    *,
    message: str,
    date: str,
    game_pk: Optional[int] = None,
    player_id: Optional[int] = None,
    team_id: Optional[int] = None,
) -> Dict[str, Any]:
    context = core.build_model_projection_intelligence_context(session, date)
    packet = _base_packet(
        intent="best_model_edges",
        date=date,
        game_pk=game_pk,
        player_id=player_id,
        team_id=team_id,
        sources_used=context.get("sources_used"),
    )
    packet["projection_edges"] = context.get("projection_edges") or []
    packet["prop_watchlist"] = context.get("prop_watchlist") or []
    packet["primary_recommendations"] = [_recommendation_from_projection_edge(edge) for edge in packet["projection_edges"][:5]]
    packet["watchlist"] = [_watchlist_from_projection_signal(item) for item in packet["prop_watchlist"][:8]]
    packet["data_quality"] = context.get("data_quality") or {}
    packet["missing_data"] = context.get("missing_data") or []
    packet["warnings"] = context.get("projection_errors") or []
    return _normalize_packet(packet)


def build_odds_and_props_packet(
    session,
    *,
    message: str,
    date: str,
    game_pk: Optional[int] = None,
    player_id: Optional[int] = None,
    team_id: Optional[int] = None,
) -> Dict[str, Any]:
    context = core.build_odds_and_props_context(session, date)
    odds_summary = context.get("odds_summary") or {}
    top_candidates = odds_summary.get("top_prop_model_candidates") or []
    packet = _base_packet(
        intent="odds_and_props",
        date=date,
        game_pk=game_pk,
        player_id=player_id,
        team_id=team_id,
        sources_used=context.get("sources_used"),
    )
    packet["odds_summary"] = odds_summary
    packet["primary_recommendations"] = [_recommendation_from_odds_candidate(item) for item in top_candidates[:6]]
    packet["watchlist"] = [_watchlist_from_projection_signal(item) for item in (context.get("projection_watchlist") or [])[:8]]
    packet["prop_watchlist"] = context.get("projection_watchlist") or []
    packet["data_quality"] = context.get("data_quality") or {}
    packet["missing_data"] = context.get("missing_data") or []
    return _normalize_packet(packet)


def build_game_explanation_packet(
    session,
    *,
    message: str,
    date: str,
    game_pk: Optional[int] = None,
    player_id: Optional[int] = None,
    team_id: Optional[int] = None,
) -> Dict[str, Any]:
    target_game_pk = int(game_pk) if game_pk is not None else None
    context = core.build_game_explanation_context(session, target_game_pk, date=date)
    packet = _base_packet(
        intent="game_explanation",
        date=context.get("date") or date,
        game_pk=target_game_pk,
        player_id=player_id,
        team_id=team_id,
        sources_used=context.get("sources_used"),
    )
    if context.get("game_projection_edge"):
        edge = context["game_projection_edge"]
        packet["projection_edges"] = [edge]
        packet["primary_recommendations"] = [_recommendation_from_projection_edge(edge)]
        packet["watchlist"] = [_watchlist_from_projection_signal(item) for item in (edge.get("prop_watchlist") or [])[:6]]
        packet["prop_watchlist"] = edge.get("prop_watchlist") or []
    elif context.get("game"):
        packet["games"] = [context.get("game")]
    packet["data_quality"] = context.get("data_quality") or {}
    packet["missing_data"] = context.get("missing_data") or []
    return _normalize_packet(packet)


def build_pitcher_analysis_packet(
    session,
    *,
    message: str,
    date: str,
    game_pk: Optional[int] = None,
    player_id: Optional[int] = None,
    team_id: Optional[int] = None,
) -> Dict[str, Any]:
    context = core.build_pitcher_lean_context(session, date)
    leans = context.get("pitcher_leans") or []
    packet = _base_packet(
        intent="pitcher_analysis",
        date=date,
        game_pk=game_pk,
        player_id=player_id,
        team_id=team_id,
        sources_used=context.get("sources_used"),
    )
    packet["pitcher_leans"] = leans
    packet["primary_recommendations"] = [_recommendation_from_pitcher_lean(item) for item in leans[:5]]
    packet["watchlist"] = [_recommendation_from_pitcher_lean(item) for item in leans[5:10]]
    packet["data_quality"] = context.get("data_quality") or {}
    packet["missing_data"] = context.get("missing_data") or []
    return _normalize_packet(packet)


def build_data_quality_packet(
    session,
    *,
    message: str,
    date: str,
    game_pk: Optional[int] = None,
    player_id: Optional[int] = None,
    team_id: Optional[int] = None,
) -> Dict[str, Any]:
    context = core.build_data_quality_context(session, date)
    packet = _base_packet(
        intent="data_quality",
        date=date,
        game_pk=game_pk,
        player_id=player_id,
        team_id=team_id,
        sources_used=context.get("sources_used"),
    )
    packet["data_quality"] = context.get("data_quality") or {}
    packet["missing_data"] = context.get("missing_data") or []
    return _normalize_packet(packet)


def build_stored_365_packet(
    session,
    *,
    message: str,
    date: str,
    game_pk: Optional[int] = None,
    player_id: Optional[int] = None,
    team_id: Optional[int] = None,
) -> Dict[str, Any]:
    context = core.build_stored_365_sweep_context(session, date)
    rows = context.get("top_matchups") or []
    packet = _base_packet(
        intent="stored_365_matchups",
        date=date,
        game_pk=game_pk,
        player_id=player_id,
        team_id=team_id,
        sources_used=context.get("sources_used"),
    )
    packet["primary_recommendations"] = [_recommendation_from_stored_matchup(row) for row in rows[:5]]
    packet["watchlist"] = [_recommendation_from_stored_matchup(row) for row in rows[5:10]]
    packet["data_quality"] = context.get("data_quality") or {}
    packet["missing_data"] = context.get("missing_data") or []
    packet["top_matchups"] = rows[:10]
    return _normalize_packet(packet)


def build_daily_slate_packet(
    session,
    *,
    message: str,
    date: str,
    game_pk: Optional[int] = None,
    player_id: Optional[int] = None,
    team_id: Optional[int] = None,
) -> Dict[str, Any]:
    context = core.build_daily_slate_context(session, date)
    packet = _base_packet(
        intent=context.get("intent") or "daily_slate_summary",
        date=date,
        game_pk=game_pk,
        player_id=player_id,
        team_id=team_id,
        sources_used=context.get("sources_used"),
    )
    packet["games"] = context.get("games") or []
    edges = context.get("top_edges") or []
    packet["projection_edges"] = edges
    packet["prop_watchlist"] = context.get("prop_watchlist") or []
    packet["primary_recommendations"] = [_recommendation_from_projection_edge(edge) for edge in edges[:5]]
    packet["watchlist"] = [_watchlist_from_projection_signal(item) for item in packet["prop_watchlist"][:8]]
    packet["data_quality"] = context.get("data_quality") or {}
    packet["missing_data"] = context.get("missing_data") or []
    return _normalize_packet(packet)


def build_assistant_packet(
    session,
    *,
    message: str,
    date: Optional[str] = None,
    game_pk: Optional[int] = None,
    player_id: Optional[int] = None,
    team_id: Optional[int] = None,
) -> Dict[str, Any]:
    target_date = core.date_from_message(message, date)
    intent = core.classify_assistant_intent(message)
    if game_pk and intent in {"game_explanation", "unknown_app_question", "daily_slate_summary", "best_model_edges"}:
        return build_game_explanation_packet(
            session,
            message=message,
            date=target_date,
            game_pk=game_pk,
            player_id=player_id,
            team_id=team_id,
        )
    builders = {
        "best_model_edges": build_best_model_edges_packet,
        "odds_and_props": build_odds_and_props_packet,
        "pitcher_analysis": build_pitcher_analysis_packet,
        "data_quality": build_data_quality_packet,
        "stored_365_matchups": build_stored_365_packet,
    }
    builder = builders.get(intent, build_daily_slate_packet)
    packet = builder(
        session,
        message=message,
        date=target_date,
        game_pk=game_pk,
        player_id=player_id,
        team_id=team_id,
    )
    if intent == "unknown_app_question":
        packet["intent"] = "daily_slate_summary"
    return _normalize_packet(packet)


def compact_packet_preview(packet: Dict[str, Any], max_chars: int = 6000) -> Dict[str, Any]:
    preview = {
        "intent": packet.get("intent"),
        "date": packet.get("date"),
        "game_pk": packet.get("game_pk"),
        "player_id": packet.get("player_id"),
        "team_id": packet.get("team_id"),
        "sources_used": packet.get("sources_used") or [],
        "primary_recommendations": (packet.get("primary_recommendations") or [])[:4],
        "watchlist": (packet.get("watchlist") or [])[:6],
        "games": (packet.get("games") or [])[:4],
        "data_quality": packet.get("data_quality") or {},
        "missing_data": (packet.get("missing_data") or [])[:8],
        "warnings": (packet.get("warnings") or [])[:8],
    }
    raw = json.dumps(preview, default=str)
    if len(raw) <= max_chars:
        return preview
    preview["watchlist"] = preview.get("watchlist", [])[:3]
    preview["primary_recommendations"] = preview.get("primary_recommendations", [])[:3]
    preview["games"] = preview.get("games", [])[:2]
    preview["missing_data"] = preview.get("missing_data", [])[:4]
    return preview


def _stringify_list(values: List[Dict[str, Any]], label_key: str = "label", selection_key: str = "selection") -> List[str]:
    items: List[str] = []
    for value in values:
        label = value.get(label_key) or value.get(selection_key) or "item"
        selection = value.get(selection_key)
        score = value.get("score")
        tier = value.get("confidence_tier")
        parts = [str(label)]
        if selection and selection != label:
            parts.append(str(selection))
        if score not in (None, ""):
            parts.append(f"score {core.format_value(score)}")
        if tier:
            parts.append(str(tier))
        items.append(" | ".join(parts))
    return items


def _lead_line(packet: Dict[str, Any]) -> str:
    intent = packet.get("intent")
    recs = packet.get("primary_recommendations") or []
    watchlist = packet.get("watchlist") or []
    first = recs[0] if recs else None
    first_watch = watchlist[0] if watchlist else None
    if intent == "best_model_edges":
        if first:
            return f"The cleanest model edge right now is {first.get('label') or first.get('selection')}."
        return "I don’t have a clean model edge to push right now from the current packet."
    if intent == "odds_and_props":
        odds = packet.get("odds_summary") or {}
        if first:
            return f"Here’s what jumps out from Daily Odds first: {first.get('label') or first.get('selection')} looks like the strongest priced angle in the current packet."
        return f"Daily Odds loaded {odds.get('count') or 0} game rows, but I don’t have a priced angle I’d elevate from this packet yet."
    if intent == "pitcher_analysis":
        if first:
            return f"The best pitcher angle on the board is {first.get('selection') or first.get('label')}, but I’d still treat it as a watchlist lean unless you have a live market in front of you."
        return "I don’t have a strong pitcher lean from the current model packet."
    if intent == "stored_365_matchups":
        if first:
            return f"The best Stored 365 matchup flag is {first.get('label')}. That’s a watchlist signal, not a priced edge by itself."
        return "Stored 365 didn’t return a matchup I’d elevate right now."
    if intent == "data_quality":
        return "The biggest thing here is data quality, so I’m surfacing what looks thin instead of pretending the slate is cleaner than it is."
    if intent == "game_explanation":
        if first:
            return f"Here’s the short version: the current packet leans toward {first.get('selection') or first.get('label')}."
        return "I can explain the matchup, but the signal is thinner than I’d like from the current packet."
    if first:
        return f"Here’s what stands out first: {first.get('label') or first.get('selection')} is the clearest angle in the current slate packet."
    if first_watch:
        return f"I don’t have a full recommendation yet, but {first_watch.get('label') or first_watch.get('selection')} is worth keeping on the watchlist."
    return f"I checked the {packet.get('date')} slate, but I don’t have a strong angle to force from the current packet."


def render_structured_answer(packet: Dict[str, Any]) -> str:
    lead = _lead_line(packet)
    watchlist = packet.get("watchlist") or []
    warnings = packet.get("warnings") or []
    missing = packet.get("missing_data") or []
    sources = packet.get("sources_used") or []

    lines = [lead]
    if watchlist:
        labels = [item.get("label") or item.get("selection") for item in watchlist[:3] if item.get("label") or item.get("selection")]
        if labels:
            lines.append(f"After that, I’d keep an eye on {', '.join(labels)}.")
    if warnings or missing:
        lines.append("There are still some data gaps or weak spots behind this answer, so I wouldn’t treat it as cleaner than the packet actually is.")
    if sources:
        lines.append(f"I’m grounding this in {', '.join(sources[:4])}." + ("" if len(sources) <= 4 else ""))
    return "\n\n".join(lines)


def _confidence_note(packet: Dict[str, Any]) -> str:
    if packet.get("missing_data"):
        return "Answer is limited by missing or weak app-owned data flagged in the response. Model confidence is separated from betting certainty."
    quality = packet.get("data_quality") or {}
    if quality.get("game_count") == 0 and quality.get("projection_games") == 0:
        return "No games were found in the app-owned matchup/projection feed for this date."
    return "Answer is based only on DK/model-projection-first app-owned evidence. The LLM, when enabled, rewrites the answer only and does not change source-of-truth fields."


def render_structured_response(message: str, packet: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_packet(packet)
    return {
        "answer": render_structured_answer(normalized),
        "intent": normalized.get("intent"),
        "primary_recommendations": normalized.get("primary_recommendations") or [],
        "watchlist": normalized.get("watchlist") or [],
        "data_used": normalized.get("sources_used") or [],
        "sources_used": normalized.get("sources_used") or [],
        "missing_data": normalized.get("missing_data") or [],
        "warnings": normalized.get("warnings") or [],
        "confidence_note": _confidence_note(normalized),
        "date": normalized.get("date"),
        "game_pk": normalized.get("game_pk"),
        "player_id": normalized.get("player_id"),
        "team_id": normalized.get("team_id"),
        "context_preview": normalized.get("context_preview") or compact_packet_preview(normalized),
        "data_quality": normalized.get("data_quality") or {},
    }


def answer_with_optional_llm_structured(message: str, packet: Dict[str, Any], use_llm: bool = True) -> Dict[str, Any]:
    response = render_structured_response(message, packet)
    if not use_llm or not os.getenv("OPENAI_API_KEY"):
        return response
    try:
        from openai import OpenAI

        client = OpenAI()
        compact = json.dumps(response.get("context_preview") or compact_packet_preview(packet, max_chars=10000), default=str)
        llm_response = client.chat.completions.create(
            model=os.getenv("AI_DATA_ASSISTANT_MODEL", "gpt-4o-mini"),
            messages=[
                {"role": "system", "content": core.AI_DATA_ASSISTANT_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": (
                        f"Question: {message}\n\n"
                        "You may rewrite the answer text only. Do not invent data. "
                        "Do not change the structured response contract.\n\n"
                        f"App-ranked evidence packet JSON:\n{compact}"
                    ),
                },
            ],
            temperature=0.2,
            max_tokens=900,
        )
        answer = llm_response.choices[0].message.content if llm_response.choices else None
        if answer:
            response["answer"] = answer
            response["llm_answer"] = answer
            response["confidence_note"] += " LLM summarized the backend-ranked compact evidence packet only."
    except Exception as exc:
        response["confidence_note"] += f" LLM unavailable, deterministic fallback used: {exc}"
    return response
