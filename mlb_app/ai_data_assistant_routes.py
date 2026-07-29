from __future__ import annotations

import datetime as dt
import json
import os
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Cookie, Header, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from .admin_access import resolve_principal
from .ai_data_assistant import AI_DATA_ASSISTANT_SYSTEM_PROMPT, PROMPT_CHIPS, classify_assistant_intent
from .ai_data_assistant_performance import build_ai_data_assistant_response, clear_ai_data_assistant_caches
from .database import create_tables, get_engine, get_session
from .my_dashboard_routes import DashboardPlayerReportRequest, my_dashboard_player_report_query, router as my_dashboard_router
from .saved_report_analysis import (
    build_saved_report_packet,
    render_saved_report_answer,
    resolve_owned_saved_reports,
)

router = APIRouter()
router.include_router(my_dashboard_router)


class ConversationTurn(BaseModel):
    role: str
    content: str


class AIDataAssistantRequest(BaseModel):
    message: str
    date: Optional[str] = None
    game_pk: Optional[int] = None
    player_id: Optional[int] = None
    team_id: Optional[int] = None
    use_llm: Optional[bool] = None
    conversation: Optional[List[ConversationTurn]] = None
    saved_report_ids: List[int] = Field(default_factory=list, max_length=5)


def session_factory():
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    return get_session(engine)


def _llm_configured() -> bool:
    return bool(os.getenv("OPENAI_API_KEY"))


def _default_use_llm() -> bool:
    if not _llm_configured():
        return False
    raw = os.getenv("AI_DATA_ASSISTANT_DEFAULT_USE_LLM", "true").strip().lower()
    return raw not in {"0", "false", "no", "off"}


def _resolve_use_llm(requested: Optional[bool]) -> bool:
    if requested is None:
        return _default_use_llm()
    if not _llm_configured():
        return False
    return bool(requested)


def _normalize_conversation(conversation: Optional[List[ConversationTurn]]) -> List[Dict[str, str]]:
    normalized: List[Dict[str, str]] = []
    for turn in conversation or []:
        role = str(turn.role or "").strip().lower()
        content = str(turn.content or "").strip()
        if role not in {"user", "assistant"} or not content:
            continue
        normalized.append({"role": role, "content": content})
    return normalized[-8:]


def _conversation_transcript(conversation: List[Dict[str, str]]) -> str:
    if not conversation:
        return "No prior conversation provided."
    lines = []
    for turn in conversation[-6:]:
        label = "User" if turn["role"] == "user" else "Assistant"
        lines.append(f"{label}: {turn['content']}")
    return "\n".join(lines)


def _apply_conversational_llm_polish(message: str, result: Dict[str, Any], conversation: List[Dict[str, str]]) -> Dict[str, Any]:
    if not _llm_configured():
        return result
    from openai import OpenAI

    client = OpenAI()
    packet = result.get("context_preview") or {}
    transcript = _conversation_transcript(conversation)
    structured_summary = json.dumps(
        {
            "intent": result.get("intent"),
            "primary_recommendations": result.get("primary_recommendations") or [],
            "watchlist": result.get("watchlist") or [],
            "warnings": result.get("warnings") or [],
            "missing_data": result.get("missing_data") or [],
            "confidence_note": result.get("confidence_note"),
            "saved_report_analysis": result.get("saved_report_analysis"),
            "deterministic_answer": result.get("answer"),
        },
        default=str,
    )
    style_prompt = (
        "You are rewriting the final assistant answer for a chat UI. "
        "Sound like a sharp, natural MLB analyst. Be conversational, direct, and useful. "
        "Do not sound like a report template. Do not invent facts. Do not change the underlying recommendations, warnings, or evidence. "
        "You may reference recent chat context to resolve pronouns or continue the conversation naturally, but app-owned evidence remains the only factual source of truth. "
        "Keep it concise unless the user asked for detail. Use bullets only when they genuinely help. "
        "Make the answer feel like a real chatbot reply."
    )
    response = client.chat.completions.create(
        model=os.getenv("AI_DATA_ASSISTANT_MODEL", "gpt-4o-mini"),
        messages=[
            {"role": "system", "content": AI_DATA_ASSISTANT_SYSTEM_PROMPT + "\n\n" + style_prompt},
            {
                "role": "user",
                "content": (
                    f"Recent conversation:\n{transcript}\n\n"
                    f"Current user question:\n{message}\n\n"
                    f"Structured response contract JSON:\n{structured_summary}\n\n"
                    f"Evidence packet preview JSON:\n{json.dumps(packet, default=str)}\n\n"
                    "Rewrite the answer text only."
                ),
            },
        ],
        temperature=0.35,
        max_tokens=700,
    )
    answer = response.choices[0].message.content if response.choices else None
    if answer:
        result["answer"] = answer
        result["llm_answer"] = answer
    return result


@router.get("/ai-data-assistant", response_class=HTMLResponse)
def ai_data_assistant_page() -> str:
    chips = "".join(f'<button class="chip" type="button">{chip}</button>' for chip in PROMPT_CHIPS)
    today = dt.date.today().isoformat()
    use_llm_checked = "checked" if _default_use_llm() else ""
    llm_note = "LLM polish available" if _llm_configured() else "Deterministic mode only"
    return f"""
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>AI Data Assistant</title>
  <style>
    body {{ margin: 0; font-family: Inter, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #0b1020; color: #eef2ff; }}
    .wrap {{ max-width: 1120px; margin: 0 auto; padding: 32px 20px 56px; }}
    .hero {{ border: 1px solid rgba(148,163,184,.25); background: linear-gradient(135deg, rgba(30,41,59,.92), rgba(15,23,42,.94)); border-radius: 24px; padding: 28px; box-shadow: 0 24px 80px rgba(0,0,0,.35); }}
    h1 {{ margin: 0 0 8px; font-size: 34px; }}
    .sub {{ margin: 0; color: #cbd5e1; line-height: 1.55; }}
    .panel {{ margin-top: 20px; border: 1px solid rgba(148,163,184,.22); background: rgba(15,23,42,.72); border-radius: 20px; padding: 18px; }}
    .row {{ display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }}
    label {{ color: #cbd5e1; font-size: 13px; }}
    input[type="date"], input[type="number"], textarea {{ background: #020617; color: #f8fafc; border: 1px solid rgba(148,163,184,.35); border-radius: 12px; padding: 10px 12px; }}
    textarea {{ width: 100%; min-height: 96px; resize: vertical; box-sizing: border-box; margin-top: 12px; font-size: 15px; line-height: 1.45; }}
    button {{ border: 0; border-radius: 999px; padding: 10px 14px; background: #38bdf8; color: #04111f; font-weight: 700; cursor: pointer; }}
    .chip {{ background: rgba(56,189,248,.12); color: #e0f2fe; border: 1px solid rgba(56,189,248,.35); margin: 6px 6px 0 0; font-weight: 600; }}
    pre {{ white-space: pre-wrap; overflow-wrap: anywhere; background: #020617; border: 1px solid rgba(148,163,184,.22); padding: 16px; border-radius: 16px; line-height: 1.55; }}
    .meta {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin-top: 14px; }}
    .card {{ background: rgba(2,6,23,.72); border: 1px solid rgba(148,163,184,.2); border-radius: 14px; padding: 12px; color: #cbd5e1; }}
    .muted {{ color: #94a3b8; font-size: 13px; }}
  </style>
</head>
<body>
  <main class="wrap">
    <section class="hero">
      <h1>AI Data Assistant</h1>
      <p class="sub">A focused baseball analyst over this app's own matchup, model, odds, Stored 365, player-profile, live, and data-quality outputs. It does not browse or invent missing baseball data.</p>
      <div class="panel">
        <div class="row">
          <label>Date <input id="date" type="date" value="{today}" /></label>
          <label>Game PK <input id="gamePk" type="number" placeholder="optional" /></label>
          <label>Player ID <input id="playerId" type="number" placeholder="optional" /></label>
          <label><input id="useLlm" type="checkbox" {use_llm_checked} /> Use LLM polish</label>
          <span class="muted">{llm_note}</span>
          <button id="askBtn" type="button">Ask</button>
        </div>
        <textarea id="message" placeholder="Ask about today's slate, model edges, Daily Odds, Stored 365, missing data, or a specific matchup.">Summarize today's slate</textarea>
        <div id="chips">{chips}</div>
      </div>
      <div class="panel">
        <div class="muted" id="status">Ready</div>
        <pre id="answer">Ask a question to query the app-owned data packet.</pre>
        <div class="meta">
          <div class="card"><strong>Sources used</strong><div id="sources" class="muted">None yet</div></div>
          <div class="card"><strong>Timing</strong><div id="timing" class="muted">None yet</div></div>
          <div class="card"><strong>Data quality</strong><div id="quality" class="muted">None yet</div></div>
        </div>
      </div>
    </section>
  </main>
<script>
const $ = (id) => document.getElementById(id);
document.querySelectorAll('.chip').forEach(btn => btn.addEventListener('click', () => {{ $('message').value = btn.textContent; ask(); }}));
$('askBtn').addEventListener('click', ask);
async function ask() {{
  $('status').textContent = 'Querying app-owned data...';
  $('answer').textContent = '';
  const payload = {{
    message: $('message').value,
    date: $('date').value || null,
    game_pk: $('gamePk').value ? Number($('gamePk').value) : null,
    player_id: $('playerId').value ? Number($('playerId').value) : null,
    use_llm: $('useLlm').checked
  }};
  try {{
    const res = await fetch('/ai-data-assistant', {{ method: 'POST', headers: {{ 'Content-Type': 'application/json' }}, body: JSON.stringify(payload) }});
    const data = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(data.detail || data));
    $('status').textContent = `Intent: ${{data.intent || 'unknown'}} | Date: ${{data.date || ''}} | LLM: ${{data.llm_mode?.active ? 'active' : data.llm_mode?.requested ? 'requested' : 'off'}}`;
    $('answer').textContent = data.answer || 'No answer returned.';
    $('sources').textContent = (data.sources_used || []).join(', ') || 'None';
    $('timing').textContent = JSON.stringify(data.timing || {{}}, null, 2);
    $('quality').textContent = JSON.stringify({{ data_quality: data.data_quality || {{}}, missing_data: data.missing_data || [] }}, null, 2);
  }} catch (err) {{
    $('status').textContent = 'Error';
    $('answer').textContent = String(err);
  }}
}}
</script>
</body>
</html>
"""


@router.get("/ai-data-assistant/prompts")
def ai_data_assistant_prompts() -> Dict[str, Any]:
    return {"name": "AI Data Assistant", "prompt_chips": PROMPT_CHIPS}


@router.get("/ai-data-assistant/health")
def ai_data_assistant_health() -> Dict[str, Any]:
    return {
        "name": "AI Data Assistant",
        "status": "ok",
        "llm_configured": _llm_configured(),
        "deterministic_default": not _default_use_llm(),
        "default_use_llm": _default_use_llm(),
    }


@router.post("/ai-data-assistant")
def ai_data_assistant_query(
    payload: AIDataAssistantRequest,
    mlb_dashboard_session: Optional[str] = Cookie(default=None),
    x_dashboard_session: Optional[str] = Header(default=None, alias="X-Dashboard-Session"),
) -> Dict[str, Any]:
    if not payload.message or not payload.message.strip():
        raise HTTPException(status_code=400, detail="message is required")
    try:
        factory = session_factory()
        with factory() as session:
            conversation = _normalize_conversation(payload.conversation)
            resolved_use_llm = _resolve_use_llm(payload.use_llm)
            owned_reports = []
            if payload.saved_report_ids:
                principal = resolve_principal(session, x_dashboard_session or mlb_dashboard_session)
                if not principal:
                    raise HTTPException(status_code=401, detail="Dashboard sign-in required")
                try:
                    owned_reports = resolve_owned_saved_reports(
                        session, principal.user_id, payload.saved_report_ids
                    )
                except LookupError as exc:
                    # Deliberately does not reveal whether an ID exists for another user.
                    raise HTTPException(status_code=404, detail="One or more saved reports are unavailable") from exc
                except ValueError as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc
            result = build_ai_data_assistant_response(
                session=session,
                message=payload.message,
                date=payload.date,
                game_pk=payload.game_pk,
                player_id=payload.player_id,
                team_id=payload.team_id,
                use_llm=False,
            )
            if owned_reports:
                try:
                    saved_packet = build_saved_report_packet(
                        owned_reports,
                        lambda request: my_dashboard_player_report_query(
                            DashboardPlayerReportRequest(**request)
                        ),
                    )
                    base_context = result.get("context_preview") or {}
                    saved_packet["authoritative_context"] = {
                        "projection_edges": list(
                            base_context.get("projection_edges")
                            or base_context.get("top_edges")
                            or []
                        )[:5],
                        "game_projection_edge": base_context.get("game_projection_edge"),
                        "projection_watchlist": list(base_context.get("projection_watchlist") or [])[:5],
                        "odds_summary": base_context.get("odds_summary"),
                        "data_quality": base_context.get("data_quality"),
                    }
                except ValueError as exc:
                    raise HTTPException(status_code=400, detail=str(exc)) from exc
                result["saved_report_analysis"] = saved_packet
                result["answer"] = render_saved_report_answer(saved_packet)
                result["context_preview"] = {
                    **(result.get("context_preview") or {}),
                    "saved_report_analysis": saved_packet,
                }
                result["sources_used"] = list(dict.fromkeys(
                    [*(result.get("sources_used") or []), "owned_saved_reports", "my_dashboard_report_engine"]
                ))
            llm_mode = {
                "requested": bool(payload.use_llm) if payload.use_llm is not None else resolved_use_llm,
                "configured": _llm_configured(),
                "active": False,
                "conversation_turns": len(conversation),
            }
            if resolved_use_llm and _llm_configured():
                result = _apply_conversational_llm_polish(payload.message, result, conversation)
                llm_mode["active"] = True
            elif (payload.use_llm or resolved_use_llm) and not _llm_configured():
                warnings = list(result.get("warnings") or [])
                warning = "LLM mode was requested but OPENAI_API_KEY is not configured, so deterministic fallback was used."
                if warning not in warnings:
                    warnings.append(warning)
                result["warnings"] = warnings
                result["confidence_note"] = (result.get("confidence_note") or "") + " Deterministic fallback was used because LLM mode is not configured."
            result["llm_mode"] = llm_mode
            result["name"] = "AI Data Assistant"
            result["intent"] = result.get("intent") or classify_assistant_intent(payload.message)
            return result
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail={"message": "AI Data Assistant failed", "error": str(exc)}) from exc


@router.post("/ai-data-assistant/cache/clear")
def ai_data_assistant_cache_clear() -> Dict[str, Any]:
    return clear_ai_data_assistant_caches()
