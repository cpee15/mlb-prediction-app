from __future__ import annotations

import datetime as dt
import os
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from .ai_data_assistant import PROMPT_CHIPS, classify_assistant_intent
from .ai_data_assistant_performance import build_ai_data_assistant_response, clear_ai_data_assistant_caches
from .database import create_tables, get_engine, get_session

router = APIRouter()


class AIDataAssistantRequest(BaseModel):
    message: str
    date: Optional[str] = None
    game_pk: Optional[int] = None
    player_id: Optional[int] = None
    team_id: Optional[int] = None
    use_llm: bool = False


def session_factory():
    database_url = os.getenv("DATABASE_URL", "sqlite:///mlb.db")
    engine = get_engine(database_url)
    create_tables(engine)
    return get_session(engine)


@router.get("/ai-data-assistant", response_class=HTMLResponse)
def ai_data_assistant_page() -> str:
    chips = "".join(f'<button class="chip" type="button">{chip}</button>' for chip in PROMPT_CHIPS)
    today = dt.date.today().isoformat()
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
          <label><input id="useLlm" type="checkbox" /> Use LLM polish</label>
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
document.querySelectorAll('.chip').forEach(btn => btn.addEventListener('click', () => {{ $('message').value = btn.textContent; $('useLlm').checked = false; ask(); }}));
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
    $('status').textContent = `Intent: ${{data.intent || 'unknown'}} | Date: ${{data.date || ''}} | Cache: ${{data.cache_status || 'none'}}`;
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
    return {"name": "AI Data Assistant", "status": "ok", "llm_configured": bool(os.getenv("OPENAI_API_KEY")), "deterministic_default": True}


@router.post("/ai-data-assistant")
def ai_data_assistant_query(payload: AIDataAssistantRequest) -> Dict[str, Any]:
    if not payload.message or not payload.message.strip():
        raise HTTPException(status_code=400, detail="message is required")
    try:
        factory = session_factory()
        with factory() as session:
            result = build_ai_data_assistant_response(
                session=session,
                message=payload.message,
                date=payload.date,
                game_pk=payload.game_pk,
                player_id=payload.player_id,
                team_id=payload.team_id,
                use_llm=payload.use_llm,
            )
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
