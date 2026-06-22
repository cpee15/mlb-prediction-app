# AI Data Assistant refactor notes

## Packet builder architecture

The assistant now remains on the existing `/ai-data-assistant` endpoint but routes requests through an explicit packet-builder layer before answer rendering.

Primary packet builders:
- `build_best_model_edges_packet`
- `build_odds_and_props_packet`
- `build_game_explanation_packet`
- `build_pitcher_analysis_packet`
- `build_data_quality_packet`

Support builders:
- `build_stored_365_packet`
- `build_daily_slate_packet`

Each builder:
1. Collects app-owned evidence only.
2. Normalizes the response into a shared packet contract.
3. Produces compact recommendation and watchlist objects.
4. Preserves missing-data and warning visibility.
5. Generates a compact packet preview for optional LLM summarization.

## Structured response contract

The route response remains backward compatible but now exposes a structured product contract:
- `answer`
- `intent`
- `primary_recommendations`
- `watchlist`
- `data_used`
- `sources_used`
- `missing_data`
- `warnings`
- `confidence_note`
- `date`
- `game_pk`
- `player_id`
- `team_id`
- `context_preview`
- `data_quality`

The deterministic backend remains the source of truth. Optional LLM mode may rewrite `answer` only.

## Trace logging design

Trace logging is implemented as an opt-in JSONL logger:
- env: `AI_DATA_ASSISTANT_TRACE_LOG_ENABLED=true`
- env: `AI_DATA_ASSISTANT_TRACE_LOG_PATH=/tmp/ai_data_assistant_traces.jsonl`

Each trace record stores:
- timestamp
- user message
- classified intent
- compact evidence packet
- deterministic answer
- optional LLM answer
- structured response summary
- feedback placeholders for later click/edit/follow-up/outcome signals

Logging failures do not block the user response.

## Remaining limitations

- Frontend still uses a simple form and does not yet collect explicit user feedback signals.
- Trace persistence is file-based for now rather than warehouse-backed.
- The current assistant still depends on the existing `ai_data_assistant.py` evidence functions underneath the new packet layer.
- No fine-tuning or self-hosting is introduced in this pass.
