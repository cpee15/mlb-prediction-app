from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _trace_enabled() -> bool:
    return os.getenv("AI_DATA_ASSISTANT_TRACE_LOG_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}


def _trace_path() -> str:
    return os.getenv("AI_DATA_ASSISTANT_TRACE_LOG_PATH", "/tmp/ai_data_assistant_traces.jsonl")


def build_trace_record(
    *,
    message: str,
    intent: str,
    packet_preview: Dict[str, Any],
    deterministic_answer: str,
    structured_response: Dict[str, Any],
    llm_answer: Optional[str] = None,
    feedback: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    feedback = dict(feedback or {})
    feedback.setdefault("user_clicked", None)
    feedback.setdefault("user_edited", None)
    feedback.setdefault("user_follow_up", None)
    feedback.setdefault("user_challenged", None)
    feedback.setdefault("user_accepted", None)
    feedback.setdefault("game_outcome", None)
    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "user_message": message,
        "classified_intent": intent,
        "compact_evidence_packet": packet_preview,
        "deterministic_answer": deterministic_answer,
        "llm_answer": llm_answer,
        "structured_response": structured_response,
        "feedback": feedback,
    }


def write_trace_record(record: Dict[str, Any]) -> Dict[str, Any]:
    if not _trace_enabled():
        return {"logged": False, "reason": "disabled"}
    path = _trace_path()
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, default=str) + "\n")
    return {"logged": True, "path": path}
