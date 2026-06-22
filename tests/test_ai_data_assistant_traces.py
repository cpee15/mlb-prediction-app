import json

from mlb_app.ai_data_assistant_traces import build_trace_record, write_trace_record


def test_build_trace_record_has_training_fields():
    record = build_trace_record(
        message="What is the strongest model edge?",
        intent="best_model_edges",
        packet_preview={"intent": "best_model_edges", "date": "2026-06-22"},
        deterministic_answer="deterministic",
        llm_answer="llm",
        structured_response={"intent": "best_model_edges", "answer": "final"},
    )
    assert record["user_message"] == "What is the strongest model edge?"
    assert record["classified_intent"] == "best_model_edges"
    assert "compact_evidence_packet" in record
    assert "feedback" in record
    assert "user_follow_up" in record["feedback"]


def test_write_trace_record_disabled_by_default(monkeypatch):
    monkeypatch.delenv("AI_DATA_ASSISTANT_TRACE_LOG_ENABLED", raising=False)
    result = write_trace_record({"hello": "world"})
    assert result["logged"] is False


def test_write_trace_record_enabled(monkeypatch, tmp_path):
    path = tmp_path / "assistant-traces.jsonl"
    monkeypatch.setenv("AI_DATA_ASSISTANT_TRACE_LOG_ENABLED", "true")
    monkeypatch.setenv("AI_DATA_ASSISTANT_TRACE_LOG_PATH", str(path))
    result = write_trace_record({"hello": "world"})
    assert result["logged"] is True
    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert json.loads(lines[-1]) == {"hello": "world"}
