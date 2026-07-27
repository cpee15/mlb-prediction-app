import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "railway.game-day-warm-cron.json"
DOCS = ROOT / "docs" / "model_projection_warming.md"
WARMER = ROOT / "scripts" / "warm_game_day.py"


def test_game_day_warmer_has_dedicated_railway_contract():
    payload = json.loads(
        CONFIG.read_text(encoding="utf-8")
    )
    command = payload["deploy"]["startCommand"]

    assert payload["build"]["builder"] == "DOCKERFILE"
    assert "scripts/warm_game_day.py" in command
    assert "--timeout 180" in command
    assert (
        "mlb-prediction-app-production-732c.up.railway.app"
        in command
    )
    assert WARMER.is_file()


def test_game_day_warming_operations_are_documented():
    text = DOCS.read_text(encoding="utf-8")

    assert "0 * * * *" in text
    assert "yesterday, today, and tomorrow" in text
    assert "data_status: not_ready" in text
    assert "30-minute browser cache" in text
