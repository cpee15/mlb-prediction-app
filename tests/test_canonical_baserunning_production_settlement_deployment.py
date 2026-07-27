import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "railway.baserunning-settlement-cron.json"
DOCS_PATH = ROOT / "docs" / "canonical_baserunning_production_monitoring.md"
RUNNER_PATH = ROOT / "scripts" / "run_canonical_baserunning_production_settlement.py"


def test_settlement_cron_uses_dedicated_runner():
    payload = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))

    assert payload["build"]["builder"] == "DOCKERFILE"
    assert payload["deploy"]["startCommand"] == (
        "python -m scripts.run_canonical_baserunning_production_settlement"
    )
    assert RUNNER_PATH.is_file()


def test_settlement_operations_are_documented():
    text = DOCS_PATH.read_text(encoding="utf-8")

    assert "0 * * * *" in text
    assert "DATABASE_URL" in text
    assert "MLB_CANONICAL_SETTLEMENT_ALLOW_SQLITE" in text
    assert "100 settled games" in text
    assert "does not automatically" in text
