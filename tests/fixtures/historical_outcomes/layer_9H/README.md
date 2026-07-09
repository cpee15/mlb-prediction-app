# Layer 9H Historical Outcome Fixture Corpus

Corpus version: `layer_9H_historical_outcome_fixture_corpus_v1`

This deterministic local corpus implements the 30 scenarios planned by Layer
9G and replays them through the Layer 9F provider adapter and Layer 9D
historical outcome contract.

## Artifacts

- `manifest.json`
- `schema.json`
- `provider_payloads.jsonl`
- `expected_adapter_outputs.jsonl`
- `expected_outcome_records.jsonl`
- `fixture_index.csv`
- `README.md`

## Authority boundary

This corpus performs no external fetches, production collection, production
materialization, feature/outcome joins, prediction joins, predictive metric
calculation, model tuning, market comparison, pricing, edge detection, or
betting recommendation.
