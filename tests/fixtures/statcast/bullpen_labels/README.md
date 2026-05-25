# Candidate Bullpen Statcast Label Fixtures

This directory is a scaffold for future fixture-backed candidate bullpen Statcast label replay.

This layer intentionally contains no per-date fixture payload rows. The `dates/` directory is present only as a target location for future JSONL fixture files.

Safety guarantees for this scaffold:
- scaffold-only
- no fixture payload rows
- no external fetch
- no database writes
- no production route, sportsbook, frontend, simulation, or canonical probability coupling

Future layers will add:
1. fixture payload files,
2. fixture corpus audits,
3. fixture replay adapters,
4. replay parity audits,
5. live-fetch dry-run adapters only after fixture replay passes.
