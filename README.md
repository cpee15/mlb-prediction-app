# MLB Prediction App

A production full-stack MLB matchup and prediction engine. Data is ingested from the **MLB Stats API** and **Baseball Savant / Statcast**, stored in PostgreSQL, and served through a **FastAPI** backend to a **React 18** frontend hosted at [mlbgpt.com](https://mlbgpt.com).

---

## Matchup Analyzer — Current Production Reference

The matchup analyzer is now live in production and should be treated as the current known-good behavior for daily game analysis. This section exists to preserve, in plain English, what the analyzer is doing right now so future changes can always be measured against a written baseline. The goal is simple: if the analyzer ever drifts, breaks, or starts returning incomplete matchup cards, this description should make it obvious what changed.

At a high level, the analyzer pulls the daily MLB schedule, resolves each game into a structured matchup object, validates that both teams and both expected starting pitchers can be tied back to internal data, and then returns those matchups through the production API for the frontend to render. It is not intended to guess, partially fill, or fabricate missing core matchup data. A matchup should only move forward when the required game-level identity fields are present and usable.

The analyzer begins with the official game schedule for the requested date. From that schedule it identifies every scheduled game and extracts the base metadata that defines a matchup: game ID, date, away team, home team, team IDs, probable or assigned starting pitchers, and game status. That core layer is the foundation for everything else in the system. If the analyzer cannot reliably resolve those values, it should fail that matchup cleanly rather than emit a misleading or half-built result.

Once the schedule layer is resolved, the analyzer maps the teams and pitchers into the internal data model used by the rest of the app. This includes linking the matchup to database-backed historical statistics, rolling performance views, player split data, and any supporting aggregation used by the scoring and analysis pipeline. Pitcher identity resolution is especially important here. The analyzer must correctly associate the live game pitcher with the stored player record so downstream pages, matchup detail views, and scoring logic all stay aligned.

After identity resolution succeeds, the analyzer assembles a finalized matchup object that the API can return consistently. That object should be stable enough to support the homepage matchup cards, game detail pages, competitive lineup views, and downstream scoring functions without requiring the frontend to invent missing values. In other words, the backend is responsible for giving the frontend a clean, trustworthy matchup payload.

In production, this means the matchup analyzer is expected to do the following every day: retrieve the correct game slate, identify both clubs in every game, resolve both starting pitchers when available, connect those entities to the internal statistical system, assemble complete matchup records, and expose those records through the live API in a structure the frontend can render directly. When no games are scheduled, the analyzer should return a valid empty result, not a broken response. When a specific matchup cannot be built correctly, it should be excluded or flagged cleanly rather than degrade the full slate.

This is the reference behavior that should be preserved. Future refactors can improve speed, coverage, and depth, but they should not change the core expectation that the analyzer returns validated, production-safe matchup objects built from real scheduled games, correctly resolved teams and pitchers, and internally consistent data links across the rest of the application.

---

## Architecture — Two Separate Railway Services

This project deploys as **two independent Railway services**. Understanding this is mandatory before contributing.

| Service | Builder | Role | Domain |
|---------|---------|------|--------|
| `mlb-prediction-app` | Dockerfile | FastAPI backend + API | `*.up.railway.app` |
| Frontend | Railpack (Node) | React SPA | `mlbgpt.com` |

The frontend calls the backend via `VITE_API_BASE_URL` (set in Railway env vars at build time). If `VITE_API_BASE_URL` is unset, API calls fall back to relative URLs — which **breaks** because the frontend service has no API routes.

### CORS Policy

The backend (`mlb_app/app.py`) allows:
- `https://mlbgpt.com` and `https://www.mlbgpt.com`
- `https://*.up.railway.app` via `allow_origin_regex`

**Never restrict CORS to only the custom domain.** The Railway service URL must always be allowed.

---

## Stack

| Layer | Technology |
|-------|-----------|
| Backend | Python 3.11, FastAPI, Uvicorn |
| ORM / DB | SQLAlchemy 2.x, PostgreSQL (SQLite fallback for local) |
| Data | pybaseball (Statcast), MLB Stats API (`statsapi.mlb.com`) |
| Frontend | React 18, Vite, React Router 6 |
| Deployment | Docker (backend), Railpack/Node (frontend), Railway, GitHub Actions |

---

## Repository Structure

```
mlb-prediction-app/
├── mlb_app/                    # Core Python package
│   ├── app.py                  # FastAPI application — all API routes
│   ├── database.py             # SQLAlchemy ORM models
│   ├── db_utils.py             # Database query helpers
│   ├── etl.py                  # ETL pipeline (Statcast, arsenal, splits → DB)
│   ├── matchup_generator.py    # Assembles game-level feature vectors from DB
│   ├── scoring.py              # Matchup scoring engine / win probability
│   ├── aggregation.py          # Rolling-window and seasonal stat aggregation
│   ├── data_ingestion.py       # MLB Stats API wrappers (schedule, standings, splits)
│   ├── statcast_utils.py       # Statcast retrieval and aggregation (pybaseball)
│   ├── pitcher_analysis.py     # Pitcher metric retrieval helpers
│   ├── batter_analysis.py      # Batter metric retrieval helpers
│   ├── player_splits.py        # Player splits vs L/R pitching
│   ├── analysis_pipeline.py    # Matchup analysis orchestration
│   └── hitter_profile.py       # Hitter profile scaffold (in progress)
├── frontend/
│   ├── src/
│   │   ├── App.jsx             # Root component + routing
│   │   ├── pages/
│   │   │   ├── HomePage.jsx                # Daily matchups
│   │   │   ├── MatchupDetailPage.jsx       # Single-game drill-down
│   │   │   ├── CompetitiveAnalysisPage.jsx # Lineup-vs-pitcher matrix
│   │   │   ├── PitcherPage.jsx             # Pitcher profile + arsenal
│   │   │   ├── RollingPitcherPage.jsx      # Pitcher rolling stats (L15G–L150G)
│   │   │   ├── BatterPage.jsx              # Batter profile + platoon splits
│   │   │   ├── RollingBatterPage.jsx       # Batter rolling stats (L10–L1000 ABs)
│   │   │   ├── TeamPage.jsx                # Team vsL/vsR splits + standings
│   │   │   ├── StandingsPage.jsx           # AL/NL standings
│   │   │   ├── YesterdayTodayPage.jsx      # Calendar view (yesterday/today/tomorrow)
│   │   │   └── AIPage.jsx                  # Lightweight MLB Q&A assistant
│   │   └── utils/
│   │       └── formatters.js   # Shared number/percent/date formatters
│   ├── index.html
│   └── package.json
├── main.py                     # Uvicorn entry point for Railway
├── seed_db.py                  # Bootstrap: loads last N days of Statcast into DB
├── generate_matchups.py        # CLI: prints matchups JSON for a given date
├── Dockerfile                  # Multi-stage build (Python 3.11 + Node 20)
├── railway.json                # Railway deploy config (healthcheck, restart policy)
├── CLAUDE.md                   # Architecture notes for AI-assisted development
└── requirements.txt            # Python dependencies
```

---

## API Endpoints

All endpoints are served by `mlb_app/app.py`.

### Health
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Health check |

### Matchups / Schedule
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/matchups` | List games for a date (`?date=YYYY-MM-DD`) |
| `GET` | `/matchups/calendar` | Yesterday / today / tomorrow snapshot |
| `POST` | `/matchups/snapshot/{date_str}` | Cache matchups for a specific date |
| `GET` | `/matchup/{game_pk}` | Full game detail (pitchers, lineups, splits, game log) |
| `GET` | `/matchup/{game_pk}/competitive` | Lineup-level competitive matchup matrix |

### Pitchers
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/pitcher/{id}` | Aggregate stats + pitch arsenal |
| `GET` | `/pitcher/{id}/rolling` | Rolling stats (L15G–L150G) |
| `GET` | `/pitcher/{id}/game-log` | Recent game-by-game appearances |

### Batters
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/batter/{id}` | Aggregate stats + platoon splits |
| `GET` | `/batter/{id}/rolling` | Rolling stats (L10, L25, L50, L100, L200, L400, L1000 ABs) |
| `GET` | `/batter/{id}/splits` | Multi-season vsL/vsR splits |
| `GET` | `/batter/{id}/at-bats` | Chronological Statcast-level at-bat log |

### Teams / Standings / Rosters
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/standings` | AL/NL standings |
| `GET` | `/team/{team_id}` | Team splits (vsL/vsR) + standings |
| `GET` | `/team/{team_id}/roster` | Full active roster |
| `GET` | `/lineup/{team_id}` | Day-of lineup (`?date=YYYY-MM-DD`) |

### Players
| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/players/search` | Search by name (`?name=...`) |
| `GET` | `/players/all` | All active MLB players (`?season=YYYY`) |

### AI / Prediction
| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/ai/ask` | Lightweight MLB data Q&A assistant |
| `POST` | `/predict` | Score a specific pitcher vs batter matchup |

---

## Local Development

### Backend

```bash
# 1. Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set environment variables
#    DATABASE_URL  — PostgreSQL connection string (omit to use SQLite fallback)
#    VITE_API_BASE_URL — only needed when building the frontend
export DATABASE_URL=postgresql://user:pass@localhost:5432/mlb

# 4. Seed the database with recent Statcast data
python seed_db.py

# 5. Start the API server
uvicorn mlb_app.app:app --reload --port 8000
```

The API will be available at `http://localhost:8000`. Interactive docs at `http://localhost:8000/docs`.

### Frontend

```bash
cd frontend

# Install Node dependencies
npm install

# Set the API base URL to point at your local backend
echo "VITE_API_BASE_URL=http://localhost:8000" > .env.local

# Start the dev server
npm run dev
```

The frontend dev server runs at `http://localhost:5173`.

---

## Deployment

Pushing to `main` triggers two automatic deploys:

1. **Backend** — GitHub Actions runs `railway up --detach --service mlb-prediction-app`, which builds and deploys the Dockerfile.
2. **Frontend** — Railway's Railpack detects Node and deploys the React SPA automatically.

The `VITE_API_BASE_URL` environment variable **must** be set in the Railway frontend service's env vars before deploying, or the frontend will break.

---

## Contributing

Before opening a PR, read this entire README and `CLAUDE.md`.

### Branch naming

Use descriptive prefixes:

```
feature/<short-description>
fix/<short-description>
refactor/<short-description>
```

### Code conventions

- **Python**: Follow the existing module structure. New backend modules go in `mlb_app/`. Keep logic out of `app.py` — routes should call helpers, not contain business logic inline.
- **Comments**: Only add a comment when the *why* is non-obvious. Do not write docstrings that restate what the function name already says. Do not write multi-paragraph docstrings for placeholder or scaffold code.
- **Parameters**: Do not define function parameters that are not used. If a function is a scaffold, either omit the parameter until it is needed or use it.
- **No dead code**: Do not merge modules or functions that are entirely placeholder (returning `None` for every field). At minimum, implement enough logic to be testable.
- **Tests**: Every new module must include a corresponding test file in `tests/`. There is currently no test suite — new contributions are expected to establish one.
- **Trailing newline**: All Python files must end with a newline character.

### PR checklist

- [ ] New Python files end with a trailing newline
- [ ] No unused function parameters
- [ ] No overly verbose docstrings on scaffold/placeholder code
- [ ] A test file exists for every new module (`tests/test_<module>.py`)
- [ ] If touching `mlb_app/app.py` CORS config, review `CLAUDE.md` first
- [ ] If touching the frontend build or `VITE_API_BASE_URL`, verify the two-service deploy still works

---

## Data Sources

| Source | Used For |
|--------|---------|
| [MLB Stats API](https://statsapi.mlb.com) | Schedule, standings, rosters, lineups, player splits |
| [Baseball Savant / Statcast](https://baseballsavant.mlb.com) | Pitch velocity, spin rate, exit velocity, barrel rate, pitch arsenal CSVs |
| [pybaseball](https://github.com/jldbc/pybaseball) | Python wrapper for Statcast bulk downloads |
