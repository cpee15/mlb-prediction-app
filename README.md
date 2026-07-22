# MLB Prediction App

A production full-stack MLB data, matchup, projection, and sportsbook analysis platform hosted at [mlbgpt.com](https://mlbgpt.com).

The application ingests official MLB schedule and roster data, Statcast/Baseball Savant data, sportsbook feeds, and locally persisted model features. A FastAPI backend serves normalized contracts to a React frontend deployed as a separate Railway service.

---

## Production Architecture

This repository deploys as **two independent Railway services**.

| Service | Runtime | Role | Typical domain |
|---|---|---|---|
| `mlb-prediction-app` | Docker, Python 3.11, Uvicorn | FastAPI backend, data services, model routes, refresh endpoints | Railway service domain |
| Frontend | Railpack, Node, Vite | React single-page application | `mlbgpt.com` |

The frontend calls the backend through `VITE_API_BASE_URL`, which must be set in the **frontend Railway service at build time**. Relative API URLs are not a valid production fallback because the frontend service does not own the FastAPI routes.

The backend uses `DATABASE_URL` for SQLAlchemy persistence. PostgreSQL is the production database. SQLite fallback exists for local development and tests and must not be relied on in production.

### CORS requirements

The backend must allow:

- `https://mlbgpt.com`
- `https://www.mlbgpt.com`
- Railway service domains through the configured `allow_origin_regex`

Do not restrict CORS to only the custom frontend domain; direct Railway service access is required for deployment checks and internal service communication.

---

## Golden Matchup Analyzer Path

The daily matchup analyzer is the current production reference path and should be preserved unless direct evidence proves it is broken.

The expected execution path is:

1. Load the official MLB schedule for the requested `YYYY-MM-DD` date.
2. Resolve `game_pk`, teams, team IDs, game status, and probable or assigned starters.
3. Map teams and pitchers into persisted historical, rolling, split, arsenal, and Statcast data.
4. Build validated matchup records with stable backend-owned fields.
5. Return a valid empty slate when no games are scheduled.
6. Exclude or explicitly flag incomplete matchups rather than inventing teams, pitchers, odds, or statistics.

Primary implementation:

- `mlb_app/matchup_generator.py`
- `generate_matchups_for_date(...)`
- `GET /matchups?date=YYYY-MM-DD`
- `GET /matchup/{game_pk}`
- `GET /matchup/{game_pk}/competitive`
- `frontend/src/pages/HomePage.jsx`
- `frontend/src/pages/MatchupDetailPage.jsx`
- `frontend/src/pages/CompetitiveAnalysisPage.jsx`

`frontend/src/pages/MatchupDetailPage.jsx` is the current chart-rich production matchup-detail component. Do not replace it with an alternate shell or simplified page without regression evidence and explicit chart/render verification.

---

## Current Product Surfaces

| Surface | Frontend route | Frontend component | Backend area |
|---|---|---|---|
| Daily Matchups | `/` | `HomePage.jsx` | `mlb_app/app.py`, `matchup_generator.py` |
| Matchup Detail | `/matchup/:game_pk` | `MatchupDetailPage.jsx` | `GET /matchup/{game_pk}` |
| Competitive Analysis | `/matchup/:game_pk/competitive` | `CompetitiveAnalysisPage.jsx` | competitive matchup route |
| Daily Odds | `/daily-odds` | `DailyOddsPage.jsx` | `mlb_app/daily_odds_routes.py` |
| Bet105 Sportsbook | `/sportsbook/bet105` | `Bet105SportsbookPage.jsx` | `mlb_app/sportsbook_routes.py` and KIBL provider modules |
| Model Projections | `/models/projections` | `ModelProjectionsPage.jsx` | `mlb_app/model_projection_routes.py` |
| My Dashboard | `/my-dashboard` | `MyDashboardReportBuilderPage.jsx` | `mlb_app/my_dashboard_routes.py`, `my_dashboard_solver.py` |
| MLBGPT Control Center | `/admin` (private, owner only) | `AdminControlCenterPage.jsx` | `mlb_app/admin_routes.py`, `admin_access.py` |
| AI Data Assistant | `/ai-data-assistant` | `AIPage.jsx` | `mlb_app/ai_data_assistant_routes.py` |
| Model Tracker | `/model-tracker` | `ModelTrackerPage.jsx` | `mlb_app/model_tracker_routes.py` |
| News | `/news` | `NewsPageClean.jsx` | `mlb_app/news_routes.py` |
| Live Games | `/live`, `/live/:game_pk` | live scoreboard/game pages | live routes in `mlb_app/app.py` |
| Pitchers | `/pitcher`, `/pitcher/:id` | `PitcherPage.jsx` | pitcher routes and profile stores |
| Batters | `/batter`, `/batter/:id` | feature-gated Batter pages | `mlb_app/batter_routes.py` |
| Teams | `/team`, `/team/:id` | `TeamPage.jsx` | team routes in `mlb_app/app.py` |
| Standings | `/standings` | `StandingsPage.jsx` | standings route |
| Calendar | `/calendar` | `YesterdayTodayPage.jsx` | matchup/calendar routes |

The Batter frontend remains controlled by `VITE_ENABLE_BATTER_PAGE`. Keep it disabled until the production leaderboard and rolling-data contracts are verified.

---

## Backend Structure

The backend application is created in `mlb_app/app.py`. Older core routes remain there, while newer product surfaces use feature routers.

```text
mlb_app/
├── app.py                         # FastAPI app bootstrap and core route families
├── database.py                    # SQLAlchemy models, engine/session helpers
├── db_utils.py                    # Database query helpers
├── matchup_generator.py           # Daily matchup assembly
├── matchup_analysis.py            # Matchup analysis composition
├── scoring.py                     # Matchup scoring and probability helpers
├── daily_odds_routes.py           # Daily Odds aggregate contract
├── daily_odds_models.py           # Game and prop model builders
├── model_projection_routes.py     # Model Projections API
├── my_dashboard_routes.py         # Dashboard/report API routes
├── my_dashboard_solver.py         # Dashboard component ranking and report inputs
├── ai_data_assistant_routes.py    # AI Data Assistant routes
├── sportsbook_routes.py           # Sportsbook and Bet105 routes
├── model_tracker_routes.py        # Historical model result tracking
├── batter_routes.py               # Batter endpoints
├── batter_data_contract.py        # Official stats vs local Statcast contract
├── news_routes.py                 # News API
├── shared_payload_cache.py        # Shared artifact cache utilities
├── pitcher_profile_store.py       # Pitcher overview/arsenal/recent-game serializers
├── starting_pitcher_arsenal_refresh.py
└── simulation/                    # Game and inning simulation code
```

### FastAPI entry point

Local and Railway-compatible startup:

```bash
uvicorn mlb_app.app:app --host 0.0.0.0 --port 8000
```

`main.py` is retained as the repository-level production entry point where required by deployment configuration.

---

## Frontend Structure

The frontend is React 18, Vite, and React Router 6.

```text
frontend/
├── src/
│   ├── App.jsx                    # Route map and navigation
│   ├── lib/api.js                 # Shared API client and browser-cache behavior
│   ├── pages/
│   │   ├── HomePage.jsx
│   │   ├── MatchupDetailPage.jsx
│   │   ├── CompetitiveAnalysisPage.jsx
│   │   ├── DailyOddsPage.jsx
│   │   ├── Bet105SportsbookPage.jsx
│   │   ├── ModelProjectionsPage.jsx
│   │   ├── MyDashboardReportBuilderPage.jsx
│   │   ├── AIPage.jsx
│   │   ├── ModelTrackerPage.jsx
│   │   └── NewsPageClean.jsx
│   └── utils/
└── package.json
```

Do not add route-specific interception logic to the generic `fetchJson(...)` helper. Surface-specific request sequencing belongs in the page or dedicated API service so unrelated endpoints keep predictable semantics.

Authenticated or session-specific My Dashboard responses must not use a global shared browser cache unless the cache key includes the complete user/session scope.

---

## Data Sources

| Source | Use |
|---|---|
| MLB Stats API | schedule, game IDs, teams, probable pitchers, lineups, rosters, standings, official player statistics |
| Baseball Savant / Statcast | pitch and batted-ball events, arsenals, velocity, movement, xwOBA, hard-hit and barrel metrics |
| `pybaseball` | bulk Statcast retrieval and supported Savant datasets |
| PostgreSQL | persisted Statcast events, aggregates, profiles, solver inputs, model and tracking data |
| DraftKings provider integration | Daily Odds events, markets, selections, prices |
| KIBL Bet105 feed | Bet105 events, markets, selections, prices, sportsbook display |
| Internal simulation/model modules | matchup probabilities, expected runs, diagnostic simulations, ranked dashboard/model outputs |

Never fabricate provider data. A missing sportsbook payload must remain distinguishable from a valid priced market.

---

## Data Contract Rules

### Matchups

The backend owns the matchup contract. Frontend code should not invent missing team names, starters, IDs, scores, or probabilities.

Core matchup identity fields should include, when available:

- `game_pk`
- requested game date
- away/home team names and IDs
- away/home pitcher names and IDs
- game status
- model probabilities and their source
- data-quality or missing-input metadata

### Daily Odds

Daily Odds combines matchup data, sportsbook events, model builders, My Dashboard summaries, and Model Projection summaries.

Model-only fallback rows may be returned when sportsbook events or prices are unavailable. Those rows must retain:

- `event_id: null`
- `line: null`
- `price: null`
- `market_implied_probability: null`
- `odds_missing: true` or equivalent missing-input metadata
- a visible `source` identifying the internal model-only path

The frontend must label these as **model-only** or **watchlist** signals. They must not be rendered as priced sportsbook bets, expected-value plays, or verified edges.

### Probability fields

Several probability concepts can coexist:

- canonical final model probability
- base simulation diagnostic probability
- bullpen-adjusted simulation diagnostic probability
- sportsbook implied probability

Do not label all of them generically as “Win Probability.” Preserve explicit source labels in API responses and UI components.

### My Dashboard

Current solver components include:

- `hitters`
- `pitchers`
- `teams`
- `totals`
- `overall_players`

Dashboard/report rows may include:

- rank and entity identity
- team, opponent, and `game_pk`
- score and confidence
- category and reasoning
- metric dictionary
- source and missing-data metadata
- batter-vs-arsenal pitch angles
- confirmed-lineup metadata

The report builder supports newer `report_view` saved items while retaining compatibility with earlier `workbench_view` items. Treat this as a compatibility contract until a deliberate migration is completed.

---

## Date Handling

Every public date parameter uses `YYYY-MM-DD`.

Explicit user-supplied dates must be preserved exactly. Implicit concepts such as “today,” “yesterday,” default slate date, and cron hydration date must use one documented MLB business timezone rather than raw server or UTC calendar dates.

Railway containers may run in UTC. Do not use an unqualified `datetime.date.today()` for production MLB date selection without confirming the intended business timezone.

This rule applies to:

- homepage/default matchup date
- calendar windows
- Daily Odds defaults
- Model Projection defaults
- My Dashboard hydration
- refresh and warm scripts
- model result grading dates

Generated timestamps may remain UTC when clearly suffixed with `Z` and distinguished from the MLB slate date.

---

## Cache and Refresh Behavior

The repository contains several cache layers:

- process-local live response cache
- matchup snapshot cache
- shared payload/artifact cache
- frontend/browser cache
- local-storage dashboard/report state

Cache keys must include every value that changes response meaning, including:

- route and contract version
- target MLB date
- filters and component type
- provider
- authentication/session scope

Process-local caches are not authoritative across Railway workers, restarts, or deployments. Database refresh success does not automatically invalidate browser or in-memory payloads.

Known refresh and warming responsibilities include:

- matchup snapshots
- model projection warming
- starting-pitcher arsenal refresh
- My Dashboard solver hydration, including the `hydrate-yesterday` workflow
- provider-specific odds refreshes

A refresh job is not considered healthy because it receives HTTP 200 alone. Operational logs should include target date, data source, row/artifact count, duration, cache key, and completion status.

Cron jobs must call the deployed backend service domain. Do not call `127.0.0.1` unless the cron process actually starts the API server in the same container.

---

## Environment Variables

### Required production variables

| Variable | Service | Purpose |
|---|---|---|
| `DATABASE_URL` | Backend and refresh jobs | Production PostgreSQL connection |
| `VITE_API_BASE_URL` | Frontend build | FastAPI service base URL |

### Important feature/provider variables

The exact provider modules are authoritative, but production deployments may also require variables for:

- KIBL Cognito/Bet105 authentication
- odds-provider credentials or feed configuration
- cache TTL values
- news/Twitter provider selection
- frontend feature flags such as `VITE_ENABLE_BATTER_PAGE`

Never commit secrets, tokens, passwords, cookies, or production connection strings.

A production backend should fail clearly when `DATABASE_URL` is missing or points to SQLite. Local development and tests may continue to use SQLite.

---

## Local Development

### Backend

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export DATABASE_URL=postgresql://user:pass@localhost:5432/mlb
uvicorn mlb_app.app:app --reload --port 8000
```

For local-only work, omitting `DATABASE_URL` may use the repository SQLite fallback.

### Frontend

```bash
cd frontend
npm install
echo "VITE_API_BASE_URL=http://localhost:8000" > .env.local
npm run dev
```

The default Vite development URL is `http://localhost:5173`.

---

## Validation Before Merge

Run the checks relevant to the changed area. At minimum, backend changes should prove the package imports and frontend changes should prove the SPA builds.

```bash
python -m compileall mlb_app
python -c "from mlb_app.app import app"
pytest

cd frontend
npm install
npm run build
```

For production-path work, verify exact response contracts for an explicit MLB date rather than only checking for HTTP 200.

Recommended smoke matrix:

1. `GET /health`
2. matchups for a known game date
3. one chart-rich matchup detail
4. Model Projections for the same date
5. Daily Odds with and without sportsbook events
6. My Dashboard solver/report generation
7. AI Data Assistant request
8. Bet105 event/market normalization
9. refresh job output and freshness metadata

---

## Contribution Rules

- Preserve the golden matchup analyzer and chart-rich Matchup Detail path.
- Do not assume a route exists; verify its decorator and router registration.
- Do not add a new endpoint when an existing route or contract can be extended safely.
- Keep business logic out of `mlb_app/app.py` when adding new feature code.
- Avoid broad rewrites of working production paths.
- Do not touch files known to be truncated or incompletely retrieved.
- Do not hide provider, database, or model failures behind plausible empty data.
- Keep missing/null fields semantically distinct from zero.
- Add tests for new modules, response contracts, cache behavior, and date rollover.
- Use separate PRs for schema migration, formula changes, deployment changes, and UI redesigns.

### Branch naming

```text
feature/<short-description>
fix/<short-description>
refactor/<short-description>
agent/<short-description>
```

### PR checklist

- [ ] Scope is narrow and production behavior is preserved
- [ ] Backend imports successfully
- [ ] Relevant tests pass
- [ ] Frontend builds when frontend code changed
- [ ] Explicit MLB date tested
- [ ] Null, empty, error, and degraded-provider states tested
- [ ] `DATABASE_URL` and `VITE_API_BASE_URL` assumptions verified
- [ ] Cache keys and invalidation reviewed
- [ ] Probability/model source labels remain explicit
- [ ] No secrets or production tokens committed

---

## Deployment Notes

Pushing or merging to `main` may trigger independent backend and frontend deployments. A successful backend deploy does not prove the frontend rebuilt with the correct API base URL, and a successful frontend deploy does not prove the database, odds provider, cron jobs, or warmed artifacts are healthy.

Production acceptance should verify:

- backend health endpoint
- frontend-to-backend connectivity
- PostgreSQL connection
- current MLB slate date
- matchup completeness
- provider status
- cache freshness
- refresh-job completion
- one representative render for every major product surface
