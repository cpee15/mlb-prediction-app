# MLB Prediction App — Architecture Notes

## CRITICAL: Two Separate Railway Services

This project has **two independent Railway services**. Forgetting this breaks the app.

| Service | Builder | Role | Domain |
|---------|---------|------|--------|
| `mlb-prediction-app` | Dockerfile | FastAPI backend + API | backend `*.up.railway.app` URL |
| Frontend service | Railpack (Node) | React SPA | `mlbgpt.com` (custom domain) |

The frontend calls the backend via `VITE_API_BASE_URL` (set in Railway env vars at build time).
If `VITE_API_BASE_URL` is unset, API calls fall back to relative URLs — which **breaks** because
the frontend service has no API routes.

## CORS Policy

The backend (`mlb_app/app.py`) must allow:
1. `https://mlbgpt.com` and `https://www.mlbgpt.com` — the live custom domain
2. `https://*.up.railway.app` — the frontend's Railway-generated URL (used internally and during deploys)

This is handled via `allow_origin_regex=r"https://.*\.up\.railway\.app"` + explicit origin list.
**Never restrict CORS to only the custom domain — the Railway service URL must always be allowed.**

## Stack

- **Backend**: Python 3.11, FastAPI, SQLAlchemy, PostgreSQL (SQLite fallback), Uvicorn
- **Frontend**: React 18, Vite, React Router

## Deployment

- Push to `main` triggers GitHub Actions → `railway up --detach --service mlb-prediction-app`
- Frontend service deploys automatically via Railway's Railpack on push to `main`
