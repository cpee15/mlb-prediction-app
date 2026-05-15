from __future__ import annotations

from .app import app
from .news_routes import router as news_router

try:
    app.include_router(news_router)
except Exception:
    # Keep the production app import-safe even if optional news dependencies fail.
    pass
