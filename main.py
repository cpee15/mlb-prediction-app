#!/usr/bin/env python3
"""Entry point for Railway and other platforms that auto-detect main.py."""

import os
import uvicorn

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("mlb_app.app:app", host="0.0.0.0", port=port)
