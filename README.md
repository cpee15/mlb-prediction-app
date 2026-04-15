# MLB Prediction App

This repository contains a work‑in‑progress implementation of an advanced MLB match‑ups and prediction engine.  The goal of this project is to ingest detailed baseball data from public APIs (including the **MLB Stats API** and **Statcast**) and compute rich feature vectors for each daily matchup.

## Features

- **Daily schedule ingestion**: Fetch the list of scheduled games, teams and probable pitchers for a given date.
- **Team standings and records**: Retrieve wins, losses and run differential for each team in the current season.
- **Platoon splits**: Collect hitting statistics for teams and players vs left‑ and right‑handed pitching, yielding metrics such as K%, BB%, ISO and wOBA.
- **Statcast aggregation**: Calculate advanced pitcher and batter metrics such as average velocity, spin rate, hard‑hit percentage, barrel rate, strikeout and walk rates, average exit velocity and launch angle.  These aggregations can be computed over rolling windows (e.g. last 3, 6, 9 or 12 months) or per season.
- **Matchup pipeline**: Assemble all of the above data into a feature vector for each scheduled game.  The pipeline is designed to be extensible, allowing future integration of pitch‑arsenal data, count‑based splits and machine‑learning models.

## Repository structure

- **mlb_app/main.py** – A simple script that demonstrates fetching the daily schedule and estimating win probabilities using basic team records.  This serves as a baseline example.
- **mlb_app/data_ingestion.py** – Functions to fetch schedules, team standings and team hitting splits from the MLB Stats API.
- **mlb_app/player_splits.py** – Helper functions to retrieve individual player splits vs left‑ and right‑handed pitching.
- **mlb_app/statcast_utils.py** – Utility functions to aggregate raw Statcast data for pitchers and hitters into useful metrics.  Note: the functions that fetch Statcast data are placeholders, as direct Statcast downloads require an accessible endpoint.
- **mlb_app/pitcher_analysis.py** – Wraps `statcast_utils` to compute pitcher metrics given a date range or pre‑fetched raw data.
- **mlb_app/batter_analysis.py** – Wraps `statcast_utils` to compute batter metrics given a date range or pre‑fetched raw data.
- **mlb_app/analysis_pipeline.py** – Orchestrates the data ingestion and aggregation functions to produce a list of matchup feature dictionaries for a given date.
- **generate_matchups.py** – Command‑line utility that accepts a date and prints the generated matchups in JSON format.

## Usage

1. **Clone this repository** and install Python 3.9+.
2. Install required dependencies (if any) listed in a `requirements.txt` file (not yet provided).  The current scripts rely only on Python’s standard library for HTTP requests and JSON handling.
3. Run the CLI to generate today’s matchups:

```bash
python generate_matchups.py
```

To specify a different date (YYYY‑MM‑DD), use the `--date` flag:

```bash
python generate_matchups.py --date 2026‑04‑15
```

The command prints a JSON array of matchup objects, each containing fields like home and away team win/loss records, run differential, splits and aggregated metrics.  Note that some fields (such as pitcher and batter Statcast metrics) may be empty if the corresponding data retrieval functions have not yet been implemented.

## Roadmap

The current implementation provides the skeleton of a data pipeline.  Future work includes:

- Implementing full Statcast data retrieval for pitchers and hitters, using either the official Statcast API or a proxy service.
- Downloading and aggregating Baseball Savant pitch‑arsenal CSVs to compute career and seasonal pitch mix effectiveness.
- Incorporating count‑based splits and additional contextual factors (e.g. weather, park factors).
- Building a machine‑learning model to translate the feature vectors into win probabilities or projected run totals.
- Developing a web‑based or ChatGPT‑plugin interface to view matchups and predictions interactively.

This project is under active development and contributions are welcome.
