# DFS Simulation Engine Roadmap

## Vision

The simulator should produce coherent baseball plays, derive a projected box score from those plays, and calculate DFS projections from the resulting batter and pitcher statistics.

> The simulation produces baseball plays. Game state and box-score statistics are derived from those plays. DFS scoring is derived from the box score.

This extends the probability, matchup, park, weather, lineup, historical-evaluation, and Monte Carlo work already present in the repository. It does not replace that work.

## Architecture

```text
Existing probability providers
        ↓
Primary plate-appearance outcome
        ↓
Batted-ball and fielding context
        ↓
Legal runner-transition enumeration
        ↓
Deterministic and probabilistic runner movement
        ↓
Canonical play event
        ↓
Validated post-play state
        ↓
Box-score reducer
        ↓
Projection aggregation
        ↓
Projection API
        ↓
UI
```

The frontend must not reconstruct baseball rules. It should consume presentation-ready box-score and DFS projection payloads.

## Core contracts

### Game state

Each play begins with an explicit state containing inning, half-inning, outs, score, runner identity on each base, batter, pitcher, lineup position, simulation identity, event sequence, and deterministic seed.

### Play event

Each completed play records the pre-play and post-play state, primary outcome, batted-ball context when applicable, runner movements, outs, runs, errors, RBI and sacrifice attribution, and probability provenance.

### Event ledger

Each simulation maintains an append-only ledger. Replaying the ledger must reproduce the final game state, and rebuilding the box score must reproduce incremental accounting.

## Existing features to preserve

- batter and pitcher outcome probabilities;
- pitcher-versus-hitter and pitch-type matchup inputs;
- lineup order and active-player state;
- park and weather inputs;
- pitcher workload and availability;
- historical evaluation and probability provenance;
- Monte Carlo batching and random-seed behavior;
- configured DFS scoring rules.

The first implementation layers must preserve the existing primary plate-appearance probability model.

## Transition philosophy

Rules determine which movements are legal, forced, or prohibited. Probability models choose only among legal discretionary outcomes.

Deterministic behavior includes home-run scoring, forced advancement on walks and hit batters, lineup advancement, inning termination, and force/timing rules.

Initial probabilistic behavior should use explicit versioned league-average baselines for advancement on singles and doubles, tagging on caught flies, advancement on groundouts, extra-base attempts, and outs on bases. These can later be modified by runner speed, fielder arm, park, weather, score, inning, fatigue, and managerial behavior.

## DFS projection contract

The projected box score is the authoritative statistical output.

Batter projections should include plate appearances, at bats, singles, doubles, triples, home runs, walks, hit by pitch, strikeouts, runs, RBI, stolen bases, caught stealing, sacrifice flies, and DFS points.

Pitcher projections should include batters faced, outs, innings, hits, home runs, walks, hit batters, strikeouts, runs, earned runs, win probability, quality-start probability, saves or holds when applicable, and DFS points.

Across simulations, the system should expose means, medians, percentiles, standard deviations, event probabilities, DFS-point distributions, and relevant correlations.

## UI contract

The UI should distinguish between:

1. an aggregate projected box score across all simulations;
2. an individual representative simulated box score.

The matchup page should eventually display:

- projected score, win probability, run range, simulation count, and model version;
- batter projection table;
- pitcher projection table;
- projected team box score;
- DFS percentile and probability details;
- lineup, weather, fallback, and provenance status;
- optional representative simulation and play-by-play inspector.

The normal UI should receive aggregated projection payloads, not every event from every simulation. Event ledgers should remain available for replay, debugging, validation, and selected representative simulations.

## Invariants

- no runner occupies multiple bases;
- no base contains multiple runners;
- every run comes from explicit runner movement;
- every out comes from an explicit out record;
- force and timing rules determine whether runs count;
- lineup order advances exactly once per completed plate appearance;
- replay reproduces final state;
- box-score reconstruction reproduces live accounting;
- fixed seeds reproduce deterministic artifacts;
- every DFS statistic is supported by simulated events.

## Delivery roadmap

### Layer 10A — Event contract and roadmap

Define the game-state, play-event, runner-movement, box-score, API, UI, invariant, and migration contracts.

### Layer 10B — Play ledger and deterministic transitions

Implement runner-aware state, canonical events, explicit runner movements and outs, append-only ledger, deterministic walk, hit-by-pitch, and home-run transitions, lineup advancement, compatibility adapters, and replay validation.

### Layer 10C — Baseline batted-ball context

Add ground-ball, line-drive, fly-ball, popup, direction, depth, and contact-quality context.

### Layer 10D — Baseline runner advancement

Add versioned league-average advancement, tag-up, extra-base, and out-on-base probabilities.

### Layer 10E — Multi-out and scoring-rule realism

Add double plays, force plays, fielder's choices, sacrifice flies and bunts, errors, and timing rules.

### Layer 10F — Box-score reducer and replay validation

Build team, batter, and pitcher reducers and derive DFS scoring from reduced outputs.

### Layer 10G — Projection API and UI integration

Expose aggregate projected box scores, batter and pitcher projections, DFS distributions, metadata, representative simulations, and UI-ready status fields.

### Layer 10H — Calibration harness

Compare simulated baseball and DFS distributions with historical baselines.

### Layer 10I and beyond — Contextual realism

Add player baserunning, fielder arm and range, park, weather, fatigue, bullpen, defensive alignment, strategy, and improved earned-run reconstruction.

## Definition of success

The target architecture is reached when the simulator can generate a coherent chronological game, explain every run and out, replay its ledger into the same final state, rebuild the same box score, derive all DFS statistics from that box score, reproduce realistic aggregate distributions, and accept improved probability providers without rewriting baseball rules.
