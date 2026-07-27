# Canonical Baserunning Production Monitoring

The calibrated canonical baserunning model is production authority while a
frozen 100-game evidence window is collected.

## Monitoring lifecycle

1. Model Projections stores an immutable eligible pregame observation.
2. A dedicated settlement cron checks pending observations.
3. Only games MLB marks Final are sourced from the MLB schedule and play-by-play feed.
4. Canonical parsing counts stolen bases and caught stealing.
5. Exactly one immutable settlement is stored for each game.
6. Diagnostics report pregame and settlement progress separately.
7. Parameter review remains locked until 100 unique games are settled.

Settlement does not rerun simulations, mutate pregame observations, change
production authority, or automatically tune the calibrated transform.

## Railway cron

Create a dedicated Railway cron service with config file
`railway.baserunning-settlement-cron.json` and schedule `0 * * * *`.

The service must share the production `DATABASE_URL`. Do not set
`MLB_CANONICAL_SETTLEMENT_ALLOW_SQLITE` in production.

The start command is
`python scripts/run_canonical_baserunning_production_settlement.py`.

The runner is idempotent. It processes only pending observations whose MLB
schedule state is Final and stores at most one settlement per game.

## Review boundary

The attempt multiplier and success-rate adjustment remain frozen throughout
the monitoring window. Reaching 100 settled games permits human review; it
does not automatically select or deploy new parameters.
