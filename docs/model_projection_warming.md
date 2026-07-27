# Model Projection Game-Day Warming

Model Projections serves precomputed artifacts and does not run heavyweight
simulation work inside a user GET request.

## Railway service

Create a dedicated Railway cron service using
`railway.game-day-warm-cron.json`. Configure its cron schedule as:

`0 * * * *`

The service calls the production web service and warms yesterday, today, and tomorrow. The 180-second per-request timeout covers the canonical production
trial batch without reverting the public GET route to cold computation.

The warmer must run after every deployment and hourly during game days.
A successful model-projection snapshot reports `warmed: true` and a positive
`games_cached` count when MLB games exist for the date.

## Read behavior

`GET /models/projections` remains read-only. When an artifact is unavailable,
it returns `data_status: not_ready` and an explanatory message. The frontend
must not persist that transient response in its 30-minute browser cache.
