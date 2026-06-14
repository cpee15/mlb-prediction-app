# Bet105 fixture-id market request

The confirmed request shape for selected MLB fixture markets is:

```json
{
  "feed_source_id": 171,
  "betting_type_id": 1,
  "sport_id": "2",
  "league_id": "7",
  "fixture_id": "<selected MLB fixture id>",
  "start_date": "YYYY-MM-DD 00:00:00",
  "end_date": "YYYY-MM-DD 00:00:00",
  "from": "YYYY-MM-DD 00:00:00",
  "to": "YYYY-MM-DD 00:00:00",
  "offset": 0,
  "limit": 250
}
```

Broad/list/event/id requests can return stale futures fixture rows, so production should use one fixture-scoped request per selected MLB fixture.
