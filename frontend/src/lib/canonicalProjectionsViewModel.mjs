function asObject(value) {
  return value && typeof value === 'object'
    ? value
    : {}
}

function asArray(value) {
  return Array.isArray(value)
    ? value
    : []
}

function number(value) {
  const parsed = Number(value)

  return Number.isFinite(parsed)
    ? parsed
    : null
}

function metricSummary(row, name) {
  const metrics = asObject(row?.metrics)
  const metric = asObject(metrics[name])

  return metric
}

function metricValue(row, name, key = 'mean') {
  return number(
    metricSummary(row, name)?.[key],
  )
}

function sumValues(values) {
  const available = values.filter(
    value => value !== null,
  )

  if (!available.length) return null

  return available.reduce(
    (total, value) => total + value,
    0,
  )
}

function playerName(row) {
  return (
    row?.full_name ||
    row?.player_name ||
    row?.player_id ||
    'Unknown player'
  )
}

function teamSide(row) {
  const value = String(
    row?.team_side ||
    row?.side ||
    '',
  ).trim()

  return value || '—'
}

function dfsValue(row, key) {
  const direct = number(row?.[key])

  if (direct !== null) return direct

  const mapping = {
    projected_dfs_points: 'mean',
    dfs_floor: 'p10',
    dfs_median: 'median',
    dfs_ceiling: 'p90',
  }

  return metricValue(
    row,
    'dfs_points',
    mapping[key],
  )
}

function batterRow(row) {
  const singles = metricValue(row, 'singles')
  const doubles = metricValue(row, 'doubles')
  const triples = metricValue(row, 'triples')
  const homeRuns = metricValue(
    row,
    'home_runs',
  )

  return {
    playerId: row?.player_id ?? null,
    mlbPlayerId: row?.mlb_player_id ?? null,
    name: playerName(row),
    side: teamSide(row),
    plateAppearances: metricValue(
      row,
      'plate_appearances',
    ),
    hits: sumValues([
      singles,
      doubles,
      triples,
      homeRuns,
    ]),
    runs: metricValue(row, 'runs'),
    rbis: metricValue(row, 'rbis'),
    singles,
    doubles,
    triples,
    homeRuns,
    walks: metricValue(row, 'walks'),
    strikeouts: metricValue(
      row,
      'strikeouts',
    ),
    dfsMean: dfsValue(
      row,
      'projected_dfs_points',
    ),
    dfsFloor: dfsValue(row, 'dfs_floor'),
    dfsMedian: dfsValue(row, 'dfs_median'),
    dfsCeiling: dfsValue(
      row,
      'dfs_ceiling',
    ),
  }
}

function pitcherRow(row) {
  const outs = metricValue(row, 'outs')

  return {
    playerId: row?.player_id ?? null,
    mlbPlayerId: row?.mlb_player_id ?? null,
    name: playerName(row),
    side: teamSide(row),
    battersFaced: metricValue(
      row,
      'batters_faced',
    ),
    inningsPitched: (
      outs === null
        ? null
        : outs / 3
    ),
    hitsAllowed: (
      metricValue(row, 'hits_allowed') ??
      metricValue(row, 'hits')
    ),
    walks: metricValue(row, 'walks'),
    hitByPitch: (
      metricValue(row, 'hit_by_pitch') ??
      metricValue(row, 'hit_batters')
    ),
    strikeouts: metricValue(
      row,
      'strikeouts',
    ),
    runs: (
      metricValue(row, 'runs_allowed') ??
      metricValue(row, 'runs')
    ),
    earnedRuns: (
      metricValue(row, 'earned_runs') ??
      metricValue(row, 'earned_runs_allowed')
    ),
    dfsMean: dfsValue(
      row,
      'projected_dfs_points',
    ),
    dfsFloor: dfsValue(row, 'dfs_floor'),
    dfsMedian: dfsValue(row, 'dfs_median'),
    dfsCeiling: dfsValue(
      row,
      'dfs_ceiling',
    ),
  }
}

function sortRows(rows) {
  return [...rows].sort((left, right) => {
    const sideComparison = String(
      left.side,
    ).localeCompare(String(right.side))

    if (sideComparison) return sideComparison

    return String(left.name).localeCompare(
      String(right.name),
    )
  })
}

export function buildCanonicalProjectionsViewModel(
  game,
) {
  const shadow = asObject(
    game?.diagnostics?.canonical_shadow,
  )
  const projections = asObject(
    shadow.player_projections,
  )
  const players = asArray(projections.players)

  const available = (
    projections.schema_version ===
      'canonical_player_projection_rows_v1' &&
    players.length > 0
  )

  return {
    available,
    status: projections.status || (
      available ? 'available' : 'unavailable'
    ),
    schemaVersion: (
      projections.schema_version || null
    ),
    sourceProjectionSchemaVersion: (
      projections
        .source_projection_schema_version ||
      null
    ),
    runId: projections.run_id || null,
    modelVersion: (
      projections.model_version || null
    ),
    simulationCount: number(
      projections.simulation_count,
    ),
    authoritative: (
      projections.authoritative === true
    ),
    authoritativeSource: (
      projections.authoritative_source ||
      shadow.authoritative_source ||
      'legacy'
    ),
    identityEnrichmentApplied: (
      projections
        .identity_enrichment_applied === true
    ),
    batters: sortRows(
      players
        .filter(
          row => row?.player_type === 'batter',
        )
        .map(batterRow),
    ),
    pitchers: sortRows(
      players
        .filter(
          row => row?.player_type === 'pitcher',
        )
        .map(pitcherRow),
    ),
    errorMessage: (
      projections.error_message || null
    ),
    raw: projections,
  }
}
