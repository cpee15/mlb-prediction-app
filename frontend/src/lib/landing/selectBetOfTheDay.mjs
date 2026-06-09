export function americanOddsToImpliedProbability(odds) {
  const n = Number(odds)
  if (!Number.isFinite(n) || n === 0) return null
  return n < 0 ? Math.abs(n) / (Math.abs(n) + 100) : 100 / (n + 100)
}

export function americanOddsToDecimal(odds) {
  const n = Number(odds)
  if (!Number.isFinite(n) || n === 0) return null
  return n > 0 ? 1 + n / 100 : 1 + 100 / Math.abs(n)
}

export function probabilityToFairAmericanOdds(probability) {
  const p = normalizeProbability(probability)
  if (p == null || p <= 0 || p >= 1) return null
  return p >= 0.5 ? -1 * ((p / (1 - p)) * 100) : ((1 - p) / p) * 100
}

export function calculateEdge(modelProbability, impliedProbability) {
  const model = normalizeProbability(modelProbability)
  const implied = normalizeProbability(impliedProbability)
  if (model == null || implied == null) return null
  return model - implied
}

export function calculateEvPer100(modelProbability, americanOdds) {
  const model = normalizeProbability(modelProbability)
  const decimal = americanOddsToDecimal(americanOdds)
  if (model == null || decimal == null) return null
  const evPerDollar = model * (decimal - 1) - (1 - model)
  return evPerDollar * 100
}

export function selectBetOfTheDay({ matchups = [], events = [], modelPayload = null, boardPayload = null } = {}) {
  const matchupByKey = new Map()
  asArray(matchups).forEach(matchup => {
    const key = keyFromMatchup(matchup)
    if (key !== '@') matchupByKey.set(key, matchup)
  })

  const modelByKey = new Map()
  modelGamesFromPayload(modelPayload).forEach(game => {
    const key = matchupKey(
      firstValue(game, ['away_team', 'away_team_name', 'away', 'teams.away.name']),
      firstValue(game, ['home_team', 'home_team_name', 'home', 'teams.home.name']),
    )
    if (key !== '@') modelByKey.set(key, game)
  })

  const boardItems = boardItemsFromPayload(boardPayload)
  const candidates = []

  asArray(events).forEach(event => {
    const key = keyFromEvent(event)
    const matchup = matchupByKey.get(key)
    const modelGame = modelByKey.get(key)
    const away = teamName(event?.away_team) || matchup?.away_team_name || matchup?.away_team || modelGame?.away_team || 'Away'
    const home = teamName(event?.home_team) || matchup?.home_team_name || matchup?.home_team || modelGame?.home_team || 'Home'
    const modelSources = [matchup, modelGame, ...(boardItems.filter(item => sameTeamText(item, away) || sameTeamText(item, home)))]

    gameMarkets(event).forEach(market => {
      asArray(market.selections).forEach(selection => {
        const americanOdds = number(selection.price ?? selection.american_odds ?? selection.odds?.american)
        if (americanOdds == null) return
        const pick = selection.description || selection.name || selectionLabel(selection)
        const side = inferSide(selection, away, home)
        const modelProbability = probabilityForSelection({ side, market, selection, matchup, modelGame, sources: modelSources })
        if (modelProbability == null) return
        const impliedProbability = americanOddsToImpliedProbability(americanOdds)
        const fairOdds = probabilityToFairAmericanOdds(modelProbability)
        const edgePct = calculateEdge(modelProbability, impliedProbability)
        const evPer100 = calculateEvPer100(modelProbability, americanOdds)
        if (edgePct == null || evPer100 == null || edgePct <= 0 || evPer100 <= 0) return

        candidates.push({
          id: `${event.event_id || matchup?.game_pk || key}-${marketKey(market)}-${pick}-${americanOdds}`,
          gameId: matchup?.game_pk || event.event_id,
          matchup: `${away} @ ${home}`,
          market: cleanMarketName(market.market_name || marketKey(market)),
          pick,
          americanOdds,
          modelProbability,
          impliedProbability,
          fairOdds,
          edgePct,
          evPer100,
          reason: reasonForCandidate({ market, side, away, home, matchup, modelGame }),
          riskNote: 'If confirmed lineups, starters, or market prices change materially, the edge should be recalculated.',
          href: matchup?.game_pk ? `/matchup/${matchup.game_pk}` : '/',
          source: {
            modelProbabilityField: sourceFieldForSelection(side, market),
            oddsField: 'selection.price',
          },
        })
      })
    })
  })

  candidates.sort((a, b) => b.evPer100 - a.evPer100 || b.edgePct - a.edgePct)
  return candidates[0] || null
}

function normalizeProbability(value) {
  const n = number(value)
  if (n == null) return null
  if (n > 1 && n <= 100) return n / 100
  if (n <= 0 || n >= 1) return null
  return n
}

function number(value) {
  if (value === null || value === undefined || value === '') return null
  const n = Number(value)
  return Number.isFinite(n) ? n : null
}

function asArray(value) {
  return Array.isArray(value) ? value : []
}

function cleanMarketName(value) {
  return String(value || 'Market').replaceAll('_', ' ').replace(/\b\w/g, c => c.toUpperCase())
}

function normalizeTeamName(name) {
  return String(name || '').toLowerCase().replace(/[^a-z0-9]/g, '').replace(/^the/, '')
}

function matchupKey(away, home) {
  return `${normalizeTeamName(away)}@${normalizeTeamName(home)}`
}

function teamName(value) {
  if (!value) return ''
  return typeof value === 'string' ? value : value.name || value.fullName || value.teamName || ''
}

function keyFromMatchup(matchup) {
  return matchupKey(matchup?.away_team_name || matchup?.away_team || matchup?.away_name, matchup?.home_team_name || matchup?.home_team || matchup?.home_name)
}

function keyFromEvent(event) {
  return matchupKey(teamName(event?.away_team) || event?.away_team, teamName(event?.home_team) || event?.home_team)
}

function marketKey(market) {
  return String(market?.market_key || market?.market_type || market?.market_name || '').toLowerCase()
}

function gameMarkets(event) {
  return asArray(event?.markets).filter(market => {
    const key = marketKey(market)
    const name = String(market?.market_name || '').toLowerCase()
    return ['h2h', 'spreads', 'totals'].includes(key) || name.includes('moneyline') || name.includes('run line') || name.includes('total') || name.includes('first 5') || name.includes('f5')
  })
}

function selectionLabel(selection) {
  return `${selection?.description || selection?.name || 'Selection'}${selection?.line != null ? ` ${selection.line}` : ''}`
}

function inferSide(selection, away, home) {
  const text = `${selection?.description || ''} ${selection?.name || ''}`.toLowerCase()
  if (text.includes(normalizeTeamName(away)) || normalizeTeamName(text).includes(normalizeTeamName(away))) return 'away'
  if (text.includes(normalizeTeamName(home)) || normalizeTeamName(text).includes(normalizeTeamName(home))) return 'home'
  if (text.includes('over')) return 'over'
  if (text.includes('under')) return 'under'
  return null
}

function probabilityForSelection({ side, market, selection, matchup, modelGame, sources }) {
  const key = marketKey(market)
  if (side === 'away') return firstProbability(sources, ['away_win_prob', 'away_win_probability', 'away_model_probability', 'away_projected_probability'])
  if (side === 'home') return firstProbability(sources, ['home_win_prob', 'home_win_probability', 'home_model_probability', 'home_projected_probability'])
  const selectionProb = firstProbability([selection, market, matchup, modelGame], ['model_probability', 'projected_probability', 'projectedProbability', 'fair_probability', 'modelProbability'])
  if (selectionProb != null) return selectionProb
  if (key === 'totals') return null
  return firstProbability(sources, ['model_probability', 'projected_probability', 'win_probability', 'projectedWinProb', 'modelWinProb'])
}

function firstProbability(sources, keys) {
  for (const source of sources.filter(Boolean)) {
    for (const key of keys) {
      const value = firstValue(source, [key])
      const p = normalizeProbability(value)
      if (p != null) return p
    }
  }
  return null
}

function firstValue(source, paths) {
  for (const path of paths) {
    const value = getPath(source, path)
    if (value !== null && value !== undefined && value !== '') return value
  }
  return null
}

function getPath(obj, path) {
  if (!obj || !path) return null
  const parts = String(path).split('.')
  let cur = obj
  for (const part of parts) {
    if (!cur || typeof cur !== 'object') return null
    cur = cur[part]
  }
  return cur
}

function modelGamesFromPayload(payload) {
  if (Array.isArray(payload?.games)) return payload.games
  if (Array.isArray(payload?.models)) return payload.models
  return []
}

function boardItemsFromPayload(payload) {
  const results = payload?.results || {}
  return Object.values(results).flatMap(result => asArray(result?.items))
}

function sameTeamText(item, team) {
  const t = normalizeTeamName(team)
  const haystack = normalizeTeamName(`${item?.team || ''} ${item?.opponent || ''} ${item?.entity_name || ''} ${item?.pick || ''}`)
  return Boolean(t && haystack.includes(t))
}

function sourceFieldForSelection(side, market) {
  if (side === 'away') return 'away_win_prob / away_win_probability'
  if (side === 'home') return 'home_win_prob / home_win_probability'
  return `${marketKey(market)} model probability field`
}

function reasonForCandidate({ market, side, away, home }) {
  const marketName = cleanMarketName(market.market_name || marketKey(market))
  if (side === 'away') return `MLBGPT model probability on ${away} is higher than the sportsbook implied probability for this ${marketName} price.`
  if (side === 'home') return `MLBGPT model probability on ${home} is higher than the sportsbook implied probability for this ${marketName} price.`
  return `MLBGPT found a positive value gap between model probability and sportsbook implied probability for this ${marketName}.`
}
