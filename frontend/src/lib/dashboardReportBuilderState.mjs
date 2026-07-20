import { queryPayload } from './dashboardQueryState.mjs'

export const CANONICAL_REPORT_TYPES = {
  hitters: 'all_active_hitters',
  pitchers: 'all_active_pitchers',
}

const LEGACY_DEFAULT_FIELDS = ['rank', 'entity_name', 'team', 'opponent', 'score', 'confidence']

function usesLegacyLineupDataset(objectKey, activeLineupsOnly) {
  return Boolean(activeLineupsOnly && ['hitters', 'overall_players'].includes(objectKey))
}

export const DEFAULT_FIELDS_BY_OBJECT = {
  hitters: ['rank', 'full_name', 'team_name', 'model_score', 'confidence'],
  pitchers: ['rank', 'full_name', 'team_name', 'model_score', 'confidence'],
  teams: LEGACY_DEFAULT_FIELDS,
  totals: ['rank', 'entity_name', 'score', 'confidence'],
  overall_players: LEGACY_DEFAULT_FIELDS,
}

export function defaultFieldsForObject(objectKey) {
  return [...(DEFAULT_FIELDS_BY_OBJECT[objectKey] || LEGACY_DEFAULT_FIELDS)]
}

export function initialFieldsByObject(objects, persisted = {}) {
  const defaults = Object.fromEntries(objects.map(object => [object.key, defaultFieldsForObject(object.key)]))
  if (persisted.selectedFieldsByObject && typeof persisted.selectedFieldsByObject === 'object') {
    Object.entries(persisted.selectedFieldsByObject).forEach(([key, fields]) => {
      if (Array.isArray(fields) && fields.length) defaults[key] = fields
    })
  }
  return defaults
}

export function reportFieldsForMode({ objectKey, activeLineupsOnly, selectedFields }) {
  if (usesLegacyLineupDataset(objectKey, activeLineupsOnly)) return [...LEGACY_DEFAULT_FIELDS]
  return Array.isArray(selectedFields) && selectedFields.length
    ? [...selectedFields]
    : defaultFieldsForObject(objectKey)
}

export function canonicalSortField(field) {
  return ({
    entity_name: 'full_name',
    team: 'team_name',
    score: 'adjusted_score',
    base_score: 'model_score',
  })[field] || field
}

export function buildReportRequest({ objectKey, activeLineupsOnly, date, cleanedFilters, query }) {
  const useLineups = usesLegacyLineupDataset(objectKey, activeLineupsOnly)
  const reportType = useLineups ? null : CANONICAL_REPORT_TYPES[objectKey]
  const { weights = {}, ...criteria } = cleanedFilters || {}
  if (reportType) {
    return {
      path: '/my-dashboard/reports/query',
      reportType,
      payload: {
        report_type: reportType,
        as_of_date: date,
        filters: criteria,
        weights,
        page_size: query.page_size,
        page_number: query.page_number,
        sort_by: canonicalSortField(query.sort_by),
        sort_direction: query.sort_direction,
        include_metadata: true,
      },
    }
  }
  return {
    path: useLineups ? '/my-dashboard/solver/active-lineups' : '/my-dashboard/solver',
    reportType: null,
    payload: queryPayload({ date, component: objectKey, filters: cleanedFilters, query }),
  }
}


export function canonicalBootstrapMessage(result) {
  const bootstrap = result?.population_bootstrap
  if (!bootstrap || !bootstrap.status) {
    return { tone: 'empty', title: 'No qualifying rows', detail: 'No records matched this server query.' }
  }
  const run = bootstrap.run_id ? ` Run #${bootstrap.run_id}.` : ''
  if (bootstrap.status === 'in_progress') {
    return { tone: 'loading', title: 'Canonical population is refreshing', detail: `The verified roster refresh is still running.${run} Retry shortly.` }
  }
  if (bootstrap.status === 'failed') {
    const errorType = bootstrap.error_type || 'UnknownError'
    return { tone: 'error', title: 'Canonical population failed', detail: `The guarded refresh failed with ${errorType}.${run}` }
  }
  if (bootstrap.status === 'disabled') {
    return { tone: 'error', title: 'Canonical population is disabled', detail: 'Automatic canonical population is disabled by server configuration.' }
  }
  if (bootstrap.status === 'empty') {
    return { tone: 'error', title: 'Canonical population remained empty', detail: `The guarded refresh completed without reportable current rows.${run}` }
  }
  return { tone: 'empty', title: 'No qualifying rows', detail: 'No records matched this server query.' }
}

export function normalizeCanonicalPage(json, query) {
  const total = Number(json?.totalSize || 0)
  const records = Array.isArray(json?.records) ? json.records : []
  const pageCount = total ? Math.ceil(total / query.page_size) : 0
  const hasNext = Boolean(json?.page_info?.has_next_page)
  return {
    ...json,
    execution_path: 'dashboard_player_current_sql_query',
    page_info: {
      ...json?.page_info,
      page_number: query.page_number,
      page_size: query.page_size,
      page_count: pageCount,
      record_count: records.length,
      has_next: hasNext,
      has_previous: query.page_number > 1 && total > 0,
      next_page: hasNext ? query.page_number + 1 : null,
      previous_page: query.page_number > 1 ? query.page_number - 1 : null,
    },
  }
}
