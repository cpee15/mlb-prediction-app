export const DEFAULT_BUILDER_FIELDS = ['entity_name', 'score', 'confidence']

const STATIC_FIELDS = [
  { accessor: 'entity_name', label: 'Entity Name', group: 'identity' },
  { accessor: 'entity_id', label: 'Entity ID', group: 'identity' },
  { accessor: 'entity_type', label: 'Entity Type', group: 'identity' },
  { accessor: 'score', label: 'Score', group: 'scoring' },
  { accessor: 'confidence', label: 'Confidence', group: 'scoring' },
  { accessor: 'primary_reason', label: 'Primary Reason', group: 'analysis' },
  { accessor: 'source', label: 'Source', group: 'analysis' },
  { accessor: 'team', label: 'Team', group: 'team' },
  { accessor: 'opponent', label: 'Opponent', group: 'team' },
  { accessor: 'category', label: 'Category', group: 'analysis' },
]

const GROUP_TITLES = {
  identity: 'Identity',
  team: 'Team & Matchup',
  scoring: 'Score & Confidence',
  metrics: 'Metrics',
  analysis: 'Analysis',
  filters: 'Filterable Fields',
  metadata: 'Metadata',
  other: 'Other',
}

function titleCase(value) {
  return String(value || '')
    .replace(/[_-]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .replace(/\b\w/g, ch => ch.toUpperCase())
}

function labelForAccessor(accessor) {
  if (!accessor) return 'Field'
  const trimmed = String(accessor)
  if (trimmed.startsWith('metrics.')) return titleCase(trimmed.slice('metrics.'.length))
  return titleCase(trimmed.replace(/\./g, ' '))
}

function inferGroup(accessor) {
  if (!accessor) return 'other'
  if (accessor.startsWith('metrics.')) return 'metrics'
  if (accessor.includes('entity_') || accessor.includes('player') || accessor.includes('pitcher') || accessor.includes('batter') || accessor.endsWith('_id')) return 'identity'
  if (accessor.includes('team') || accessor.includes('opponent') || accessor.includes('game')) return 'team'
  if (accessor.includes('score') || accessor.includes('confidence')) return 'scoring'
  if (accessor.includes('reason') || accessor.includes('warning') || accessor.includes('source') || accessor.includes('lineup')) return 'analysis'
  if (accessor.includes('filter') || accessor.includes('category') || accessor.includes('pitch_type')) return 'filters'
  if (accessor.includes('date') || accessor.includes('created') || accessor.includes('updated') || accessor.includes('saved')) return 'metadata'
  return 'other'
}

function isScalar(value) {
  return value == null || ['string', 'number', 'boolean'].includes(typeof value)
}

function flattenObject(value, prefix = '', out = {}) {
  if (value == null) {
    if (prefix) out[prefix] = value
    return out
  }

  if (Array.isArray(value)) {
    if (prefix && value.every(isScalar)) {
      out[prefix] = value.join(', ')
      return out
    }
    value.slice(0, 3).forEach((entry, index) => {
      if (isScalar(entry)) {
        if (prefix) out[`${prefix}[${index}]`] = entry
      } else {
        flattenObject(entry, `${prefix}[${index}]`, out)
      }
    })
    return out
  }

  if (typeof value === 'object') {
    Object.entries(value).forEach(([key, entry]) => {
      const nextPrefix = prefix ? `${prefix}.${key}` : key
      if (isScalar(entry)) out[nextPrefix] = entry
      else flattenObject(entry, nextPrefix, out)
    })
    return out
  }

  if (prefix) out[prefix] = value
  return out
}

function registerField(map, accessor, meta = {}) {
  if (!accessor) return
  const existing = map.get(accessor)
  const next = existing || {
    accessor,
    label: meta.label || labelForAccessor(accessor),
    group: meta.group || inferGroup(accessor),
    components: new Set(),
  }
  if (meta.component) next.components.add(meta.component)
  if (meta.group) next.group = meta.group
  if (meta.label) next.label = meta.label
  map.set(accessor, next)
}

function scanResultItems(map, component, items) {
  ;(items || []).forEach(item => {
    const flattened = flattenObject(item)
    Object.keys(flattened).forEach(accessor => registerField(map, accessor, { component }))
  })
}

function scanWorkspaceItems(map, workspace) {
  ;(workspace?.folders || []).forEach(folder => {
    ;(folder.items || []).forEach(item => {
      const payload = item?.payload_json || {}
      const boardState = payload?.board_state || {}
      scanResultItems(map, payload.saved_from_component || boardState.component || 'saved', boardState.items || [])
      registerField(map, 'saved_item.title', { label: 'Saved Item Title', group: 'metadata' })
      registerField(map, 'saved_item.subtitle', { label: 'Saved Item Subtitle', group: 'metadata' })
      registerField(map, 'saved_item.notes', { label: 'Saved Item Notes', group: 'metadata' })
      registerField(map, 'saved_item.source_type', { label: 'Saved Item Source Type', group: 'metadata' })
    })
  })
}

export function collectBuilderFieldGroups({ results = {}, workspace = null }) {
  const fieldMap = new Map()

  STATIC_FIELDS.forEach(field => registerField(fieldMap, field.accessor, field))

  Object.entries(results || {}).forEach(([component, result]) => {
    scanResultItems(fieldMap, component, result?.items || [])
    registerField(fieldMap, 'result.result_count_before_filters', { label: 'Result Count Before Filters', group: 'metadata', component })
    registerField(fieldMap, 'result.result_count_after_filters', { label: 'Result Count After Filters', group: 'metadata', component })
    ;(result?.available_filters?.suggested_metric_filters || []).forEach(metric => {
      registerField(fieldMap, `metrics.${metric}`, { label: metric, group: 'metrics', component })
    })
    ;(result?.available_filters?.suggested_weight_metrics || []).forEach(metric => {
      registerField(fieldMap, `metrics.${metric}`, { label: metric, group: 'metrics', component })
    })
    ;['search_text', 'team', 'opponent', 'min_score', 'max_score', 'min_confidence', 'category', 'player_type', 'pitch_type', 'source'].forEach(filterKey => {
      registerField(fieldMap, `filters.${filterKey}`, { label: titleCase(filterKey), group: 'filters', component })
    })
  })

  scanWorkspaceItems(fieldMap, workspace)

  const grouped = {}
  Array.from(fieldMap.values()).forEach(field => {
    const key = field.group || 'other'
    if (!grouped[key]) grouped[key] = []
    grouped[key].push({ accessor: field.accessor, label: field.label, components: Array.from(field.components || []).sort() })
  })

  return Object.entries(grouped)
    .sort((a, b) => (GROUP_TITLES[a[0]] || a[0]).localeCompare(GROUP_TITLES[b[0]] || b[0]))
    .map(([groupKey, fields]) => ({ groupKey, title: GROUP_TITLES[groupKey] || titleCase(groupKey), fields: fields.sort((a, b) => a.label.localeCompare(b.label)) }))
}

export function getValueByPath(source, accessor) {
  if (!source || !accessor) return null
  if (accessor.startsWith('saved_item.')) {
    const key = accessor.slice('saved_item.'.length)
    return source?.[key] ?? null
  }
  if (accessor.startsWith('result.')) return null
  if (accessor.startsWith('filters.')) return null
  return accessor.split('.').reduce((acc, key) => {
    if (acc == null) return null
    return acc[key]
  }, source)
}
