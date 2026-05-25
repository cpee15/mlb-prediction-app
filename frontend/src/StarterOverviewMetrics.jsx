const wanted = new Set(['era','whip','k_pct','bb_pct','k_minus_bb_pct','hr_per_9','innings_pitched','strikeouts','walks','xwoba_allowed','xba_allowed','hard_hit_rate_allowed','barrel_rate_allowed','avg_velocity','avg_spin_rate','pitch_count_sample_size','batters_faced','pitches_thrown'])

const styles = {
  grid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(74px, 1fr))', gap: 7, marginTop: 12 },
  box: { border: '1px solid var(--border-subtle)', borderRadius: 'var(--radius-sm)', background: 'rgba(17, 24, 39, 0.62)', padding: '7px 8px' },
  label: { color: 'var(--text-muted)', fontSize: 9, textTransform: 'uppercase', letterSpacing: '0.06em', marginBottom: 4, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' },
  value: { color: 'var(--text-primary)', fontSize: 13, fontWeight: 850, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis' },
}

function fmt(metric) {
  if (!metric || metric.value == null) return null
  if (metric.formatted_value) return metric.formatted_value
  const key = metric.key || ''
  const n = Number(metric.value)
  if (Number.isNaN(n)) return String(metric.value)
  if (key.endsWith('_pct') || key === 'hr_fb' || key === 'babip' || key === 'lob_pct') return (n * 100).toFixed(1) + '%'
  if (key === 'era' || key === 'whip' || key === 'hr_per_9') return n.toFixed(2)
  if (key === 'xwoba_allowed' || key === 'xba_allowed') return n.toFixed(3)
  if (key === 'avg_velocity') return n.toFixed(1)
  if (key === 'avg_spin_rate' || key === 'strikeouts' || key === 'walks' || key === 'batters_faced' || key === 'pitches_thrown' || key === 'pitch_count_sample_size') return Math.round(n).toLocaleString()
  return Number.isInteger(n) ? n.toLocaleString() : n.toFixed(2)
}

export default function StarterOverviewMetrics({ overview, align = 'left' }) {
  const rows = (overview?.display_metrics || [])
    .filter(m => m && m.available && m.value != null && wanted.has(m.key))
    .map(m => ({ ...m, displayValue: fmt(m) }))
    .filter(m => m.displayValue)
    .slice(0, 12)
  if (!rows.length) return null
  return (
    <div style={styles.grid}>
      {rows.map(m => (
        <div key={m.key} style={styles.box} title={m.source || ''}>
          <div style={{ ...styles.label, textAlign: align }}>{m.label}</div>
          <div style={{ ...styles.value, textAlign: align }}>{m.displayValue}</div>
        </div>
      ))}
    </div>
  )
}
