import React, { useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { API_BASE, getMlbLiveDate } from '../lib/api'

const BUCKETS = ['hourly', 'daily', 'weekly', 'monthly', 'beat', 'betting']
const API = API_BASE

const s = {
  page: { display: 'grid', gap: 16 },
  hero: { background: 'linear-gradient(135deg,#101820 0%,#0d1117 55%,#111827 100%)', border: '1px solid #30363d', borderRadius: 16, padding: 20, boxShadow: '0 18px 48px rgba(0,0,0,.24)' },
  row: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 12, flexWrap: 'wrap' },
  eyebrow: { color: '#58a6ff', fontSize: 12, fontWeight: 900, textTransform: 'uppercase', letterSpacing: 1.2 },
  title: { color: '#e6edf3', fontSize: 32, lineHeight: 1.05, fontWeight: 950, margin: '6px 0 0' },
  subtitle: { color: '#8b949e', fontSize: 14, maxWidth: 920, marginTop: 8, lineHeight: 1.45 },
  input: { background: '#0d1117', border: '1px solid #30363d', color: '#e6edf3', borderRadius: 10, padding: '10px 12px', fontSize: 13, outline: 'none' },
  button: { background: '#238636', border: '1px solid #2ea043', color: '#fff', borderRadius: 10, padding: '10px 14px', fontSize: 13, fontWeight: 900, cursor: 'pointer' },
  muted: { background: '#21262d', border: '1px solid #30363d', color: '#58a6ff', borderRadius: 10, padding: '9px 12px', fontSize: 12, fontWeight: 900, cursor: 'pointer', textDecoration: 'none' },
  ticker: { border: '1px solid #30363d', borderRadius: 14, background: '#05080d', overflow: 'hidden' },
  tickerHead: { display: 'flex', gap: 10, alignItems: 'center', padding: '10px 12px', borderBottom: '1px solid #30363d', color: '#e6edf3', fontWeight: 900, fontSize: 12, textTransform: 'uppercase', letterSpacing: .9 },
  tickerRow: { display: 'flex', gap: 18, overflowX: 'auto', padding: '11px 12px', whiteSpace: 'nowrap' },
  tickerItem: { color: '#c9d1d9', fontSize: 13, display: 'inline-flex', gap: 8, alignItems: 'center', textDecoration: 'none' },
  badge: { border: '1px solid #30363d', background: '#0d1117', color: '#8b949e', borderRadius: 999, padding: '4px 8px', fontSize: 11, fontWeight: 900 },
  hot: { border: '1px solid rgba(248,81,73,.45)', background: 'rgba(248,81,73,.14)', color: '#f85149', borderRadius: 999, padding: '4px 8px', fontSize: 11, fontWeight: 900 },
  stats: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(145px,1fr))', gap: 10, marginTop: 16 },
  stat: { background: 'rgba(13,17,23,.72)', border: '1px solid #30363d', borderRadius: 12, padding: '12px 13px' },
  statLabel: { color: '#8b949e', fontSize: 10, textTransform: 'uppercase', letterSpacing: .8, fontWeight: 900 },
  statValue: { color: '#e6edf3', fontSize: 23, fontWeight: 950, marginTop: 4 },
  section: { background: '#161b22', border: '1px solid #30363d', borderRadius: 14, padding: 15 },
  sectionTitle: { color: '#e6edf3', fontSize: 18, fontWeight: 950 },
  tabs: { display: 'flex', gap: 8, flexWrap: 'wrap' },
  tab: active => ({ background: active ? '#238636' : '#21262d', border: active ? '1px solid #2ea043' : '1px solid #30363d', color: active ? '#fff' : '#c9d1d9', borderRadius: 999, padding: '8px 11px', fontSize: 12, fontWeight: 900, cursor: 'pointer', textTransform: 'capitalize' }),
  filters: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(160px,1fr))', gap: 10 },
  label: { color: '#8b949e', fontSize: 10, textTransform: 'uppercase', letterSpacing: .8, fontWeight: 900, marginBottom: 6 },
  select: { width: '100%', background: '#0d1117', border: '1px solid #30363d', color: '#e6edf3', borderRadius: 10, padding: '10px 11px', fontSize: 13, outline: 'none' },
  grid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit,minmax(280px,1fr))', gap: 12 },
  card: { border: '1px solid #30363d', borderRadius: 13, background: '#0d1117', padding: 13, minHeight: 142 },
  cardTitle: { color: '#e6edf3', fontSize: 15, fontWeight: 950, lineHeight: 1.28, textDecoration: 'none' },
  meta: { color: '#8b949e', fontSize: 12, marginTop: 7, lineHeight: 1.4 },
  tagRow: { display: 'flex', gap: 6, flexWrap: 'wrap', marginTop: 10 },
  tag: { color: '#58a6ff', border: '1px solid rgba(88,166,255,.35)', background: 'rgba(88,166,255,.08)', borderRadius: 999, padding: '3px 7px', fontSize: 10, fontWeight: 900 },
  empty: { color: '#8b949e', textAlign: 'center', padding: 28, border: '1px solid #30363d', borderRadius: 14, background: '#0d1117' },
  error: { color: '#f85149', background: '#1f1116', border: '1px solid #3b2222', borderRadius: 12, padding: 13 },
}

function asArray(value) { return Array.isArray(value) ? value : [] }
function fmtTime(iso) { if (!iso) return 'Pending'; try { return new Date(iso).toLocaleString('en-US', { month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit' }) } catch { return 'Pending' } }
function score(v) { const n = Number(v); return Number.isFinite(n) ? n.toFixed(0) : '0' }
function bucketCount(payload) { return BUCKETS.reduce((sum, bucket) => sum + asArray(payload?.buckets?.[bucket]).length, 0) }
function providerLabel(provider) { return ['rss', 'rss_network', 'rss_feed_network'].includes(provider) ? 'RSS Feed Network' : (provider || 'loading') }
function statusLabel(status) {
  if (status === 'ok') return 'Connected'
  if (status === 'empty') return 'Connected, no items returned'
  if (status === 'provider_not_configured') return 'Provider Missing'
  if (status === 'provider_missing_dependency') return 'Dependency Missing'
  return status || 'loading'
}
function emptyMessage(payload) {
  if (['rss', 'rss_network', 'rss_feed_network'].includes(payload?.provider) && payload?.status === 'empty') return 'RSS provider is connected, but no matching MLB news items were returned for this date/filter.'
  if (payload?.status === 'provider_not_configured') return 'Provider missing. Enable RSS on the backend or configure a paid provider.'
  return `No items in this bucket yet. Status: ${statusLabel(payload?.status)}.`
}
function compactNumber(v) { const n = Number(v || 0); return Number.isFinite(n) ? n.toLocaleString() : '0' }
function engagementText(item) {
  const e = item?.engagement || {}
  const parts = []
  if (e.likes) parts.push(`${compactNumber(e.likes)} likes`)
  if (e.reposts) parts.push(`${compactNumber(e.reposts)} reposts`)
  if (e.replies) parts.push(`${compactNumber(e.replies)} replies`)
  if (e.views) parts.push(`${compactNumber(e.views)} views`)
  if (!parts.length && e.engagement_total) parts.push(`${compactNumber(e.engagement_total)} interactions`)
  return parts.join(' · ')
}

function Ticker({ rows, status, provider }) {
  const emptyText = ['rss', 'rss_network', 'rss_feed_network'].includes(provider)
    ? 'RSS provider is connected, but no matching MLB news items were returned for this date/filter.'
    : 'No provider items yet. Enable RSS or configure a paid provider to light up the terminal.'
  return <section style={s.ticker}>
    <div style={s.tickerHead}><span style={s.hot}>Live Wire</span><span>MLB News Terminal</span><span style={s.badge}>{statusLabel(status)}</span></div>
    <div style={s.tickerRow}>{rows.length === 0 ? <span style={s.tickerItem}>{emptyText}</span> : rows.map(row => <a key={row.id} href={row.url || '#'} target="_blank" rel="noreferrer" style={s.tickerItem}><span style={s.hot}>{row.tag || 'news'}</span><strong>{row.headline}</strong><span>{row.source}</span><span>{row.time_ago}</span></a>)}</div>
  </section>
}

function NewsCard({ item }) {
  const body = <div style={s.cardTitle}>{item.title || 'Untitled news item'}</div>
  return <article style={s.card}>
    {item.url ? <a href={item.url} target="_blank" rel="noreferrer" style={{ textDecoration: 'none' }}>{body}</a> : body}
    <div style={s.meta}>{item.source || 'Unknown source'} · {item.source_type || 'source'} · {fmtTime(item.published_at)} · Score {score(item.importance_score)}</div>
    {item.summary && <div style={s.meta}>{String(item.summary).slice(0, 220)}</div>}
    <div style={s.tagRow}>{asArray(item.tags).slice(0, 6).map(tag => <span key={tag} style={s.tag}>{tag}</span>)}</div>
  </article>
}

function TweetCard({ item }) {
  const handle = item.handle || item.author || 'X/Twitter'
  const eText = engagementText(item)
  return <article style={{ ...s.card, minHeight: 0 }}>
    <div style={s.row}><strong style={{ color: '#e6edf3' }}>{handle}</strong><span style={s.badge}>Quality {score(item.twitter_quality_score)}</span></div>
    <div style={s.meta}>{item.author && item.handle ? item.author : 'Twitter/X post'} · {fmtTime(item.published_at)}{eText ? ` · ${eText}` : ''}</div>
    <div style={{ color: '#c9d1d9', fontSize: 13, lineHeight: 1.45, marginTop: 9 }}>{item.summary || item.title || 'No post text returned.'}</div>
    <div style={s.tagRow}>{asArray(item.tags).slice(0, 6).map(tag => <span key={tag} style={s.tag}>{tag}</span>)}{item.url && <a href={item.url} target="_blank" rel="noreferrer" style={s.tag}>Open</a>}</div>
  </article>
}

function TeamIntel({ boards }) {
  const rows = Object.values(boards || {})
  if (!rows.length) return <div style={s.empty}>No team intel board available yet.</div>
  return <div style={s.grid}>{rows.map(board => <div key={board.team} style={s.card}>
    <div style={s.row}><strong style={{ color: '#e6edf3' }}>{board.team}</strong><span style={s.badge}>{statusLabel(board.confidence_provider_status)}</span></div>
    <div style={s.meta}>{board.team_name}</div>
    <div style={s.meta}>Beat/local: {board.beat_report_count} · National: {board.national_headline_count} · Local sources: {board.local_source_count}</div>
    <div style={s.meta}>Latest local: {board.latest_local_item?.title || 'None configured'}</div>
    <div style={s.meta}>Injury/lineup/starter: {board.latest_injury_lineup_starter_item?.title || 'No item'}</div>
    <div style={s.tagRow}>{asArray(board.top_tags).slice(0, 6).map(tag => <span key={tag} style={s.tag}>{tag}</span>)}</div>
  </div>)}</div>
}

function MatchupTwitterIntel({ date, teams }) {
  const [mode, setMode] = useState('matchups')
  const [payload, setPayload] = useState(null)
  const [team, setTeam] = useState('CHC')
  const [query, setQuery] = useState('Cubs lineup')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  function endpoint() {
    if (mode === 'betting') return `${API}/news/twitter/betting?date=${date}&limit=50`
    if (mode === 'team') return `${API}/news/twitter/team?team=${encodeURIComponent(team || 'CHC')}&date=${date}&limit=50`
    if (mode === 'search') return `${API}/news/twitter/search?query=${encodeURIComponent(query || 'MLB lineup')}&date=${date}&limit=50`
    if (mode === 'baseball_feed') return `${API}/news/twitter/baseball-feed?date=${date}&limit=50`
    return `${API}/news/twitter/matchups?date=${date}&limit=20`
  }

  function loadTwitter() {
    setLoading(true)
    setError(null)
    fetch(endpoint())
      .then(async r => { if (!r.ok) throw new Error(`/news/twitter/${mode} failed ${r.status}: ${await r.text()}`); return r.json() })
      .then(data => { setPayload(data); setLoading(false) })
      .catch(err => { setPayload(null); setError(String(err?.message || err)); setLoading(false) })
  }

  useEffect(() => { loadTwitter() }, [date, mode])

  const message = payload?.message || asArray(payload?.errors)[0]
  const blocks = asArray(payload?.matchups)
  const flatItems = asArray(payload?.items)
  const disabled = ['provider_not_configured', 'provider_missing_dependency'].includes(payload?.status)
  const flatCap = mode === 'matchups' ? 20 : 50

  return <section style={s.section}>
    <div style={s.row}>
      <div><div style={s.sectionTitle}>Matchup Twitter Intel</div><div style={s.meta}>Apify-backed X/Twitter layer ranked by engagement, relevance, and baseball signal. Matchups cap at 20 quality tweets per game; search/team/feed modes return up to 50.</div></div>
      <div style={s.tabs}>
        {['matchups', 'betting', 'team', 'search', 'baseball_feed'].map(name => <button key={name} type="button" style={s.tab(mode === name)} onClick={() => setMode(name)}>{name === 'matchups' ? 'Twitter Matchups' : name === 'betting' ? 'Twitter Betting' : name === 'team' ? 'Twitter Team Search' : name === 'baseball_feed' ? 'Baseball Feed' : 'Twitter Search'}</button>)}
        <button type="button" style={s.muted} onClick={loadTwitter} disabled={loading}>{loading ? 'Loading...' : 'Refresh Twitter'}</button>
      </div>
    </div>
    <div style={s.meta}>Ranking: {payload?.ranking || 'twitter_quality_score_desc'} · Cache: {payload?.cache_hit ? 'Hit' : 'Fresh'} · Cap: {payload?.tweet_cap_per_game || payload?.tweet_cap || flatCap}</div>
    {mode === 'team' && <div style={{ ...s.filters, marginTop: 12 }}><label><div style={s.label}>Team</div><select style={s.select} value={team} onChange={e => setTeam(e.target.value)}>{asArray(teams).map(t => <option key={t} value={t}>{t}</option>)}</select></label><div style={{ display: 'flex', alignItems: 'end' }}><button type="button" style={s.button} onClick={loadTwitter}>Search Team</button></div></div>}
    {mode === 'search' && <div style={{ ...s.filters, marginTop: 12 }}><label><div style={s.label}>Keyword Search</div><input style={s.input} value={query} onChange={e => setQuery(e.target.value)} placeholder="Cubs lineup, MLB sharp money" /></label><div style={{ display: 'flex', alignItems: 'end' }}><button type="button" style={s.button} onClick={loadTwitter}>Search X/Twitter</button></div></div>}
    {error && <div style={{ ...s.error, marginTop: 12 }}>{error}</div>}
    {disabled && <div style={{ ...s.empty, marginTop: 12 }}>Twitter/X provider is disabled or missing credentials. Configure NEWS_X_PROVIDER=apify, APIFY_TOKEN, and TWITTER_X_ACTOR_ID to enable matchup intel.</div>}
    {!disabled && message && <div style={{ ...s.error, marginTop: 12 }}>{message}</div>}
    {!disabled && mode === 'matchups' && blocks.length === 0 && <div style={{ ...s.empty, marginTop: 12 }}>No matchup Twitter intel returned yet.</div>}
    {!disabled && mode === 'matchups' && blocks.length > 0 && <div style={{ display: 'grid', gap: 12, marginTop: 12 }}>{blocks.map(block => <article key={block.game_id} style={s.card}>
      <div style={s.row}><div><div style={s.cardTitle}>{block.away_team || 'Away'} at {block.home_team || 'Home'}</div><div style={s.meta}>{block.away_pitcher || 'Away pitcher pending'} vs {block.home_pitcher || 'Home pitcher pending'} · {block.matchup_source || 'source pending'}</div></div><span style={s.badge}>{asArray(block.items).length}/20 posts</span></div>
      <div style={s.tagRow}>{asArray(block.queries).map(q => <span key={q} style={s.tag}>{q}</span>)}</div>
      {asArray(block.items).length === 0 ? <div style={s.empty}>No high-quality posts returned for this matchup yet.</div> : <div style={{ display: 'grid', gap: 10, marginTop: 12 }}>{asArray(block.items).slice(0, 20).map(item => <TweetCard key={item.id} item={item} />)}</div>}
    </article>)}</div>}
    {!disabled && mode !== 'matchups' && flatItems.length === 0 && <div style={{ ...s.empty, marginTop: 12 }}>No Twitter/X posts returned for this search.</div>}
    {!disabled && mode !== 'matchups' && flatItems.length > 0 && <div style={{ display: 'grid', gap: 10, marginTop: 12 }}>{flatItems.slice(0, 50).map(item => <TweetCard key={item.id} item={item} />)}</div>}
  </section>
}

export default function NewsPage() {
  const [date, setDate] = useState(getMlbLiveDate())
  const [payload, setPayload] = useState(null)
  const [intel, setIntel] = useState(null)
  const [sources, setSources] = useState([])
  const [activeBucket, setActiveBucket] = useState('hourly')
  const [team, setTeam] = useState('')
  const [tag, setTag] = useState('')
  const [sourceType, setSourceType] = useState('')
  const [search, setSearch] = useState('')
  const [breakingOnly, setBreakingOnly] = useState(false)
  const [bettingOnly, setBettingOnly] = useState(false)
  const [localOnly, setLocalOnly] = useState(false)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  function load() {
    setLoading(true)
    setError(null)
    Promise.all([
      fetch(`${API}/news/all?date=${date}`).then(async r => { if (!r.ok) throw new Error(`/news/all failed ${r.status}: ${await r.text()}`); return r.json() }),
      fetch(`${API}/news/team-intel?date=${date}`).then(async r => { if (!r.ok) throw new Error(`/news/team-intel failed ${r.status}: ${await r.text()}`); return r.json() }),
      fetch(`${API}/news/sources`).then(async r => { if (!r.ok) throw new Error(`/news/sources failed ${r.status}: ${await r.text()}`); return r.json() }),
    ]).then(([news, boards, sourcePayload]) => {
      setPayload(news)
      setIntel(boards)
      setSources(asArray(sourcePayload.sources))
      setLoading(false)
    }).catch(err => { setError(String(err?.message || err)); setLoading(false) })
  }

  useEffect(() => { load() }, [date])

  const teams = useMemo(() => asArray(sources).map(row => row.team).filter(Boolean), [sources])
  const allItems = useMemo(() => BUCKETS.flatMap(bucket => asArray(payload?.buckets?.[bucket])), [payload])
  const tags = useMemo(() => Array.from(new Set(allItems.flatMap(item => asArray(item.tags)))).sort(), [allItems])
  const sourceTypes = useMemo(() => Array.from(new Set(allItems.map(item => item.source_type).filter(Boolean))).sort(), [allItems])
  const visible = useMemo(() => {
    const base = activeBucket === 'team_intel' ? [] : asArray(payload?.buckets?.[activeBucket])
    const q = search.trim().toLowerCase()
    return base.filter(item => {
      const text = `${item.title || ''} ${item.summary || ''} ${item.source || ''}`.toLowerCase()
      if (q && !text.includes(q)) return false
      if (team && !text.includes(team.toLowerCase()) && !asArray(item.teams).includes(team)) return false
      if (tag && !asArray(item.tags).includes(tag)) return false
      if (sourceType && item.source_type !== sourceType) return false
      if (breakingOnly && !item.is_breaking) return false
      if (bettingOnly && !item.is_betting_relevant) return false
      if (localOnly && !item.is_local && !item.is_beat_report) return false
      return true
    })
  }, [payload, activeBucket, team, tag, sourceType, search, breakingOnly, bettingOnly, localOnly])

  return <div style={s.page}>
    <Ticker rows={asArray(payload?.ticker)} status={payload?.status} provider={payload?.provider} />
    <section style={s.hero}>
      <div style={s.row}>
        <div><div style={s.eyebrow}>Bloomberg style MLB clubhouse and betting intelligence</div><h1 style={s.title}>News</h1><div style={s.subtitle}>A command center for injuries, lineup hints, starter changes, bullpen availability, local beat context, weather chatter, and betting-market relevant items. Provider placeholders are intentionally safe when credentials are missing.</div></div>
        <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap' }}><input style={s.input} type="date" value={date} onChange={e => setDate(e.target.value)} /><button style={s.button} type="button" onClick={load} disabled={loading}>{loading ? 'Refreshing...' : 'Refresh'}</button></div>
      </div>
      <div style={s.stats}>
        <div style={s.stat}><div style={s.statLabel}>Provider</div><div style={s.statValue}>{providerLabel(payload?.provider)}</div></div>
        <div style={s.stat}><div style={s.statLabel}>Status</div><div style={s.statValue}>{statusLabel(payload?.status)}</div></div>
        <div style={s.stat}><div style={s.statLabel}>Items</div><div style={s.statValue}>{bucketCount(payload)}</div></div>
        <div style={s.stat}><div style={s.statLabel}>Last Updated</div><div style={s.statValue}>{fmtTime(payload?.generated_at)}</div></div>
        <div style={s.stat}><div style={s.statLabel}>Cache</div><div style={s.statValue}>{payload?.cache_hit ? 'Hit' : 'Fresh'}</div></div>
      </div>
    </section>
    {error && <div style={s.error}>{error}</div>}
    {asArray(payload?.errors).length > 0 && <div style={s.error}>{payload.errors.slice(0, 3).map(err => String(err).slice(0, 180)).join(' · ')}</div>}
    <section style={s.section}>
      <div style={s.row}><div><div style={s.sectionTitle}>Filters</div><div style={s.meta}>Team, tag, source, breaking, betting, local, and text search.</div></div><button type="button" style={s.muted} onClick={() => { setTeam(''); setTag(''); setSourceType(''); setSearch(''); setBreakingOnly(false); setBettingOnly(false); setLocalOnly(false) }}>Reset Filters</button></div>
      <div style={s.filters}>
        <label><div style={s.label}>Team</div><select style={s.select} value={team} onChange={e => setTeam(e.target.value)}><option value="">All teams</option>{teams.map(t => <option key={t} value={t}>{t}</option>)}</select></label>
        <label><div style={s.label}>Tag</div><select style={s.select} value={tag} onChange={e => setTag(e.target.value)}><option value="">All tags</option>{tags.map(t => <option key={t} value={t}>{t}</option>)}</select></label>
        <label><div style={s.label}>Source Type</div><select style={s.select} value={sourceType} onChange={e => setSourceType(e.target.value)}><option value="">All source types</option>{sourceTypes.map(t => <option key={t} value={t}>{t}</option>)}</select></label>
        <label><div style={s.label}>Search</div><input style={s.input} value={search} onChange={e => setSearch(e.target.value)} placeholder="player, source, injury, lineup" /></label>
      </div>
      <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', marginTop: 12 }}><button style={breakingOnly ? s.button : s.muted} type="button" onClick={() => setBreakingOnly(v => !v)}>Breaking Only</button><button style={bettingOnly ? s.button : s.muted} type="button" onClick={() => setBettingOnly(v => !v)}>Betting Relevant</button><button style={localOnly ? s.button : s.muted} type="button" onClick={() => setLocalOnly(v => !v)}>Beat/Local Only</button><Link style={s.muted} to="/daily-odds">Daily Odds</Link><Link style={s.muted} to="/models/projections">Model Projections</Link></div>
    </section>
    <MatchupTwitterIntel date={date} teams={teams.length ? teams : ['CHC', 'CWS', 'STL', 'MIL']} />
    <section style={s.section}>
      <div style={s.row}><div><div style={s.sectionTitle}>Terminal Buckets</div><div style={s.meta}>Exclusive buckets prevent national wire spam from flooding every section.</div></div><div style={s.tabs}>{[...BUCKETS, 'team_intel'].map(bucket => <button key={bucket} type="button" style={s.tab(activeBucket === bucket)} onClick={() => setActiveBucket(bucket)}>{bucket.replace('_', ' ')}</button>)}</div></div>
      {activeBucket === 'team_intel' ? <TeamIntel boards={intel?.team_intel} /> : visible.length === 0 ? <div style={s.empty}>{emptyMessage(payload)}</div> : <div style={s.grid}>{visible.map(item => <NewsCard key={item.id} item={item} />)}</div>}
    </section>
  </div>
}
