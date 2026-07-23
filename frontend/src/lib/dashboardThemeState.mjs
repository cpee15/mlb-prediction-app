export const DASHBOARD_THEME_KEY = 'mlbgpt-dashboard-theme:v1'
export const DASHBOARD_THEME_OPTIONS = ['light', 'dark', 'system']

export function normalizeDashboardTheme(value) {
  return DASHBOARD_THEME_OPTIONS.includes(value) ? value : 'system'
}

export function resolveDashboardTheme(preference, systemDark = false) {
  const normalized = normalizeDashboardTheme(preference)
  return normalized === 'system' ? (systemDark ? 'dark' : 'light') : normalized
}

export function dashboardThemeVariables(theme) {
  const dark = theme === 'dark'
  return {
    '--md-bg': dark ? '#090b11' : '#eceef4',
    '--md-text': dark ? '#f4f5f8' : '#111318',
    '--md-muted': dark ? '#aeb4c2' : '#5f6675',
    '--md-border': dark ? 'rgba(226,232,240,.16)' : 'rgba(55,65,81,.17)',
    '--md-panel': dark ? 'rgba(22,25,34,.86)' : 'rgba(255,255,255,.78)',
    '--md-panel-2': dark ? 'rgba(32,36,47,.82)' : 'rgba(241,243,248,.82)',
    '--md-control': dark ? 'rgba(39,44,57,.88)' : 'rgba(255,255,255,.66)',
    '--md-control-soft': dark ? 'rgba(45,50,64,.72)' : 'rgba(255,255,255,.58)',
    '--md-page-bg': dark
      ? 'radial-gradient(circle at 88% 0%,rgba(112,100,245,.18),transparent 30%),radial-gradient(circle at 4% 36%,rgba(64,70,88,.2),transparent 30%),linear-gradient(145deg,#090b11,#181c27)'
      : 'radial-gradient(circle at 88% 0%,rgba(112,100,245,.14),transparent 28%),radial-gradient(circle at 4% 36%,rgba(255,255,255,.95),transparent 30%),linear-gradient(145deg,#f8f9fc,#dfe3eb)',
    '--md-auth-bg': dark
      ? 'radial-gradient(circle at 12% 18%,rgba(92,97,117,.28) 0,rgba(21,24,34,.7) 24%,transparent 48%),linear-gradient(135deg,#090b11,#202532)'
      : 'radial-gradient(circle at 12% 18%,#fff 0,rgba(255,255,255,.7) 22%,transparent 45%),linear-gradient(135deg,#f7f8fb,#d8dce5)',
    '--md-chrome': dark
      ? 'radial-gradient(circle at 28% 25%,#cfd3dc 0 7%,#4c5363 16%,#a7acb8 24%,#6f63f5 27%,#343948 33%,#10131b 39%,#868d9c 46%,transparent 49%)'
      : 'radial-gradient(circle at 28% 25%,#fff 0 7%,#b8c0d0 16%,#fff 24%,#6f63f5 27%,#e9ecf4 33%,#6f7786 39%,#fff 46%,transparent 49%)',
    '--md-glass': dark
      ? 'linear-gradient(145deg,rgba(39,43,55,.88),rgba(17,20,28,.78))'
      : 'linear-gradient(145deg,rgba(255,255,255,.82),rgba(232,235,243,.56))',
    '--md-glass-strong': dark
      ? 'linear-gradient(145deg,rgba(38,42,54,.92),rgba(20,23,32,.82))'
      : 'linear-gradient(145deg,rgba(255,255,255,.82),rgba(231,234,241,.7))',
    '--md-glass-border': dark ? 'rgba(255,255,255,.16)' : 'rgba(255,255,255,.78)',
    '--md-inset': dark ? 'inset 0 1px rgba(255,255,255,.12)' : 'inset 0 1px rgba(255,255,255,.95)',
    '--md-shadow': dark ? '0 22px 64px rgba(0,0,0,.34)' : '0 20px 58px rgba(49,55,71,.16)',
    '--md-table-head': dark ? '#202532' : '#eceff5',
    '--md-report-bg': dark ? '#10131b' : '#f7f8fb',
    '--md-label': dark ? '#d8dce5' : '#303540',
    '--md-color-scheme': dark ? 'dark' : 'light',
  }
}
