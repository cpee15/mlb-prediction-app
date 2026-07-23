import assert from 'node:assert/strict'
import test from 'node:test'

import {
  dashboardThemeVariables,
  normalizeDashboardTheme,
  resolveDashboardTheme,
} from './dashboardThemeState.mjs'

test('dashboard theme preference accepts light, dark, and system only', () => {
  assert.equal(normalizeDashboardTheme('light'), 'light')
  assert.equal(normalizeDashboardTheme('dark'), 'dark')
  assert.equal(normalizeDashboardTheme('system'), 'system')
  assert.equal(normalizeDashboardTheme('owner'), 'system')
})

test('system theme follows the operating-system preference', () => {
  assert.equal(resolveDashboardTheme('system', false), 'light')
  assert.equal(resolveDashboardTheme('system', true), 'dark')
  assert.equal(resolveDashboardTheme('light', true), 'light')
  assert.equal(resolveDashboardTheme('dark', false), 'dark')
})

test('resolved themes provide complete surface and native-control variables', () => {
  const light = dashboardThemeVariables('light')
  const dark = dashboardThemeVariables('dark')
  for (const key of ['--md-text', '--md-panel', '--md-page-bg', '--md-auth-bg', '--md-color-scheme']) {
    assert.ok(light[key])
    assert.ok(dark[key])
    assert.notEqual(light[key], dark[key])
  }
})
