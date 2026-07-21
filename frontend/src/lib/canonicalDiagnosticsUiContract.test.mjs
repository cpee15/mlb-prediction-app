import assert from 'node:assert/strict'
import { readFile } from 'node:fs/promises'
import test from 'node:test'

const pageUrl = new URL(
  '../pages/ModelProjectionsPage.jsx',
  import.meta.url,
)

async function pageSource() {
  return readFile(pageUrl, 'utf8')
}

test('diagnostics page consumes canonical view model', async () => {
  const source = await pageSource()

  assert.match(
    source,
    /buildCanonicalDiagnosticsViewModel/,
  )
  assert.match(
    source,
    /Canonical Simulation Diagnostics/,
  )
})

test('legacy diagnostic model cards are removed', async () => {
  const source = await pageSource()

  assert.doesNotMatch(
    source,
    /function DiagnosticModelCard/,
  )
  assert.doesNotMatch(
    source,
    /diagnosticModels\.map/,
  )
  assert.doesNotMatch(
    source,
    /No diagnostic models available/,
  )
})

test('raw payload is advanced and collapsed', async () => {
  const source = await pageSource()

  assert.match(
    source,
    /Advanced: raw canonical payload/,
  )
  assert.doesNotMatch(
    source,
    /Shared simulation payload/,
  )
})

test('realism diagnostics live inside diagnostics tab', async () => {
  const source = await pageSource()

  assert.doesNotMatch(
    source,
    /renderGameStateRealismDiagnostics/,
  )
  assert.match(
    source,
    /Game-State Realism/,
  )
})
