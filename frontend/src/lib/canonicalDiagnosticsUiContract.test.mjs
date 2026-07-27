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

test('not-run state hides empty canonical panels', async () => {
  const source = await pageSource()

  assert.match(
    source,
    /Not run for this payload/,
  )
  assert.match(
    source,
    /view\.hasCanonicalShadow/,
  )
  assert.match(
    source,
    /status\.availabilityReason/,
  )
})

test('raw canonical payload requires canonical shadow data', async () => {
  const source = await pageSource()

  assert.match(
    source,
    /view\.hasCanonicalShadow \? \(\s*<details/,
  )
})

test('not-run state renders bootstrap readiness blockers', async () => {
  const source = await pageSource()

  assert.match(
    source,
    /Activation readiness/,
  )
  assert.match(
    source,
    /bootstrap\.items\.map/,
  )
  assert.match(
    source,
    /bootstrap\.readyCount/,
  )
  assert.match(
    source,
    /Diagnostic only\. Readiness does not permit/,
  )
})


test('diagnostics distinguishes production monitoring', async () => {
  const source = await readFile(
    new URL(
      '../pages/ModelProjectionsPage.jsx',
      import.meta.url,
    ),
    'utf8',
  )

  assert.match(
    source,
    /Production Baserunning Monitoring/,
  )
  assert.match(
    source,
    /Frozen 100-game evidence window/,
  )
  assert.match(source, /status\.productionActive/)
  assert.match(
    source,
    /Global Profile Fallback Rate/,
  )
  assert.match(source, /Parameter Reselection/)
})
