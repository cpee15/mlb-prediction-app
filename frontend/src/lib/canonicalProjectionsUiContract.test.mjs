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

test('projections tab follows simulation and precedes diagnostics', async () => {
  const source = await pageSource()

  assert.match(
    source,
    /\['simulation', 'Simulation'\],\s*\['projections', 'Projections'\],\s*\['diagnostics', 'Diagnostics'\]/,
  )
})

test('projections tab consumes canonical projections view model', async () => {
  const source = await pageSource()

  assert.match(
    source,
    /buildCanonicalProjectionsViewModel/,
  )
  assert.match(
    source,
    /Canonical Player Projections/,
  )
  assert.match(
    source,
    /<ProjectionsTab game=\{game\}/,
  )
})

test('projections tab describes same-run coherence', async () => {
  const source = await pageSource()

  assert.match(
    source,
    /exact same trial\s*batch/i,
  )
  assert.match(
    source,
    /Non-authoritative shadow/,
  )
})

test('projections tab renders requested batter metrics', async () => {
  const source = await pageSource()

  for (const label of [
    'PA',
    'RBI',
    '1B',
    '2B',
    '3B',
    'HR',
    'BB',
    'DK Mean',
    'DK Floor',
    'DK Median',
    'DK Ceiling',
  ]) {
    assert.match(source, new RegExp(label))
  }
})
