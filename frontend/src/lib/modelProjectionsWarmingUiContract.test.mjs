import assert from 'node:assert/strict'
import fs from 'node:fs'
import test from 'node:test'

const source = fs.readFileSync(
  new URL('../pages/ModelProjectionsPage.jsx', import.meta.url),
  'utf8',
)

test('model projections bypass stale browser API cache', () => {
  assert.match(
    source,
    /fetch\(url, \{ cache: 'no-store' \}\)/,
  )
})

test('model projections distinguish unwarmed dates', () => {
  assert.match(source, /data_status === 'not_ready'/)
  assert.match(
    source,
    /payload\?\.message \|\| 'Model projections are being prepared for this date\.'/,
  )
})
