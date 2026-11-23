import assert from 'node:assert/strict'
import * as Fs from 'node:fs'
import * as Os from 'node:os'
import * as Path from 'node:path'
import { after, test } from 'node:test'
import * as Fixtures from '../scripts/test-fixtures.ts'
import * as Wasm from '../scripts/wasm.mjs'

const root = Fs.mkdtempSync(Path.join(Os.tmpdir(), 'nc-wasm-fixtures-'))
after(() => Fs.rmSync(root, { force: true, recursive: true }))
const bytes = Fs.readFileSync(Path.join(Wasm.root, 'dist/nanocode_wasm.wasm'))

function fixture() {
  const directory = Fs.mkdtempSync(Path.join(root, 'case-'))
  const receipt = Path.join(directory, 'receipt.json')
  const manifest = {
    fingerprint: Wasm.fingerprint(),
    files: { 'nanocode_wasm.wasm': Wasm.sha256(bytes) },
  }
  Fs.writeFileSync(Path.join(directory, 'nanocode_wasm.wasm'), bytes)
  Fs.writeFileSync(receipt, JSON.stringify(manifest))
  return { directory, manifest, receipt }
}

test('the fixture guard accepts the current real WASM module and a module-only receipt', () => {
  Fixtures.validate(fixture())
})

test('the fixture guard rejects a different source fingerprint', () => {
  const one = fixture()
  one.manifest.fingerprint = 'different-source'
  Fs.writeFileSync(one.receipt, JSON.stringify(one.manifest))
  assert.throws(() => Fixtures.validate(one), /current source fingerprint/)
})

test('the fixture guard rejects a changed WASM module', () => {
  const one = fixture()
  Fs.appendFileSync(Path.join(one.directory, 'nanocode_wasm.wasm'), new Uint8Array([0]))
  assert.throws(() => Fixtures.validate(one), /does not match its receipt/)
})

test('the fixture guard rejects an absent WASM module', () => {
  const one = fixture()
  Fs.rmSync(Path.join(one.directory, 'nanocode_wasm.wasm'))
  assert.throws(() => Fixtures.validate(one), /ENOENT/)
})

test('the fixture guard rejects an absent module digest', () => {
  const one = fixture()
  Fs.writeFileSync(
    one.receipt,
    JSON.stringify({ fingerprint: one.manifest.fingerprint, files: {} }),
  )
  assert.throws(() => Fixtures.validate(one), /does not match its receipt/)
})

test('the fixture guard rejects a missing or malformed receipt', () => {
  const one = fixture()
  Fs.rmSync(one.receipt)
  assert.throws(() => Fixtures.validate(one), /ENOENT/)
  Fs.writeFileSync(one.receipt, '{')
  assert.throws(() => Fixtures.validate(one), SyntaxError)
  Fs.writeFileSync(one.receipt, 'null')
  assert.throws(() => Fixtures.validate(one), /current source fingerprint/)
})
