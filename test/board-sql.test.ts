import assert from 'node:assert/strict'
import { spawn, spawnSync } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { after, test } from 'node:test'
import { Sql } from 'nanocode-ts/node'

// The SqlHost the Board suite runs on: node:sqlite in WAL mode, shared across connections and
// processes, on every Node with node:sqlite (with or without statement.setReturnArrays).

const root = mkdtempSync(join(tmpdir(), 'nc-sql-'))
let files = 0
const file = () => join(root, 'sql-' + files++ + '.sqlite')
after(() => rmSync(root, { recursive: true, force: true }))

const write = (
  host: Sql.Host,
  ...statements: [string, ...(string | number | ArrayBuffer | null)[]][]
) => host.transaction('store', 'write', statements, 'write')
const read = (
  host: Sql.Host,
  ...statements: [string, ...(string | number | ArrayBuffer | null)[]][]
) => host.transaction('store', 'read', statements, 'read')

test('a file opens in WAL mode and round-trips text, numbers, nulls and blobs', () => {
  const host = Sql.open(file())
  assert.deepEqual(read(host, ['PRAGMA journal_mode'])[0], [['wal']])
  write(host, ['CREATE TABLE t (k TEXT PRIMARY KEY, v BLOB, n INTEGER, r REAL)'])
  const blob = new Uint8Array([0, 1, 254, 255]).buffer
  write(
    host,
    ['INSERT INTO t VALUES (?1, ?2, ?3, ?4)', 'a', blob, 2 ** 53 - 1, 1.5],
    ['INSERT INTO t VALUES (?1, NULL, NULL, NULL)', 'b'],
  )
  const [rows] = read(host, ['SELECT k, v, n, r FROM t ORDER BY k'])
  assert.equal(rows!.length, 2)
  const [k, v, n, r] = rows![0]!
  assert.equal(k, 'a')
  assert.ok(v instanceof ArrayBuffer)
  assert.deepEqual([...new Uint8Array(v)], [0, 1, 254, 255])
  assert.equal(n, 2 ** 53 - 1)
  assert.equal(r, 1.5)
  assert.deepEqual(rows![1], ['b', null, null, null])
  host.close()
  assert.throws(() => read(host, ['SELECT 1']), /closed/)
})

test('rows keep repeated column names and reused numbered parameters', () => {
  const host = Sql.open(file())
  assert.deepEqual(read(host, ['SELECT ?1 AS a, ?1 AS a, ?2, ?', 'x', 'y', 'z'])[0], [
    ['x', 'x', 'y', 'z'],
  ])
  assert.deepEqual(read(host, ["SELECT '?1' AS q, ?1 -- ?2\n", 'p'])[0], [['?1', 'p']])
  write(host, ['CREATE TABLE t (a TEXT, b TEXT)'])
  assert.deepEqual(write(host, ['INSERT INTO t VALUES (?1, ?1) RETURNING a, b', 'r'])[0], [
    ['r', 'r'],
  ])
  assert.deepEqual(read(host, ['WITH w AS (SELECT a FROM t) SELECT a, a FROM w'])[0], [['r', 'r']])
  host.close()
})

test('a failed statement rolls back the whole transaction', () => {
  const host = Sql.open(file())
  write(host, ['CREATE TABLE t (k TEXT PRIMARY KEY)'], ["INSERT INTO t VALUES ('a')"])
  assert.throws(
    () => write(host, ["INSERT INTO t VALUES ('b')"], ["INSERT INTO t VALUES ('a')"]),
    /UNIQUE/,
  )
  assert.deepEqual(read(host, ['SELECT k FROM t ORDER BY k'])[0], [['a']])
  write(host, ["INSERT INTO t VALUES ('c')"])
  assert.deepEqual(read(host, ['SELECT count(*) FROM t'])[0], [[2]])
  host.close()
})

test('connections and processes share one file', () => {
  const path = file()
  const a = Sql.open(path)
  const b = Sql.open(path)
  write(a, ['CREATE TABLE t (n INTEGER)'])
  write(a, ['INSERT INTO t VALUES (1)'])
  assert.deepEqual(read(b, ['SELECT n FROM t'])[0], [[1]])

  const script =
    "import { Sql } from 'nanocode-ts/node'\n" +
    'const host = Sql.open(process.argv[1])\n' +
    "for (let i = 0; i < 200; i++) host.transaction('t', 'w', [['INSERT INTO t SELECT max(n) + 1 FROM t']], 'write')\n" +
    'host.close()\n'
  const children = Array.from({ length: 2 }, () =>
    spawnSync(process.execPath, [...process.execArgv, '--input-type=module', '-e', script, path], {
      encoding: 'utf8',
    }),
  )
  for (const child of children) assert.equal(child.status, 0, child.stderr)
  for (let i = 0; i < 200; i++) write(b, ['INSERT INTO t SELECT max(n) + 1 FROM t'])
  assert.deepEqual(read(a, ['SELECT count(*), count(DISTINCT n), max(n) FROM t'])[0], [
    [601, 601, 601],
  ])
  a.close()
  b.close()
})

test('a writer waits for another process to release the write lock', async () => {
  const path = file()
  const host = Sql.open(path, { busyTimeoutMs: 5_000 })
  write(host, ['CREATE TABLE t (n INTEGER)'])
  const script =
    "import { DatabaseSync } from 'node:sqlite'\n" +
    'const db = new DatabaseSync(process.argv[1])\n' +
    "db.exec('BEGIN IMMEDIATE'); db.exec('INSERT INTO t VALUES (1)')\n" +
    "process.stdout.write('locked\\n')\n" +
    "setTimeout(() => { db.exec('COMMIT'); db.close() }, 400)\n"
  const child = spawn(
    process.execPath,
    [...process.execArgv, '--input-type=module', '-e', script, path],
    {
      stdio: ['ignore', 'pipe', 'inherit'],
    },
  )
  const exited = new Promise<number | null>((resolve) => child.on('close', resolve))
  await new Promise<void>((resolve) =>
    child.stdout.on('data', (chunk) => String(chunk).includes('locked') && resolve()),
  )
  const started = Date.now()
  write(host, ['INSERT INTO t VALUES (2)'])
  assert.ok(Date.now() - started >= 200, 'the write did not wait for the lock')
  assert.equal(await exited, 0)
  assert.deepEqual(read(host, ['SELECT n FROM t ORDER BY n'])[0], [[1], [2]])
  host.close()
  assert.throws(() => Sql.open(path, { busyTimeoutMs: -1 }), /busyTimeoutMs/)
})
