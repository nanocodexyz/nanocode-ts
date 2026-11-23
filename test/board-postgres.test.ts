import assert from 'node:assert/strict'
import { spawn, type ChildProcess } from 'node:child_process'
import { createInterface } from 'node:readline'
import { after, describe, test } from 'node:test'
import { fileURLToPath } from 'node:url'
import { Board } from 'nanocode-ts/vercel'
import { Sql } from 'nanocode-ts/postgres'

const url = process.env.DATABASE_URL

describe('Postgres statement translation', () => {
  test('every nanocode_log statement maps to its Postgres twin', () => {
    const append = Sql.translate(
      'INSERT INTO nanocode_log (scope, seq, body) VALUES (?1, CAST(?2 AS INTEGER), ?3)',
    )
    assert.equal(
      append,
      'INSERT INTO nanocode_log (scope, seq, body) VALUES ($1::text, CAST($2::text AS BIGINT), $3::text)',
    )
    const create = Sql.translate(
      'CREATE TABLE IF NOT EXISTS nanocode_log (scope TEXT NOT NULL, seq INTEGER NOT NULL, body TEXT NOT NULL, PRIMARY KEY (scope, seq)) WITHOUT ROWID',
    )
    assert.doesNotMatch(create, /ROWID/)
    assert.match(create, /seq BIGINT/)
  })

  test('portable statements only renumber their placeholders', () => {
    assert.equal(Sql.translate('SELECT ?1, ?2, ?1'), 'SELECT $1, $2, $1')
    assert.equal(Sql.translate('SELECT ? WHERE a = ?'), 'SELECT $1 WHERE a = $2')
    assert.equal(Sql.translate("SELECT '?' || ?"), "SELECT '?' || $1")
    assert.equal(Sql.translate('SELECT 1'), 'SELECT 1')
  })

  test('SQLite-only statements without a twin are refused', () => {
    assert.throws(
      () => Sql.translate('CREATE TABLE t (a INTEGER PRIMARY KEY) WITHOUT ROWID'),
      /no Postgres form/,
    )
    assert.throws(() => Sql.translate('PRAGMA journal_mode'), /no Postgres form/)
  })
})

describe('Postgres reconnect backoff', () => {
  test('defaults to 50 ms doubling up to 1 s', () => {
    assert.deepEqual([1, 2, 3, 4, 5, 9].map(Sql.reconnect), [0.1, 0.2, 0.4, 0.8, 1, 1])
  })

  test('Sql.open takes its own backoff, which a failed connect consults', async () => {
    const attempts: number[] = []
    // Port 1 refuses the connection, so no server is needed.
    const host = Sql.open('postgres://nobody@127.0.0.1:1/none', {
      backoff: (attempt) => (attempts.push(attempt), 0.01),
    })
    await assert.rejects(host.transaction('log', 'get', [['SELECT 1']], 'read'))
    assert.ok(attempts.length > 0 && attempts[0] === 1, JSON.stringify(attempts))
    await host.close()
    assert.throws(
      () => Sql.open('postgres://nobody@127.0.0.1:1/none', { backoff: 1 as never }),
      /backoff must be a function/,
    )
  })
})

if (!url) {
  test('Board over Postgres', { skip: 'DATABASE_URL is not set' }, () => {})
} else {
  process.env.NC_BOARD_POSTGRES = url
  await import('./board.test.ts')

  const scope = 'takeover'
  const schema = 'nc_board_takeover_' + process.pid + '_' + Date.now().toString(36)
  const childFile = fileURLToPath(new URL('./board-postgres.child.ts', import.meta.url))
  const children = new Set<ChildProcess>()

  after(async () => {
    for (const child of children) child.kill('SIGKILL')
    const admin = Sql.open(url)
    try {
      await admin.transaction(
        'log',
        'drop',
        [['DROP SCHEMA IF EXISTS ' + schema + ' CASCADE']],
        'schema',
      )
    } finally {
      await admin.close()
    }
  })

  type Line = { e: string } & Record<string, unknown>

  const start = (role: object) => {
    const child = spawn(
      process.execPath,
      [childFile, JSON.stringify({ ...role, url, schema, scope })],
      {
        stdio: ['ignore', 'pipe', 'pipe'],
      },
    )
    children.add(child)
    child.on('exit', () => children.delete(child))
    let stderr = ''
    child.stderr!.on('data', (chunk: Buffer) => (stderr += chunk.toString()))
    const queue: Line[] = []
    const waiting: ((line: Line | undefined) => void)[] = []
    let ended = false
    createInterface({ input: child.stdout! }).on('line', (text) => {
      let line: Line
      try {
        line = JSON.parse(text) as Line
      } catch {
        return
      }
      const next = waiting.shift()
      if (next) next(line)
      else queue.push(line)
    })
    child.on('close', () => {
      ended = true
      for (const next of waiting.splice(0)) next(undefined)
    })
    const next = async (e: string): Promise<Line> => {
      for (;;) {
        const line =
          queue.shift() ??
          (ended
            ? undefined
            : await new Promise<Line | undefined>((resolve) => waiting.push(resolve)))
        if (!line) throw new Error('child exited before ' + e + ': ' + stderr.slice(0, 2000))
        if (line.e === 'error') throw new Error('child failed: ' + String(line.message))
        if (line.e === e) return line
      }
    }
    return { child, next }
  }

  const opened = (async () => {
    const sql = Sql.open(url, { schema })
    const board = await Board.open({ sql, scope, actor: 'agent:parent' })
    const granted = await board.grant(
      { who: [{ kind: 'kind', id: 'agent' }], can: { '*': ['manage'] } },
      { key: 'grant-agents' },
    )
    assert.ok(granted.ok, JSON.stringify(granted))
    return { sql, board }
  })()

  describe('Board over Postgres across processes', { timeout: 120_000 }, () => {
    test('a lease held by a killed process is taken over by another process', async () => {
      const { board } = await opened
      assert.ok((await board.post({ id: 'k', spec: 'killed mid-task' })).ok)

      const doomed = start({ role: 'hold', actor: 'doomed', task: 'k', leaseMs: 1_500 })
      const held = await doomed.next('claimed')
      assert.equal(held.ok, true, JSON.stringify(held))
      doomed.child.kill('SIGKILL')
      const untilMs = held.untilMs as number

      await board.refresh()
      const blocked = await board.claim('k', { leaseMs: 1_000 })
      if (Date.now() < untilMs - 100) {
        assert.equal(blocked.ok, false, JSON.stringify(blocked))
        assert.equal((blocked as { rejection: Board.Rejection }).rejection.kind, 'Held')
      }

      const rescuer = start({ role: 'rescue', actor: 'rescuer', task: 'k', leaseMs: 60_000 })
      const rescued = await rescuer.next('finished')
      assert.equal(rescued.tookOver, true, JSON.stringify(rescued))
      assert.ok((rescued.claimedAt as number) >= untilMs, JSON.stringify({ rescued, untilMs }))

      await board.refresh()
      const task = await board.task('k')
      assert.equal(task?.phase, 'done')
      assert.deepEqual(task?.outcome, { done: 'rescuer' })
      const stale = await board.finish('k', held.claim as string, { done: 'doomed' })
      assert.equal(stale.ok, false, 'a dead lease must not finish the task again')
    })

    test('two processes racing for one task: exactly one claim wins', async () => {
      const { board } = await opened
      const ids = Array.from({ length: 12 }, (_, index) => 'race-' + index)
      for (const id of ids) assert.ok((await board.post({ id, spec: 'raced' })).ok)
      const atMs = Date.now() + 1_500
      const left = start({ role: 'race', actor: 'left', tasks: ids, atMs })
      const right = start({ role: 'race', actor: 'right', tasks: ids, atMs })
      const [a, b] = await Promise.all([left.next('raced'), right.next('raced')])
      const won = [a.won as string[], b.won as string[]]
      assert.deepEqual([...won[0]!, ...won[1]!].sort(), [...ids].sort(), JSON.stringify({ a, b }))
      assert.equal(won[0]!.filter((id) => won[1]!.includes(id)).length, 0)
      await board.refresh()
      for (const id of ids) {
        const task = await board.task(id)
        assert.equal(task?.phase, 'claimed', id)
      }
    })

    test('a mixed-case schema is created and used as written', async () => {
      const mixed = 'Nc_Mixed_' + process.pid
      const host = Sql.open(url, { schema: mixed })
      try {
        await host.transaction('log', 'create', [['CREATE TABLE t (a TEXT)']], 'schema')
        await host.transaction('log', 'put', [['INSERT INTO t (a) VALUES (?1)', 'x']])
        const [rows] = await host.transaction('log', 'get', [['SELECT a FROM t']], 'read')
        assert.deepEqual(rows, [['x']])
      } finally {
        await host.transaction('log', 'drop', [['DROP SCHEMA "' + mixed + '" CASCADE']], 'schema')
        await host.close()
      }
    })

    test('close', async () => {
      const { board, sql } = await opened
      await board.close()
      await sql.close()
    })
  })
}
