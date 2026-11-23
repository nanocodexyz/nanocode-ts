import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { after, describe, test } from 'node:test'
import { fileURLToPath } from 'node:url'
import { Sql } from 'nanocode-ts/node'
import { Sql as Postgres } from 'nanocode-ts/postgres'
import { Board } from 'nanocode-ts/vercel'

// Conformance suite for the Board binding over a SQLite file. Writes pass explicit atMs so
// lease and backoff timing is exact; work() runs on the wall clock. Several processes share one
// file by running this same file with NC_BOARD_CHILD set to a JSON role. Actors are agent:<name>.

type Opened = { sql: Sql.Host | Postgres.Host; board: Board.Board; close(): Promise<void> }

const scope = 'conformance'
const postgres = process.env.NC_BOARD_POSTGRES

async function open(path: string, actor: string): Promise<Opened> {
  const sql = postgres ? Postgres.open(postgres, { schema: path }) : Sql.open(path)
  const board = await Board.open({ sql, scope, actor: 'agent:' + actor })
  // open() charters a new board for its actor; that first opener lets every agent manage it.
  const granted = await board.grant(
    { who: [{ kind: 'kind', id: 'agent' }], can: { '*': ['manage'] } },
    { key: 'grant-agents' },
  )
  if (!granted.ok) rejected(granted, 'Access')
  return {
    sql,
    board,
    async close() {
      await board.close()
      await sql.close()
    },
  }
}

function ok<T extends { ok: boolean }>(verdict: T): Extract<T, { ok: true }> {
  if (!verdict.ok)
    assert.fail(
      'expected ok, got ' + JSON.stringify((verdict as { rejection?: unknown }).rejection),
    )
  return verdict as Extract<T, { ok: true }>
}

function rejected(verdict: Board.Verdict | Board.Claimed, kind: Board.RejectionKind) {
  assert.equal(verdict.ok, false, 'expected ' + kind + ', got ' + JSON.stringify(verdict))
  const { rejection } = verdict as { rejection: Board.Rejection }
  assert.equal(rejection.kind, kind, JSON.stringify(rejection))
  return rejection
}

const ids = (tasks: Board.Task[]) => tasks.map((task) => task.id).sort()

async function task(board: Board.Board, id: string, atMs?: number) {
  const found = await board.task(id, atMs)
  assert.ok(found, 'no task ' + id)
  return found
}

type Role =
  | { role: 'drain'; path: string; actor: string }
  | { role: 'abandon'; path: string; actor: string; task: string; atMs: number }
  | { role: 'seed'; path: string; actor: string; atMs: number }

const marker = 'NC_BOARD_RESULT '

async function child(role: Role) {
  const { board, close } = await open(role.path, role.actor)
  let result: unknown
  if (role.role === 'drain') {
    const finished: string[] = []
    const refusals: Record<string, number> = {}
    for (;;) {
      await board.refresh()
      const ready = await board.ready()
      if (ready.length === 0) {
        const live = (await board.tasks()).filter(
          (one) => one.phase === 'open' || one.phase === 'claimed',
        )
        if (live.length === 0) break
        await new Promise((resolve) => setTimeout(resolve, 5))
        continue
      }
      for (const one of ready) {
        const claimed = await board.claim(one.id, { leaseMs: 60_000 })
        if (!claimed.ok) {
          refusals[claimed.rejection.kind] = (refusals[claimed.rejection.kind] ?? 0) + 1
          continue
        }
        const done = await board.finish(one.id, claimed.claim, { done: role.actor })
        if (!done.ok)
          throw new Error(one.id + ': finish refused: ' + JSON.stringify(done.rejection))
        finished.push(one.id)
      }
    }
    result = { finished, refusals }
  } else if (role.role === 'abandon') {
    result = await board.claim(role.task, { leaseMs: 1_000, atMs: role.atMs })
  } else {
    const verdicts = []
    for (const id of ['s1', 's2', 's3'])
      verdicts.push(
        await board.post({ id, spec: 'seeded ' + id }, { key: 'seed-' + id, atMs: role.atMs }),
      )
    const claimed = await board.claim('s1', { leaseMs: 600_000, atMs: role.atMs + 1 })
    if (!claimed.ok) throw new Error('seed claim refused: ' + JSON.stringify(claimed.rejection))
    const second = await board.claim('s2', { leaseMs: 600_000, atMs: role.atMs + 2 })
    if (!second.ok) throw new Error('seed claim refused: ' + JSON.stringify(second.rejection))
    const done = await board.finish('s2', second.claim, { done: 'seeded' }, role.atMs + 3)
    if (!done.ok) throw new Error('seed finish refused: ' + JSON.stringify(done.rejection))
    await board.steer('s1', 'seeded steer', { atMs: role.atMs + 4 })
    result = {
      posts: verdicts.map((verdict) => (verdict.ok ? verdict.seq : null)),
      claim: claimed.claim,
    }
  }
  await close()
  process.stdout.write(marker + JSON.stringify(result) + '\n')
}

function run<T>(role: Role): Promise<T> {
  const args = [
    ...process.execArgv.filter((arg) => !arg.startsWith('--test')),
    fileURLToPath(import.meta.url),
  ]
  return new Promise((resolve, reject) => {
    const proc = spawn(process.execPath, args, {
      env: { ...process.env, NC_BOARD_CHILD: JSON.stringify(role) },
      stdio: ['ignore', 'pipe', 'pipe'],
    })
    let out = ''
    let err = ''
    proc.stdout.on('data', (chunk) => (out += chunk))
    proc.stderr.on('data', (chunk) => (err += chunk))
    proc.on('error', reject)
    proc.on('close', (code) => {
      const line = out.split('\n').find((one) => one.startsWith(marker))
      if (code !== 0 || !line)
        return reject(new Error(role.role + ' child exited ' + code + '\n' + out + '\n' + err))
      resolve(JSON.parse(line.slice(marker.length)) as T)
    })
  })
}

if (process.env.NC_BOARD_CHILD) {
  await child(JSON.parse(process.env.NC_BOARD_CHILD) as Role)
} else {
  const root = mkdtempSync(join(tmpdir(), 'nc-board-'))
  let files = 0
  const schemas = 'nc_board_' + process.pid + '_' + Date.now().toString(36)
  const file = () =>
    postgres ? schemas + '_' + files++ : join(root, 'board-' + files++ + '.sqlite')
  after(async () => {
    rmSync(root, { recursive: true, force: true })
    if (!postgres) return
    const admin = Postgres.open(postgres)
    try {
      for (let index = 0; index < files; index++)
        await admin.transaction(
          'log',
          'drop',
          [['DROP SCHEMA IF EXISTS ' + schemas + '_' + index + ' CASCADE']],
          'schema',
        )
    } finally {
      await admin.close()
    }
  })

  describe(
    postgres ? 'Board over Postgres' : 'Board over node:sqlite',
    { timeout: 180_000 },
    () => {
      test('a reopen folds from its checkpoint to the state a full replay folds', async () => {
        const path = file()
        const t = Date.now()
        const first = await open(path, 'alice')
        const events = 10_000
        for (let from = 0; from < events; from += 500) {
          const writes = Array.from({ length: 500 }, (_, index): Board.Write => {
            const n = from + index
            return n % 4 === 3
              ? { op: { op: 'cancel', task: 't' + (n - 1), reason: 'gone' }, atMs: t }
              : { op: { op: 'post', task: { id: 't' + n, spec: 'task ' + n, labels: {}, needs: [] } }, atMs: t }
          })
          for (const verdict of await first.board.group(writes)) ok(verdict)
        }
        const live = await first.board.tasks(t)
        assert.ok(live.length >= (events / 4) * 3)
        assert.equal(await first.board.checkpoint(), true)
        await first.close()

        const reopened = await open(path, 'bob')
        const [opened] = await reopened.board.opened()
        // The checkpoint covers every row alice wrote, so the reopen folds no tail.
        assert.ok(opened?.from === 'checkpoint', JSON.stringify(opened))
        assert.equal(opened.tail, 0)
        assert.ok(opened.seq > 2)
        const restored = await reopened.board.tasks(t)
        await reopened.close()

        // Drop the checkpoints so the next open replays the whole log, and compare.
        const admin = postgres ? Postgres.open(postgres, { schema: path }) : Sql.open(path)
        try {
          await admin.transaction(
            'log',
            'drop_checkpoints',
            [['DELETE FROM nanocode_checkpoint WHERE scope = ?1', scope]],
            'write',
          )
        } finally {
          await admin.close()
        }
        const replayed = await open(path, 'carol')
        assert.deepEqual(await replayed.board.opened(), [{ from: 'replay', fallback: 'missing' }])
        const full = await replayed.board.tasks(t)
        await replayed.close()
        assert.deepEqual(restored, full)
        assert.deepEqual(restored, live)
      })

      test('needs keep a task Waiting until they are done', async () => {
        const { board, close } = await open(file(), 'alice')
        const t = Date.now()
        ok(await board.post({ id: 'a', spec: 'first' }, { atMs: t }))
        ok(await board.post({ id: 'b', spec: 'second', needs: ['a'] }, { atMs: t }))
        const unknown = rejected(
          await board.post({ id: 'c', spec: 'orphan', needs: ['missing'] }, { atMs: t }),
          'UnknownNeed',
        )
        assert.equal(unknown.value, 'missing')
        assert.equal(await board.task('c', t), null)

        assert.deepEqual(ids(await board.ready({ atMs: t })), ['a'])
        assert.equal(
          rejected(await board.claim('b', { leaseMs: 1_000, atMs: t }), 'Waiting').value,
          'a',
        )
        assert.deepEqual((await task(board, 'b', t)).needs, ['a'])

        const a = ok(await board.claim('a', { leaseMs: 1_000, atMs: t + 1 }))
        assert.equal(
          rejected(await board.claim('b', { leaseMs: 1_000, atMs: t + 2 }), 'Waiting').value,
          'a',
        )
        ok(await board.finish('a', a.claim, { done: 'A' }, t + 3))
        const done = await task(board, 'a', t + 3)
        assert.equal(done.phase, 'done')
        assert.equal(done.stage, 'done')
        assert.deepEqual(done.outcome, { done: 'A' })

        assert.deepEqual(ids(await board.ready({ atMs: t + 4 })), ['b'])
        ok(await board.claim('b', { leaseMs: 1_000, atMs: t + 4 }))
        await close()
      })

      test('a held task refuses claims, is taken over after its lease lapses, and the old holder is NotHolder', async () => {
        const path = file()
        const alice = await open(path, 'alice')
        const bob = await open(path, 'bob')
        const t = Date.now()
        ok(await alice.board.post({ id: 't', spec: 'contended' }, { atMs: t }))
        const first = ok(await alice.board.claim('t', { leaseMs: 1_000, atMs: t }))
        assert.equal(first.tookOver, false)
        assert.equal(first.untilMs, t + 1_000)
        assert.equal((await task(alice.board, 't', t + 1)).stage, 'held')
        assert.deepEqual(ids(await alice.board.held(t + 1)), ['t'])

        const held = rejected(await bob.board.claim('t', { leaseMs: 1_000, atMs: t + 500 }), 'Held')
        assert.equal(held.claim, first.claim)
        assert.equal(held.untilMs, t + 1_000)
        ok(await alice.board.renew('t', first.claim, 1_000, t + 600))
        rejected(await bob.board.claim('t', { leaseMs: 1_000, atMs: t + 1_200 }), 'Held')

        assert.equal((await task(bob.board, 't', t + 1_700)).stage, 'lapsed')
        assert.deepEqual(await bob.board.held(t + 1_700), [])
        const second = ok(await bob.board.claim('t', { leaseMs: 1_000, atMs: t + 1_700 }))
        assert.equal(second.tookOver, true)
        assert.notEqual(second.claim, first.claim)
        assert.equal((await task(bob.board, 't', t + 1_750)).worker, 'agent:bob')

        rejected(await alice.board.renew('t', first.claim, 1_000, t + 1_800), 'NotHolder')
        rejected(
          await alice.board.finish('t', first.claim, { done: 'late' }, t + 1_800),
          'NotHolder',
        )
        rejected(await alice.board.release('t', first.claim, 'late', t + 1_800), 'NotHolder')
        ok(await bob.board.finish('t', second.claim, { done: 'bob' }, t + 1_900))

        await alice.board.refresh()
        const finished = await task(alice.board, 't', t + 2_000)
        assert.equal(finished.phase, 'done')
        assert.equal(finished.attempts, 2)
        assert.deepEqual(finished.outcome, { done: 'bob' })
        rejected(await alice.board.claim('t', { leaseMs: 1_000, atMs: t + 2_000 }), 'Terminal')
        await alice.close()
        await bob.close()
      })

      test('release reopens a task for another claim', async () => {
        const { board, close } = await open(file(), 'alice')
        const t = Date.now()
        ok(await board.post({ id: 'r', spec: 'released' }, { atMs: t }))
        const first = ok(await board.claim('r', { leaseMs: 60_000, atMs: t }))
        ok(await board.release('r', first.claim, 'not mine', t + 1))
        assert.equal((await task(board, 'r', t + 2)).stage, 'open')
        const second = ok(await board.claim('r', { leaseMs: 60_000, atMs: t + 2 }))
        assert.equal(second.tookOver, false)
        rejected(await board.finish('r', first.claim, { done: 'stale' }, t + 3), 'NotHolder')
        await close()
      })

      test('a cap refuses claims past it with Full, counting only live leases', async () => {
        const { board, close } = await open(file(), 'alice')
        const t = Date.now()
        for (const id of ['x', 'y', 'z']) ok(await board.post({ id, spec: id }, { atMs: t }))
        const x = ok(await board.claim('x', { leaseMs: 1_000, cap: 2, atMs: t }))
        ok(await board.claim('y', { leaseMs: 1_000, cap: 2, atMs: t }))
        assert.equal(
          rejected(await board.claim('z', { leaseMs: 1_000, cap: 2, atMs: t + 1 }), 'Full').cap,
          2,
        )
        assert.deepEqual(ids(await board.held(t + 1)), ['x', 'y'])
        ok(await board.claim('z', { leaseMs: 1_000, cap: 3, atMs: t + 1 }))

        ok(await board.finish('x', x.claim, { done: 'x' }, t + 2))
        assert.deepEqual(ids(await board.held(t + 2)), ['y', 'z'])
        ok(await board.post({ id: 'w', spec: 'w' }, { atMs: t + 2 }))
        rejected(await board.claim('w', { leaseMs: 1_000, cap: 2, atMs: t + 3 }), 'Full')
        assert.deepEqual(await board.held(t + 5_000), [])
        ok(await board.claim('w', { leaseMs: 1_000, cap: 1, atMs: t + 5_000 }))
        await close()
      })

      test('a label cap counts only leases on tasks with that label', async () => {
        const { board, close } = await open(file(), 'alice')
        const t = Date.now()
        for (const id of ['a1', 'a2', 'b1', 'b2']) {
          ok(await board.post({ id, spec: id, labels: { lane: id[0] } }, { atMs: t }))
        }
        ok(await board.post({ id: 'u', spec: 'unlabelled' }, { atMs: t }))
        const a: Board.Cap = { max: 1, label: ['lane', 'a'] }
        const b: Board.Cap = { max: 1, label: ['lane', 'b'] }
        const a1 = ok(await board.claim('a1', { leaseMs: 1_000, cap: a, atMs: t }))
        assert.deepEqual(
          rejected(await board.claim('a2', { leaseMs: 1_000, cap: a, atMs: t + 1 }), 'Full').cap,
          a,
        )
        // lane=b has its own budget on the same partition.
        ok(await board.claim('b1', { leaseMs: 1_000, cap: b, atMs: t + 1 }))
        rejected(await board.claim('b2', { leaseMs: 1_000, cap: b, atMs: t + 2 }), 'Full')
        ok(
          await board.claim('u', {
            leaseMs: 1_000,
            cap: { max: 1, label: ['lane', 'u'] },
            atMs: t + 2,
          }),
        )
        // A partition cap still counts every live lease.
        assert.equal(
          rejected(await board.claim('a2', { leaseMs: 1_000, cap: 3, atMs: t + 3 }), 'Full').cap,
          3,
        )
        assert.deepEqual(ids(await board.held(t + 3)), ['a1', 'b1', 'u'])

        ok(await board.finish('a1', a1.claim, { done: 'a1' }, t + 4))
        ok(await board.claim('a2', { leaseMs: 1_000, cap: a, atMs: t + 5 }))
        rejected(await board.claim('b2', { leaseMs: 1_000, cap: b, atMs: t + 5 }), 'Full')
        await close()
      })

      test('a finished task keeps the worker that finished it', async () => {
        const path = file()
        const alice = await open(path, 'alice')
        const bob = await open(path, 'bob')
        const t = Date.now()
        for (const id of ['d', 'f', 'c']) ok(await alice.board.post({ id, spec: id }, { atMs: t }))
        const d = ok(await bob.board.claim('d', { leaseMs: 60_000, atMs: t + 1 }))
        ok(await bob.board.finish('d', d.claim, { done: 'ok' }, t + 2))
        const f = ok(await alice.board.claim('f', { leaseMs: 60_000, atMs: t + 3 }))
        ok(await alice.board.finish('f', f.claim, { failed: 'no' }, t + 4))
        ok(await alice.board.cancel('c', 'unneeded', t + 5))

        await alice.board.refresh()
        await bob.board.refresh()
        const done = await task(alice.board, 'd', t + 6)
        assert.equal(done.phase, 'done')
        assert.equal(done.worker, 'agent:bob')
        assert.equal(done.claim, undefined)
        const failed = await task(bob.board, 'f', t + 6)
        assert.equal(failed.phase, 'failed')
        assert.equal(failed.worker, 'agent:alice')
        assert.equal((await task(alice.board, 'c', t + 6)).worker, undefined)
        await alice.close()
        await bob.close()
      })

      test('a keyed post is idempotent and a conflicting repost is Exists', async () => {
        const { board, close } = await open(file(), 'alice')
        const t = Date.now()
        const first = ok(await board.post({ id: 'k', spec: 'keyed' }, { key: 'post-k', atMs: t }))
        const again = ok(
          await board.post({ id: 'k', spec: 'keyed' }, { key: 'post-k', atMs: t + 1 }),
        )
        assert.equal(again.seq, first.seq)
        assert.equal(again.event, first.event)
        // An identical repost is accepted without a second task; a different one is Exists.
        ok(await board.post({ id: 'k', spec: 'keyed' }, { atMs: t + 2 }))
        rejected(await board.post({ id: 'k', spec: 'changed' }, { atMs: t + 2 }), 'Exists')
        rejected(
          await board.post({ id: 'k', spec: 'changed' }, { key: 'other', atMs: t + 2 }),
          'Exists',
        )
        assert.equal((await task(board, 'k', t + 3)).spec, 'keyed')
        assert.equal((await board.tasks(t + 3)).filter((one) => one.id === 'k').length, 1)
        await close()
      })

      test('a failed attempt with retry terms comes back after doubling backoff, then fails for good', async () => {
        const { board, close } = await open(file(), 'alice')
        const t = Date.now()
        ok(
          await board.post(
            { id: 'r', spec: 'flaky', terms: { retry: { maxAttempts: 3, backoffMs: 1_000 } } },
            { atMs: t },
          ),
        )

        let claimed = ok(await board.claim('r', { leaseMs: 60_000, atMs: t }))
        ok(await board.finish('r', claimed.claim, { failed: 'boom 1' }, t + 10))
        let retrying = await task(board, 'r', t + 20)
        assert.equal(retrying.phase, 'open')
        assert.equal(retrying.stage, 'scheduled')
        assert.equal(retrying.attempts, 1)
        assert.equal(retrying.dueAtMs, t + 10 + 1_000)
        rejected(await board.claim('r', { leaseMs: 60_000, atMs: t + 500 }), 'Tasks')
        assert.deepEqual(await board.ready({ atMs: t + 500 }), [])
        assert.deepEqual(ids(await board.ready({ atMs: t + 1_010 })), ['r'])

        claimed = ok(await board.claim('r', { leaseMs: 60_000, atMs: t + 1_010 }))
        ok(await board.finish('r', claimed.claim, { failed: 'boom 2' }, t + 1_100))
        retrying = await task(board, 'r', t + 1_100)
        assert.equal(retrying.attempts, 2)
        assert.equal(retrying.dueAtMs, t + 1_100 + 2_000)
        rejected(await board.claim('r', { leaseMs: 60_000, atMs: t + 3_000 }), 'Tasks')

        claimed = ok(await board.claim('r', { leaseMs: 60_000, atMs: t + 3_100 }))
        ok(await board.finish('r', claimed.claim, { failed: 'boom 3' }, t + 3_200))
        const failed = await task(board, 'r', t + 3_200)
        assert.equal(failed.phase, 'failed')
        assert.equal(failed.stage, 'failed')
        assert.equal(failed.attempts, 3)
        assert.deepEqual(failed.outcome, { failed: 'boom 3' })
        rejected(await board.claim('r', { leaseMs: 60_000, atMs: t + 10_000 }), 'Terminal')
        await close()
      })

      test('a steer blocks finish until the holder heeds it', async () => {
        const path = file()
        const alice = await open(path, 'alice')
        const bob = await open(path, 'bob')
        const t = Date.now()
        ok(await alice.board.post({ id: 's', spec: 'steered' }, { atMs: t }))
        const claimed = ok(await alice.board.claim('s', { leaseMs: 60_000, atMs: t }))
        assert.deepEqual(await alice.board.steers('s', claimed.claim), [])

        const sent = ok(await bob.board.steer('s', 'use the blue one', { atMs: t + 1 }))
        const blocked = rejected(
          await alice.board.finish('s', claimed.claim, { done: 'red' }, t + 2),
          'Steer',
        )
        assert.ok(JSON.stringify(blocked).includes(String(sent.seq)), JSON.stringify(blocked))

        const steers = await alice.board.steers('s', claimed.claim)
        assert.equal(steers.length, 1)
        assert.equal(steers[0]!.body, 'use the blue one')
        assert.equal(steers[0]!.by, 'agent:bob')
        assert.equal(steers[0]!.seq, sent.seq)

        rejected(await bob.board.heed('s', 'not-a-claim', sent.seq, t + 3), 'NotHolder')
        ok(await alice.board.heed('s', claimed.claim, sent.seq, t + 3))
        const again = ok(await bob.board.steer('s', 'and make it bigger', { atMs: t + 4 }))
        rejected(await alice.board.finish('s', claimed.claim, { done: 'blue' }, t + 5), 'Steer')
        const later = await alice.board.steers('s', claimed.claim)
        assert.equal(later.at(-1)!.body, 'and make it bigger')
        ok(await alice.board.heed('s', claimed.claim, again.seq, t + 6))
        ok(await alice.board.finish('s', claimed.claim, { done: 'big blue' }, t + 7))
        rejected(await bob.board.steer('s', 'too late', { atMs: t + 8 }), 'Terminal')
        await alice.close()
        await bob.close()
      })

      test('work() delivers steers, heeds them, finishes, and throws LeaseLost when the lease goes', async () => {
        const path = file()
        const alice = await open(path, 'alice')
        const bob = await open(path, 'bob')
        ok(await alice.board.post({ id: 'w', spec: 'work' }))
        ok(await bob.board.steer('w', 'note this'))
        await alice.board.refresh()
        let seen: string[] = []
        const outcome = await alice.board.work('w', { leaseMs: 5_000 }, async (lease, steers) => {
          assert.equal(lease.id, 'w')
          assert.ok(lease.claim)
          assert.ok(lease.untilMs > Date.now())
          seen = steers.map((steer) => steer.body)
          return { done: 'worked' }
        })
        assert.deepEqual(outcome, { done: 'worked' })
        assert.deepEqual(seen, ['note this'])
        const worked = await task(alice.board, 'w')
        assert.equal(worked.phase, 'done')
        assert.deepEqual(worked.outcome, { done: 'worked' })

        ok(await alice.board.post({ id: 'h', spec: 'held elsewhere' }))
        ok(await bob.board.claim('h', { leaseMs: 60_000 }))
        await assert.rejects(
          alice.board.work('h', { leaseMs: 5_000 }, async () => ({ done: 'never' })),
          (error: unknown) =>
            error instanceof Board.LeaseLost &&
            error.task === 'h' &&
            error.rejection?.kind === 'Held',
        )

        ok(await alice.board.post({ id: 'v', spec: 'cancelled midway' }))
        let aborted = false
        await assert.rejects(
          alice.board.work('v', { leaseMs: 300 }, async (lease) => {
            ok(await bob.board.cancel('v', 'no longer needed'))
            await new Promise((resolve) => lease.signal.addEventListener('abort', resolve))
            aborted = true
            return { done: 'too late' }
          }),
          (error: unknown) => error instanceof Board.LeaseLost && error.task === 'v',
        )
        assert.equal(aborted, true)
        assert.equal((await task(bob.board, 'v')).phase, 'cancelled')
        await alice.close()
        await bob.close()
      })

      test('two processes drain one SQLite file without finishing any task twice', async () => {
        const path = file()
        const { board, close } = await open(path, 'parent')
        const count = 40
        const all = Array.from(
          { length: count },
          (_, index) => 'p' + String(index).padStart(2, '0'),
        )
        for (const id of all) ok(await board.post({ id, spec: 'shared ' + id }))

        type Drained = { finished: string[]; refusals: Record<string, number> }
        const [left, right] = await Promise.all([
          run<Drained>({ role: 'drain', path, actor: 'left' }),
          run<Drained>({ role: 'drain', path, actor: 'right' }),
        ])
        const both = [...left.finished, ...right.finished]
        assert.deepEqual([...both].sort(), all, JSON.stringify({ left, right }))
        assert.equal(new Set(both).size, count)

        await board.refresh()
        const tasks = await board.tasks()
        assert.equal(tasks.length, count)
        for (const one of tasks) {
          assert.equal(one.phase, 'done', one.id)
          assert.equal(one.attempts, 1, one.id)
          const by = left.finished.includes(one.id) ? 'left' : 'right'
          assert.deepEqual(one.outcome, { done: by }, one.id)
        }
        await close()
      })

      test('a lease abandoned by a crashed process is taken over by another', async () => {
        const path = file()
        const { board, close } = await open(path, 'survivor')
        const t = Date.now()
        ok(await board.post({ id: 'c', spec: 'crash' }, { atMs: t }))
        const abandoned = await run<Board.Claimed>({
          role: 'abandon',
          path,
          actor: 'crashed',
          task: 'c',
          atMs: t + 1,
        })
        assert.ok(abandoned.ok, JSON.stringify(abandoned))
        // The board never evaluates a write earlier than one already logged, so the child's claim
        // may be timed after its own open(); its untilMs says when the lease lapses.
        const until = (abandoned as Extract<Board.Claimed, { ok: true }>).untilMs
        assert.ok(until >= t + 1_001)
        await board.refresh()
        const held = rejected(await board.claim('c', { leaseMs: 1_000, atMs: until - 1 }), 'Held')
        assert.equal(held.claim, abandoned.ok ? abandoned.claim : undefined)
        assert.equal(held.untilMs, until)
        const taken = ok(await board.claim('c', { leaseMs: 1_000, atMs: until + 1 }))
        assert.equal(taken.tookOver, true)
        ok(await board.finish('c', taken.claim, { done: 'survived' }, until + 2))
        assert.deepEqual((await task(board, 'c', until + 3)).outcome, { done: 'survived' })
        await close()
      })

      test('a board reopened on its file resumes claims, keys, steers and event ids', async () => {
        const path = file()
        const t = Date.now()
        const seeded = await run<{ posts: number[]; claim: string }>({
          role: 'seed',
          path,
          actor: 'alice',
          atMs: t,
        })

        const { board, close } = await open(path, 'alice')
        const tasks = new Map((await board.tasks(t + 10)).map((one) => [one.id, one]))
        assert.deepEqual([...tasks.keys()].sort(), ['s1', 's2', 's3'])
        assert.equal(tasks.get('s1')!.stage, 'held')
        assert.equal(tasks.get('s1')!.claim, seeded.claim)
        assert.equal(tasks.get('s2')!.phase, 'done')
        assert.equal(tasks.get('s3')!.stage, 'open')

        const reposted = ok(
          await board.post({ id: 's1', spec: 'seeded s1' }, { key: 'seed-s1', atMs: t + 11 }),
        )
        assert.equal(reposted.seq, seeded.posts[0])

        ok(await board.post({ id: 's4', spec: 'after restart' }, { atMs: t + 12 }))
        ok(await board.post({ id: 's5', spec: 'after restart' }, { atMs: t + 12 }))
        assert.equal((await board.tasks(t + 13)).length, 5)

        ok(await board.renew('s1', seeded.claim, 600_000, t + 14))
        rejected(await board.finish('s1', seeded.claim, { done: 'resumed' }, t + 15), 'Steer')
        const steers = await board.steers('s1', seeded.claim)
        assert.deepEqual(
          steers.map((steer) => steer.body),
          ['seeded steer'],
        )
        ok(await board.heed('s1', seeded.claim, steers.at(-1)!.seq, t + 16))
        ok(await board.finish('s1', seeded.claim, { done: 'resumed' }, t + 17))
        await close()

        const reopened = await open(path, 'bob')
        const final = new Map((await reopened.board.tasks(t + 20)).map((one) => [one.id, one]))
        assert.deepEqual(final.get('s1')!.outcome, { done: 'resumed' })
        assert.equal(final.get('s4')!.stage, 'open')
        assert.equal(final.size, 5)
        await reopened.close()
      })
    },
  )
}
