import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { after, describe, test } from 'node:test'
import { fileURLToPath } from 'node:url'
import { Board, Sql } from 'nanocode-ts/node'

type Opened = { board: Board.Board; close(): Promise<void> }

const scope = 'pool'
const marker = 'NC_POOL_STARTED '

async function open(path: string, actor: string): Promise<Opened> {
  const sql = Sql.open(path)
  const board = await Board.open({ sql, scope, actor: 'agent:' + actor })
  const granted = await board.grant(
    { who: [{ kind: 'kind', id: 'agent' }], can: { '*': ['manage'] } },
    { key: 'grant-agents' },
  )
  if (!granted.ok) assert.equal(granted.rejection.kind, 'Access')
  return {
    board,
    async close() {
      await board.close()
      sql.close()
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

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

async function until(what: string, check: () => Promise<boolean> | boolean, ms = 10_000) {
  const deadline = Date.now() + ms
  while (!(await check())) {
    if (Date.now() > deadline) assert.fail('timed out waiting for ' + what)
    await sleep(5)
  }
}

async function phase(board: Board.Board, id: string) {
  await board.refresh()
  return (await board.task(id))?.phase
}

const aborted = (signal: AbortSignal) =>
  new Promise<void>((resolve) =>
    signal.aborted ? resolve() : signal.addEventListener('abort', () => resolve()),
  )

type Child = { path: string; task: string; leaseMs: number }

async function child(role: Child) {
  const { board } = await open(role.path, 'crashing')
  Board.pool(board, {
    concurrency: 1,
    leaseMs: role.leaseMs,
    pollMs: 10,
    async run(task) {
      process.stdout.write(marker + JSON.stringify({ id: task.id, at: Date.now() }) + '\n')
      return new Promise<Board.Outcome>(() => {})
    },
  })
}

function crashing(role: Child) {
  const args = [
    ...process.execArgv.filter((arg) => !arg.startsWith('--test')),
    fileURLToPath(import.meta.url),
  ]
  const proc = spawn(process.execPath, args, {
    env: { ...process.env, NC_POOL_CHILD: JSON.stringify(role) },
    stdio: ['ignore', 'pipe', 'pipe'],
  })
  let out = ''
  let err = ''
  const started = new Promise<{ id: string; at: number }>((resolve, reject) => {
    proc.stdout.on('data', (chunk) => {
      out += chunk
      const line = out.split('\n').find((one) => one.startsWith(marker))
      if (line) resolve(JSON.parse(line.slice(marker.length)))
    })
    proc.stderr.on('data', (chunk) => (err += chunk))
    proc.on('error', reject)
    proc.on('close', (code) => reject(new Error('child exited ' + code + '\n' + out + '\n' + err)))
  })
  const exited = new Promise<void>((resolve) => proc.on('close', () => resolve()))
  return { started, kill: () => proc.kill('SIGKILL'), exited }
}

if (process.env.NC_POOL_CHILD) {
  await child(JSON.parse(process.env.NC_POOL_CHILD) as Child)
} else {
  const root = mkdtempSync(join(tmpdir(), 'nc-board-pool-'))
  let files = 0
  const file = () => join(root, 'pool-' + files++ + '.sqlite')
  after(() => rmSync(root, { recursive: true, force: true }))

  describe('Board.pool', { timeout: 120_000 }, () => {
    test('two pools on one SQLite file drain 50 tasks with needs, finishing each once', async () => {
      const path = file()
      const left = await open(path, 'left')
      const right = await open(path, 'right')
      const count = 50
      const id = (index: number) => 't' + String(index).padStart(2, '0')
      const needs = (index: number) => [
        ...(index > 0 ? [id(Math.floor((index - 1) / 2))] : []),
        ...(index >= 3 && index % 5 === 0 ? [id(index - 3)] : []),
      ]
      for (let index = 0; index < count; index++)
        ok(await left.board.post({ id: id(index), spec: 'task ' + index, needs: needs(index) }))

      const runs = new Map<string, string[]>()
      const finished = new Set<string>()
      const early: string[] = []
      const work = (actor: string) => async (task: Board.Task) => {
        runs.set(task.id, [...(runs.get(task.id) ?? []), actor])
        for (const need of task.needs) if (!finished.has(need)) early.push(task.id + '<' + need)
        await sleep(Math.floor(Math.random() * 15))
        finished.add(task.id)
        return { done: actor }
      }
      const options = { concurrency: 4, leaseMs: 5_000, pollMs: 10, idleMs: 400 }
      const pools = [
        Board.pool(left.board, { ...options, run: work('left') }),
        Board.pool(right.board, { ...options, run: work('right') }),
      ]
      await Promise.all(pools.map((pool) => pool.idle()))
      const [a, b] = pools.map((pool) => pool.stats())

      assert.deepEqual(early, [])
      assert.equal(runs.size, count)
      for (const [task, by] of runs) assert.equal(by.length, 1, task + ' ran ' + by.join(', '))
      await right.board.refresh()
      const tasks = await right.board.tasks()
      assert.equal(tasks.length, count)
      for (const task of tasks) {
        assert.equal(task.phase, 'done', task.id)
        assert.deepEqual(task.outcome, { done: runs.get(task.id)![0] })
      }
      assert.equal(a!.done + b!.done, count, JSON.stringify({ a, b }))
      assert.equal(a!.lost + b!.lost, 0, JSON.stringify({ a, b }))
      assert.ok(a!.done > 0 && b!.done > 0, JSON.stringify({ a, b }))
      assert.equal(a!.state, 'retired')
      assert.equal(b!.state, 'retired')
      await left.close()
      await right.close()
    })

    test('a stopped pool releases its task and another pool takes it at once', async () => {
      const path = file()
      const alice = await open(path, 'alice')
      const bob = await open(path, 'bob')
      ok(await alice.board.post({ id: 'x', spec: 'stopped midway' }))
      let started = false
      let sawAbort = false
      const first = Board.pool(alice.board, {
        concurrency: 1,
        leaseMs: 60_000,
        pollMs: 10,
        async run(_task, lease) {
          started = true
          await aborted(lease.signal)
          sawAbort = true
          return { done: 'alice, too late' }
        },
      })
      await until('alice to start x', () => started)
      await first.stop()
      assert.equal(sawAbort, true)
      assert.equal(first.stats().released, 1)
      assert.equal(first.stats().state, 'stopped')
      assert.equal(await phase(bob.board, 'x'), 'open')

      const t = Date.now()
      let tookOver: boolean | undefined
      const second = Board.pool(bob.board, {
        concurrency: 1,
        leaseMs: 60_000,
        pollMs: 10,
        async run(_task, lease) {
          tookOver = lease.tookOver
          return { done: 'bob' }
        },
      })
      await until('bob to finish x', async () => (await phase(bob.board, 'x')) === 'done')
      assert.ok(Date.now() - t < 10_000)
      assert.equal(tookOver, false)
      assert.deepEqual((await bob.board.task('x'))?.outcome, { done: 'bob' })
      await second.stop()
      assert.equal(first.stats().done, 0)
      await alice.close()
      await bob.close()
    })

    test('a pool killed mid-task lets another take the task over once its lease lapses', async () => {
      const path = file()
      const bob = await open(path, 'bob')
      ok(await bob.board.post({ id: 'k', spec: 'killed midway' }))
      const leaseMs = 1_500
      const proc = crashing({ path, task: 'k', leaseMs })
      const started = await proc.started
      assert.equal(started.id, 'k')
      await sleep(leaseMs)
      proc.kill()
      await proc.exited
      const killedAt = Date.now()
      await bob.board.refresh()
      assert.equal((await bob.board.task('k'))?.stage, 'held')

      let claimedAt = 0
      let tookOver: boolean | undefined
      const pool = Board.pool(bob.board, {
        concurrency: 1,
        leaseMs: 5_000,
        pollMs: 20,
        async run(_task, lease) {
          claimedAt = Date.now()
          tookOver = lease.tookOver
          return { done: 'bob' }
        },
      })
      await until('bob to finish k', async () => (await phase(bob.board, 'k')) === 'done')
      await pool.stop()
      assert.ok(claimedAt - killedAt >= (leaseMs * 2) / 3 - 100, String(claimedAt - killedAt))
      assert.equal(tookOver, true)
      assert.deepEqual((await bob.board.task('k'))?.outcome, { done: 'bob' })
      await bob.close()
    })

    test('a steer reaches run through the lease, and an unheeded one is heeded before finish', async () => {
      const path = file()
      const alice = await open(path, 'alice')
      const bob = await open(path, 'bob')
      ok(await alice.board.post({ id: 's', spec: 'steered', labels: { kind: 'read' } }))
      ok(await alice.board.post({ id: 'u', spec: 'unheeded', labels: { kind: 'ignore' } }))
      let release = (_: unknown) => {}
      const gate = new Promise((resolve) => (release = resolve))
      const pool = Board.pool(alice.board, {
        concurrency: 2,
        leaseMs: 30_000,
        pollMs: 10,
        async run(task, lease) {
          if (task.labels.kind === 'ignore') {
            await gate
            return { done: 'never read a steer' }
          }
          for (;;) {
            const steers = await lease.steers()
            const last = steers.at(-1)
            if (last) {
              await lease.heed(last.seq)
              return { done: 'heard: ' + last.body }
            }
            await sleep(10)
          }
        },
      })
      await until('alice to claim both', async () => {
        await bob.board.refresh()
        return (await bob.board.held()).length === 2
      })
      ok(await bob.board.steer('s', 'use the blue one'))
      ok(await bob.board.steer('u', 'nobody reads this'))
      await until('s to finish', async () => (await phase(bob.board, 's')) === 'done')
      release(null)
      await until('u to finish', async () => (await phase(bob.board, 'u')) === 'done')
      assert.deepEqual((await bob.board.task('s'))?.outcome, { done: 'heard: use the blue one' })
      assert.deepEqual((await bob.board.task('u'))?.outcome, { done: 'never read a steer' })
      const stats = pool.stats()
      assert.equal(stats.delivered, 1, JSON.stringify(stats))
      assert.equal(stats.unheard, 1, JSON.stringify(stats))
      assert.ok(stats.retries >= 1, JSON.stringify(stats))
      assert.equal(stats.done, 2, JSON.stringify(stats))
      await pool.stop()
      await alice.close()
      await bob.close()
    })

    test('concurrency and label bound what a pool runs', async () => {
      const path = file()
      const { board, close } = await open(path, 'alice')
      for (let index = 0; index < 12; index++)
        ok(await board.post({ id: 'c' + index, spec: 'c', labels: { lane: 'mine' } }))
      ok(await board.post({ id: 'other', spec: 'not mine', labels: { lane: 'theirs' } }))
      let running = 0
      let most = 0
      const pool = Board.pool(board, {
        label: ['lane', 'mine'],
        concurrency: 3,
        leaseMs: 10_000,
        pollMs: 10,
        idleMs: 200,
        async run() {
          running++
          most = Math.max(most, running)
          await sleep(40)
          running--
          return { done: 'ok' }
        },
      })
      await pool.idle()
      assert.equal(most, 3)
      const stats = pool.stats()
      assert.equal(stats.done, 12, JSON.stringify(stats))
      assert.equal(stats.claimed, 12, JSON.stringify(stats))
      assert.equal(await phase(board, 'other'), 'open')
      await close()
    })

    test('a label cap bounds kind=agent tasks across two pools while untagged tasks flow', async () => {
      const path = file()
      const left = await open(path, 'left')
      const right = await open(path, 'right')
      for (let index = 0; index < 8; index++)
        ok(await left.board.post({ id: 'a' + index, spec: 'agent', labels: { kind: 'agent' } }))
      for (let index = 0; index < 8; index++)
        ok(await left.board.post({ id: 'u' + index, spec: 'untagged' }))

      let agents = 0
      let mostAgents = 0
      let alongside = 0
      const work = (actor: string) => async (task: Board.Task) => {
        const agent = task.labels.kind === 'agent'
        if (agent) mostAgents = Math.max(mostAgents, ++agents)
        else if (agents === 2) alongside++
        await sleep(agent ? 60 : 20)
        if (agent) agents--
        return { done: actor }
      }
      const options = {
        cap: { max: 2, label: ['kind', 'agent'] as [string, string] },
        concurrency: 3,
        leaseMs: 10_000,
        pollMs: 10,
        idleMs: 400,
      }
      const pools = [
        Board.pool(left.board, { ...options, run: work('left') }),
        Board.pool(right.board, { ...options, run: work('right') }),
      ]
      await Promise.all(pools.map((pool) => pool.idle()))
      const [a, b] = pools.map((pool) => pool.stats())

      assert.equal(mostAgents, 2)
      assert.ok(alongside > 0, 'no untagged task ran while two agent tasks were held')
      assert.equal(a!.done + b!.done, 16, JSON.stringify({ a, b }))
      assert.equal(a!.lost + b!.lost, 0, JSON.stringify({ a, b }))
      await right.board.refresh()
      for (const task of await right.board.tasks()) assert.equal(task.phase, 'done', task.id)
      await left.close()
      await right.close()
    })

    test('when a capped run ends, the freed slot goes to a lapsed task before an open one', async () => {
      const path = file()
      const alice = await open(path, 'alice')
      const ghost = await open(path, 'ghost')
      const agent = { kind: 'agent' }
      ok(await alice.board.post({ id: 'first', spec: 'holds the cap', labels: agent }))
      let release = (_: unknown) => {}
      const gate = new Promise((resolve) => (release = resolve))
      const order: string[] = []
      const tookOver = new Map<string, boolean>()
      const pool = Board.pool(alice.board, {
        cap: { max: 1, label: ['kind', 'agent'] },
        concurrency: 3,
        leaseMs: 10_000,
        pollMs: 10,
        async run(task, lease) {
          order.push(task.id)
          tookOver.set(task.id, lease.tookOver)
          if (task.id === 'first') await gate
          return { done: 'alice' }
        },
      })
      await until('alice to start first', () => order.length === 1)
      // Posted before the lapsed task, so the board lists it first.
      ok(await alice.board.post({ id: 'fresh', spec: 'open', labels: agent }))
      ok(await alice.board.post({ id: 'orphan', spec: 'its holder died', labels: agent }))
      await ghost.board.refresh()
      ok(await ghost.board.claim('orphan', { leaseMs: 50 }))
      await sleep(100)
      assert.equal(await phase(alice.board, 'fresh'), 'open')
      assert.equal((await alice.board.task('orphan'))?.stage, 'lapsed')
      release(null)
      await until('all three to finish', () => pool.stats().done === 3)
      assert.deepEqual(order, ['first', 'orphan', 'fresh'])
      assert.equal(tookOver.get('orphan'), true)
      await pool.stop()
      await alice.close()
      await ghost.close()
    })

    test('a lost lease finishes nothing, and a run that throws finishes failed', async () => {
      const path = file()
      const alice = await open(path, 'alice')
      const bob = await open(path, 'bob')
      ok(await alice.board.post({ id: 'gone', spec: 'cancelled midway' }))
      ok(await alice.board.post({ id: 'boom', spec: 'throws' }))
      let reason: unknown
      const pool = Board.pool(alice.board, {
        concurrency: 2,
        leaseMs: 300,
        pollMs: 10,
        async run(task, lease) {
          if (task.id === 'boom') throw new Error('it broke')
          ok(await bob.board.cancel('gone', 'no longer needed'))
          await aborted(lease.signal)
          reason = lease.signal.reason
          return { done: 'too late' }
        },
      })
      await until('both to settle', () => {
        const stats = pool.stats()
        return stats.lost === 1 && stats.failed === 1 && stats.running === 0
      })
      assert.ok(reason instanceof Board.LeaseLost)
      assert.equal((reason as Board.LeaseLost).task, 'gone')
      assert.equal(await phase(bob.board, 'gone'), 'cancelled')
      assert.deepEqual((await bob.board.task('boom'))?.outcome, { failed: 'it broke' })
      assert.equal(pool.stats().done, 0)
      await pool.stop()
      await alice.close()
      await bob.close()
    })

    test('a pool on a lazy board that forwards only async methods renews, finishes and opens once', async () => {
      const path = file()
      const alice = await open(path, 'alice')
      const methods = [
        'cancel',
        'charter',
        'checkpoint',
        'claim',
        'close',
        'finish',
        'grant',
        'group',
        'heed',
        'held',
        'opened',
        'post',
        'ready',
        'refresh',
        'release',
        'renew',
        'reply',
        'revoke',
        'steer',
        'steers',
        'submit',
        'task',
        'tasks',
        'work',
      ] as const satisfies readonly (keyof Board.Board)[]
      let opens = 0
      let renewals = 0
      let real: Promise<Opened> | undefined
      const lazily = () =>
        (real ??= (async () => {
          opens++
          return open(path, 'lazy')
        })())
      const calls = Object.fromEntries(
        methods.map((name) => [
          name,
          async (...args: never[]) => {
            if (name === 'renew') renewals++
            const board = (await lazily()).board
            return (board[name] as (...args: never[]) => unknown)(...args)
          },
        ]),
      )
      const lazy = { ...calls, actor: 'agent:lazy', scope } as never as Board.Board

      const leaseMs = 90
      const count = 4
      for (let index = 0; index < count; index++)
        ok(await alice.board.post({ id: 'p' + index, spec: 'renewed' }))
      ok(await alice.board.post({ id: 'r', spec: 'reopened' }))

      let started = false
      const pools: Board.Pool[] = []
      try {
        const first = Board.pool(lazy, {
          concurrency: count + 1,
          leaseMs,
          pollMs: 10,
          async run(task, lease) {
            if (task.id === 'r') {
              started = true
              await aborted(lease.signal)
              return { done: 'too late' }
            }
            await sleep(leaseMs * 4)
            return { done: 'first' }
          },
        })
        pools.push(first)
        await until('the first pool to finish its renewed tasks', async () => {
          await alice.board.refresh()
          const tasks = await alice.board.tasks()
          return started && tasks.filter((task) => task.phase === 'done').length === count
        })
        await first.stop()
        assert.equal(first.stats().released, 1, JSON.stringify(first.stats()))
        assert.equal(await phase(alice.board, 'r'), 'open')

        const second = Board.pool(lazy, {
          concurrency: 1,
          leaseMs,
          pollMs: 10,
          async run() {
            await sleep(leaseMs * 4)
            return { done: 'second' }
          },
        })
        pools.push(second)
        await until(
          'the second pool to finish r',
          async () => (await phase(alice.board, 'r')) === 'done',
        )
        await second.stop()

        const [a, b] = pools.map((pool) => pool.stats())
        assert.equal(a!.done + b!.done, count + 1, JSON.stringify({ a, b }))
        assert.equal(a!.lost + b!.lost, 0, JSON.stringify({ a, b }))
        assert.equal(a!.errors + b!.errors, 0, JSON.stringify({ a, b }))
        assert.ok(renewals >= count * 2, String(renewals))
        await alice.board.refresh()
        assert.deepEqual(await alice.board.held(), [])
        for (const task of await alice.board.tasks()) assert.equal(task.phase, 'done', task.id)
        assert.deepEqual((await alice.board.task('r'))?.outcome, { done: 'second' })
        assert.equal(opens, 1)
      } finally {
        await Promise.all(pools.map((pool) => pool.stop()))
        await (await lazily()).close()
        await alice.close()
      }
    })
  })
}
