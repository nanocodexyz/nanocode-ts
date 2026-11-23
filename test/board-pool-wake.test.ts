import assert from 'node:assert/strict'
import { spawn, type ChildProcess } from 'node:child_process'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createInterface } from 'node:readline'
import { after, describe, test } from 'node:test'
import { fileURLToPath } from 'node:url'
import { Board as NodeBoard, Sql as NodeSql } from 'nanocode-ts/node'
import { Board, Sql } from 'nanocode-ts/postgres'

const url = process.env.DATABASE_URL
const pollMs = 5_000
const count = 20
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

type Role = {
  url: string
  schema: string
  scope: string
  actor: string
  wake: boolean
  // Holds every task it claims under this lease until killed, instead of finishing it.
  hold?: number
}

async function child(role: Role) {
  const emit = (line: object) => process.stdout.write(JSON.stringify(line) + '\n')
  const sql = Sql.open(role.url, { schema: role.schema, max: 2 })
  const board = await Board.open({
    sql,
    scope: role.scope,
    actor: 'agent:' + role.actor,
    charter: false,
  })
  let ready = false
  const announce = () => {
    if (ready) return
    ready = true
    emit({ e: 'ready' })
  }
  const pool = Board.pool(board, {
    concurrency: 2,
    leaseMs: role.hold ?? 30_000,
    pollMs,
    wake: role.wake
      ? (notify) =>
          // The first notify is the LISTEN taking effect, so the process is ready for wakes.
          Sql.wake(sql)(() => {
            announce()
            notify()
          })
      : undefined,
    async run(task) {
      if (role.hold !== undefined) {
        emit({ e: 'start', task: task.id })
        await new Promise(() => {})
      }
      await sleep(5)
      return { done: role.actor }
    },
  })
  if (!role.wake) {
    while (pool.stats().polls === 0) await sleep(5)
    await sleep(100)
    announce()
  }
  process.on('SIGTERM', () => {
    void pool
      .stop()
      .then(() => board.close())
      .then(() => sql.close())
      .then(() => process.exit(0))
  })
}

if (process.env.NC_WAKE_CHILD) {
  await child(JSON.parse(process.env.NC_WAKE_CHILD) as Role)
} else {
  describe('Board.pool wake', () => {
    test('a notify polls at once; the pool unsubscribes when it stops', async () => {
      const root = mkdtempSync(join(tmpdir(), 'nc-board-wake-'))
      after(() => rmSync(root, { recursive: true, force: true }))
      const sql = NodeSql.open(join(root, 'wake.sqlite'))
      const board = await NodeBoard.open({ sql, scope: 'wake', actor: 'agent:solo' })
      let notify: (() => void) | undefined
      let unsubscribed = 0
      const done: string[] = []
      const pool = NodeBoard.pool(board, {
        concurrency: 2,
        leaseMs: 30_000,
        pollMs,
        wake(fire) {
          notify = fire
          return () => unsubscribed++
        },
        async run(task) {
          done.push(task.id)
          return { done: 'solo' }
        },
      })
      while (pool.stats().polls === 0) await sleep(5)
      await sleep(50)
      const polls = pool.stats().polls
      assert.ok((await board.post({ id: 'a', spec: 'woken' })).ok)
      const started = Date.now()
      notify!()
      while (done.length === 0) {
        assert.ok(Date.now() - started < 2_000, 'notify did not poll')
        await sleep(5)
      }
      assert.ok(pool.stats().polls > polls)
      await pool.stop()
      assert.equal(unsubscribed, 1)
      notify!()
      await board.close()
      sql.close()
    })
  })

  if (!url) {
    test('Board.pool wake over Postgres', { skip: 'DATABASE_URL is not set' }, () => {})
  } else {
    const schemas: string[] = []
    const children = new Set<ChildProcess>()
    after(async () => {
      for (const proc of children) proc.kill('SIGKILL')
      const admin = Sql.open(url)
      try {
        for (const schema of schemas)
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

    const start = (role: Role) => {
      const args = [
        ...process.execArgv.filter((arg) => !arg.startsWith('--test')),
        fileURLToPath(import.meta.url),
      ]
      const proc = spawn(process.execPath, args, {
        env: { ...process.env, NC_WAKE_CHILD: JSON.stringify(role) },
        stdio: ['ignore', 'pipe', 'pipe'],
      })
      children.add(proc)
      let err = ''
      proc.stderr!.on('data', (chunk: Buffer) => (err += chunk.toString()))
      let held = () => {}
      const holding = new Promise<void>((resolve) => (held = resolve))
      const ready = new Promise<void>((resolve, reject) => {
        createInterface({ input: proc.stdout! }).on('line', (line) => {
          if (line.includes('"ready"')) resolve()
          if (line.includes('"start"')) held()
        })
        proc.on('close', (code) => reject(new Error('child exited ' + code + '\n' + err)))
      })
      const exited = new Promise<void>((resolve) => proc.on('close', () => resolve()))
      return {
        ready,
        holding,
        kill() {
          proc.kill('SIGKILL')
          children.delete(proc)
        },
        async stop() {
          proc.kill('SIGTERM')
          await Promise.race([exited, sleep(5_000)])
          proc.kill('SIGKILL')
          children.delete(proc)
        },
      }
    }

    // Two processes, each a pool with pollMs 5000, drain 20 tasks (10 roots and 10 that need one).
    const drain = async (wake: boolean) => {
      const schema = 'nc_board_wake_' + (wake ? 'on_' : 'off_') + process.pid
      schemas.push(schema)
      const scope = 'wake'
      const sql = Sql.open(url, { schema })
      const board = await Board.open({ sql, scope, actor: 'agent:parent' })
      const granted = await board.grant(
        { who: [{ kind: 'kind', id: 'agent' }], can: { '*': ['manage'] } },
        { key: 'grant-agents' },
      )
      assert.ok(granted.ok, JSON.stringify(granted))
      const workers = ['left', 'right'].map((actor) => start({ url, schema, scope, actor, wake }))
      await Promise.all(workers.map((worker) => worker.ready))
      const ids = Array.from({ length: count }, (_, index) => 't' + index)
      const started = Date.now()
      for (const [index, id] of ids.entries()) {
        const needs = index < count / 2 ? [] : ['t' + (index - count / 2)]
        assert.ok((await board.post({ id, spec: 'task ' + index, needs })).ok)
      }
      let finished = 0
      while (finished < count) {
        assert.ok(Date.now() - started < 3 * pollMs, 'drain timed out at ' + finished)
        await sleep(10)
        await board.refresh()
        finished = 0
        for (const id of ids) if ((await board.task(id))?.phase === 'done') finished++
      }
      const elapsed = Date.now() - started
      await Promise.all(workers.map((worker) => worker.stop()))
      await board.close()
      await sql.close()
      return elapsed
    }

    describe('Board.pool wake over Postgres', { timeout: 120_000 }, () => {
      test('with wake two processes drain 20 tasks well under pollMs', async () => {
        const elapsed = await drain(true)
        process.stdout.write('# wake drain ' + elapsed + ' ms\n')
        assert.ok(elapsed < pollMs / 5, 'drained in ' + elapsed + ' ms')
      })

      test('without wake the same drain waits for the next poll', async () => {
        const elapsed = await drain(false)
        process.stdout.write('# poll drain ' + elapsed + ' ms\n')
        assert.ok(elapsed > pollMs / 2, 'drained in ' + elapsed + ' ms')
      })

      test("with wake a killed holder's task is taken over about when its lease ends", async () => {
        const schema = 'nc_board_wake_lapse_' + process.pid
        schemas.push(schema)
        const scope = 'lapse'
        const leaseMs = 1_000
        const sql = Sql.open(url, { schema, max: 2 })
        const board = await Board.open({ sql, scope, actor: 'agent:parent' })
        const granted = await board.grant(
          { who: [{ kind: 'kind', id: 'agent' }], can: { '*': ['manage'] } },
          { key: 'grant-agents' },
        )
        assert.ok(granted.ok, JSON.stringify(granted))
        assert.ok((await board.post({ id: 'orphan', spec: 'its holder dies' })).ok)
        const holder = start({ url, schema, scope, actor: 'holder', wake: true, hold: leaseMs })
        // ready rejects when the kill below ends the holder.
        holder.ready.catch(() => {})
        await holder.holding
        let takenAt: number | undefined
        let tookOver = false
        // pollMs 5000 and no notification for a lapse: only the lease timer can be this quick.
        const pool = Board.pool(board, {
          concurrency: 2,
          leaseMs,
          pollMs,
          wake: Sql.wake(sql),
          async run(_, lease) {
            takenAt = Date.now()
            tookOver = lease.tookOver
            return { done: 'parent' }
          },
        })
        // Let the holder renew a few times, so the pool re-arms its timer on later leases.
        await sleep(leaseMs * 1.5)
        assert.equal(takenAt, undefined)
        const killedAt = Date.now()
        holder.kill()
        while (takenAt === undefined) {
          assert.ok(Date.now() - killedAt < pollMs, 'not taken over before pollMs')
          await sleep(5)
        }
        const elapsed = takenAt - killedAt
        process.stdout.write('# lapse takeover ' + elapsed + ' ms after the kill\n')
        assert.ok(tookOver)
        assert.ok(elapsed < leaseMs + 100, 'taken over ' + elapsed + ' ms after the kill')
        await pool.stop()
        await board.close()
        await sql.close()
      })

      test("a pool's own writes wake only the pools own names", async () => {
        const schema = 'nc_board_wake_own_' + process.pid
        schemas.push(schema)
        const near = Sql.open(url, { schema })
        const far = Sql.open(url, { schema })
        const board = await Board.open({ sql: near, scope: 'own', actor: 'agent:parent' })
        const hits = { self: 0, sibling: 0, remote: 0 }
        const subscribe = (sql: Sql.Host, name: keyof typeof hits) =>
          Sql.wake(sql, { debounceMs: 20 })(() => hits[name]++)
        const self = subscribe(near, 'self')
        const sibling = subscribe(near, 'sibling')
        const remote = subscribe(far, 'remote')
        // Each LISTEN taking effect notifies once.
        while (hits.self === 0 || hits.sibling === 0 || hits.remote === 0) await sleep(5)
        const settle = async () => {
          await sleep(300)
          const seen = { ...hits }
          hits.self = hits.sibling = hits.remote = 0
          return seen
        }
        await settle()
        const post = (id: string) =>
          board.post({ id, spec: id }).then((posted) => assert.ok(posted.ok))

        await self.own('none', () => post('claim-like'))
        assert.deepEqual(await settle(), { self: 0, sibling: 0, remote: 0 })

        // The writer's own host skips its notification, so self stays quiet.
        await self.own('others', () => post('finish-like'))
        const others = await settle()
        assert.equal(others.self, 0)
        assert.ok(others.sibling > 0 && others.remote > 0, JSON.stringify(others))

        await post('post')
        const all = await settle()
        assert.ok(all.self > 0 && all.sibling > 0 && all.remote > 0, JSON.stringify(all))

        for (const stop of [self, sibling, remote]) stop()
        await board.close()
        await near.close()
        await far.close()
      })

      test("a pool woken by another's finish still wakes that pool with its own writes", async () => {
        const schema = 'nc_board_wake_carry_' + process.pid
        schemas.push(schema)
        const sql = Sql.open(url, { schema })
        const open = (actor: string) =>
          Board.open({ sql, scope: 'carry', actor, charter: actor === 'agent:parent' })
        const parent = await open('agent:parent')
        const granted = await parent.grant(
          { who: [{ kind: 'kind', id: 'agent' }], can: { '*': ['manage'] } },
          { key: 'grant-agents' },
        )
        assert.ok(granted.ok, JSON.stringify(granted))
        const [left, right] = await Promise.all([open('agent:left'), open('agent:right')])
        const done = new Map<string, number>()
        const pool = (board: Board.Board, who: string) =>
          Board.pool(board, {
            label: ['who', who],
            concurrency: 1,
            leaseMs: 30_000,
            pollMs,
            wake: Sql.wake(sql),
            async run(task) {
              // right's run posts work for left from the tick left's finish woke.
              if (task.id === 'second') {
                const back = { id: 'third', spec: 'back', labels: { who: 'left' } }
                const posted = await board.post(back)
                assert.ok(posted.ok, JSON.stringify(posted))
              }
              done.set(task.id, Date.now())
              return { done: who }
            },
          })
        const pools = [pool(left, 'left'), pool(right, 'right')]
        // Each pool's first poll finds nothing, so only wakes can bring the work below in time.
        while (pools.some((each) => each.stats().polls === 0)) await sleep(5)
        await sleep(50)
        const started = Date.now()
        assert.ok((await parent.post({ id: 'first', spec: 'go', labels: { who: 'left' } })).ok)
        const second = { id: 'second', spec: 'then', labels: { who: 'right' }, needs: ['first'] }
        assert.ok((await parent.post(second)).ok)
        while (!done.has('third')) {
          const waited = 'third waited for a poll: ' + JSON.stringify([...done])
          assert.ok(Date.now() - started < pollMs / 2, waited)
          await sleep(5)
        }
        process.stdout.write('# carried wake ' + (done.get('third')! - started) + ' ms\n')
        await Promise.all(pools.map((each) => each.stop()))
        await Promise.all([parent, left, right].map((board) => board.close()))
        await sql.close()
      })
    })
  }
}
