import assert from 'node:assert/strict'
import * as childProcess from 'node:child_process'
import * as crypto from 'node:crypto'
import * as fs from 'node:fs'
import * as os from 'node:os'
import * as path from 'node:path'
import { test } from 'node:test'
import { fileURLToPath } from 'node:url'
import * as util from 'node:util'
import { Board } from 'nanocode-ts/node'
import { Sql as Postgres } from 'nanocode-ts/postgres'
import { generator, lanes, openSql, partitionOf, type Placed, placer } from './board-fuzz.shared.ts'

type Entry = {
  body?: string
  cap?: Board.Cap | null
  child: number
  claim?: string
  event?: string
  generation: number
  id?: string
  key?: string | null
  kind: string
  lane?: string
  labels?: Record<string, string>
  leaseMs?: number
  message?: string
  needs?: string[]
  ok?: boolean
  outcome?: Board.Outcome
  parent?: string | null
  phase?: string
  pool?: boolean
  rejection?: string
  rejectionCap?: unknown
  seq?: number
  stats?: { errors: number; lastError?: string }
  t0?: number
  t1?: number
  tag?: string
  tookOver?: boolean
  untilMs?: number
  upto?: number
  worker?: string | null
}

type Run = {
  child: number
  exit: Promise<{ code: number | null; signal: NodeJS.Signals | null; stderr: string }>
  generation: number
  killed: boolean
  proc: childProcess.ChildProcess
}

type History = {
  entries: readonly Entry[]
  tasks: readonly Board.Task[]
  steers: ReadonlyMap<string, readonly Board.Steer[]>
  unjournaled: ReadonlySet<string>
  killedRuns: ReadonlySet<string>
  partitions: number
}

// Four processes (board-fuzz.child.ts) write one SQLite board at random for FUZZ_MS (18 s by
// default; set it higher for a long run), some SIGKILLed and restarted, then a fresh handle checks
// the invariants against their journals. FUZZ_SEED fixes the random choices (timing still varies);
// a failure prints it. With NC_BOARD_POSTGRES set to a database URL (board-fuzz-postgres.test.ts
// sets it from DATABASE_URL) the board is one Postgres schema per seed instead, each process on its
// own connection pool, and the schema is dropped once the seed passes.
const seed = process.env.FUZZ_SEED ? Number(process.env.FUZZ_SEED) : crypto.randomInt(2 ** 31)
const runMs = process.env.FUZZ_MS ? Number(process.env.FUZZ_MS) : 18_000
const partitions = process.env.FUZZ_PARTITIONS
  ? Number(process.env.FUZZ_PARTITIONS)
  : seed % 4 === 0
    ? 1
    : 4
const children = 4
const postgres = process.env.NC_BOARD_POSTGRES
const scope = 'fuzz'
const script = fileURLToPath(new URL('./board-fuzz.child.ts', import.meta.url))

function sleep(ms: number) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

function group<T>(items: readonly T[], by: (item: T) => string | undefined) {
  const groups = new Map<string, T[]>()
  for (const item of items) {
    const key = by(item)
    if (key === undefined) continue
    groups.set(key, [...(groups.get(key) ?? []), item])
  }
  return groups
}

function start(
  dir: string,
  file: string,
  child: number,
  generation: number,
  untilMs: number,
  pool: boolean,
): Run {
  const config = {
    child,
    generation,
    journal: path.join(dir, 'journal-' + child + '-' + generation + '.jsonl'),
    partitions,
    path: file,
    pool,
    scope,
    seed: (seed * 31 + child) * 31 + generation,
    untilMs,
  }
  const proc = childProcess.spawn(
    process.execPath,
    [
      ...process.execArgv.filter((arg) => !arg.startsWith('--test')),
      script,
      JSON.stringify(config),
    ],
    { stdio: ['ignore', 'ignore', 'pipe'] },
  )
  let stderr = ''
  proc.stderr!.on('data', (chunk) => (stderr += chunk))
  const exit = new Promise<{ code: number | null; signal: NodeJS.Signals | null; stderr: string }>(
    (resolve) => proc.on('close', (code, signal) => resolve({ code, signal, stderr })),
  )
  return { child, exit, generation, killed: false, proc }
}

function journals(dir: string) {
  return fs
    .readdirSync(dir)
    .filter((name) => name.startsWith('journal-'))
    .flatMap((name) =>
      fs
        .readFileSync(path.join(dir, name), 'utf8')
        .split('\n')
        .filter((line) => line !== '')
        .map((line) => JSON.parse(line) as Entry),
    )
}

function finisher(outcome: Board.Outcome | undefined) {
  const detail = outcome && ('done' in outcome ? outcome.done : outcome.failed)
  const match = typeof detail === 'string' ? /^c(\d+)g\d+([op])\d+$/.exec(detail) : null
  return match ? 'agent:c' + match[1] + (match[2] === 'p' ? 'p' : '') : undefined
}

function check({ entries, tasks, steers, unjournaled, killedRuns, partitions }: History) {
  const violations: string[] = []
  const byId = new Map(tasks.map((task) => [task.id, task]))
  const ok = entries.filter((entry) => entry.ok === true)
  const of = (kind: string) => ok.filter((entry) => entry.kind === kind)
  const finishes = group(of('finish'), (entry) => entry.id)
  const show = (value: unknown) => JSON.stringify(value)
  const shape = (task: Board.Task): Placed => ({
    id: task.id,
    labels: task.labels,
    parent: task.parent ?? null,
  })
  const place = placer(partitions, (id) => {
    const task = byId.get(id)
    return task && shape(task)
  })
  const where = (entry: Entry) => {
    const task = byId.get(entry.id!)
    return task
      ? place(shape(task))
      : place({ id: entry.id!, labels: entry.labels ?? {}, parent: entry.parent ?? null })
  }
  const journaled = (entry: Entry) => !killedRuns.has(entry.child + '/' + entry.generation)

  for (const entry of entries) {
    if (entry.kind === 'error') violations.push('child ' + entry.child + ' threw: ' + entry.message)
    if (entry.rejection === 'Unknown') violations.push('Unknown rejection: ' + show(entry))
    if (entry.kind === 'pool-stats' && entry.stats!.errors > 0)
      violations.push('pool of child ' + entry.child + ' hit errors: ' + show(entry.stats))
  }

  for (const [seq, same] of group(ok, (entry) => where(entry) + '/' + entry.seq))
    if (
      new Set(same.map((entry) => entry.event)).size > 1 ||
      (same.length > 1 && new Set(same.map((entry) => entry.kind + ' ' + entry.key)).size > 1) ||
      (same.length > 1 && same.some((entry) => !entry.key))
    )
      violations.push('seq ' + seq + ' went to several writes: ' + show(same))

  for (const posted of of('post'))
    if (!posted.key && !byId.has(posted.id!))
      violations.push('post ' + posted.id + ' was acknowledged but made no task: ' + show(posted))

  for (const posted of entries.filter((entry) => entry.kind === 'post' && !entry.key)) {
    const home = where(posted)
    const away = (posted.needs ?? []).filter((need) => {
      const task = byId.get(need)
      return !task || place(shape(task)) !== home
    })
    if (posted.ok && away.length > 0)
      violations.push(
        'post ' +
          posted.id +
          ' in partition ' +
          home +
          ' took needs ' +
          show(away) +
          ' from others',
      )
    if (!posted.ok && posted.rejection === 'UnknownNeed' && away.length === 0)
      violations.push('post ' + posted.id + ' was refused UnknownNeed with every need beside it')
  }

  for (const [id, done] of finishes) {
    const task = byId.get(id)
    if (done.length > 1) violations.push(id + ' finished more than once: ' + show(done))
    const outcome = done[0]!.outcome!
    const phase = 'done' in outcome ? 'done' : 'failed'
    if (task?.phase !== phase || show(task.outcome) !== show(outcome))
      violations.push(id + ' finished ' + show(outcome) + ' but is now ' + show(task))
  }

  for (const entry of entries.filter(
    (entry) => entry.kind === 'seen' || entry.kind === 'watched',
  )) {
    const task = byId.get(entry.id!)
    if (
      task?.phase !== entry.phase ||
      show(task.outcome) !== show(entry.outcome) ||
      (entry.worker !== undefined && task.worker !== entry.worker)
    )
      violations.push(entry.id + ' was seen ' + show(entry) + ' but is now ' + show(task))
  }

  for (const task of tasks) {
    const unmet = task.needs.filter((need) => byId.get(need)?.phase !== 'done')
    if (unmet.length > 0 && task.attempts > 0)
      violations.push(
        task.id + ' was claimed with needs ' + show(unmet) + ' not done: ' + show(task),
      )
    if ((task.phase === 'done' || task.phase === 'failed') && task.attempts === 0)
      violations.push(task.id + ' finished without a claim: ' + show(task))
    if (
      (task.phase === 'done' || task.phase === 'failed') &&
      (steers.get(task.id) ?? []).length > 0
    )
      violations.push(task.id + ' finished with unheeded steers: ' + show(steers.get(task.id)))
    if (task.phase === 'done' || task.phase === 'failed') {
      const expected = finisher(task.outcome)
      if (expected ? task.worker !== expected : !task.worker?.startsWith('agent:c'))
        violations.push(
          task.id +
            ' was finished by ' +
            (expected ?? 'a pool') +
            ' but names ' +
            show(task.worker),
        )
    }
  }

  for (const claim of of('claim')) {
    for (const need of byId.get(claim.id!)?.needs ?? []) {
      const finished = finishes.get(need)?.[0]
      if (finished && finished.seq! > claim.seq!)
        violations.push(
          claim.id +
            ' was claimed at ' +
            claim.seq +
            ' before ' +
            need +
            ' finished at ' +
            finished.seq,
        )
    }
  }

  const leaseKey = (entry: Entry) => entry.id + ' ' + entry.claim
  const cancelled = (id: string) => byId.get(id)?.phase === 'cancelled'
  const renews = group(of('renew'), leaseKey)
  const ends = group(
    ok.filter((entry) => entry.kind === 'finish' || entry.kind === 'release'),
    leaseKey,
  )
  type Lease = { claim: Entry; ended: number; renews: Entry[] }
  const bySeq = (a: Entry, b: Entry) => a.seq! - b.seq!
  const certainUntil = (lease: Lease, seq: number) => {
    const renew = lease.renews.findLast((renew) => renew.seq! < seq)
    return renew ? renew.t0! + renew.leaseMs! : lease.claim.untilMs!
  }
  for (const [partition, claims] of group(of('claim'), (entry) => String(where(entry)))) {
    let active: Lease[] = []
    for (const claim of claims.toSorted((a, b) => a.seq! - b.seq!)) {
      const atMs = claim.untilMs! - claim.leaseMs!
      active = active.filter(
        (lease) => lease.ended > claim.seq! && certainUntil(lease, Infinity) > atMs - 60_000,
      )
      const holding = active.filter((lease) => certainUntil(lease, claim.seq!) > atMs)
      const same = holding.filter((lease) => lease.claim.id === claim.id)
      if (same.length > 0)
        violations.push(
          claim.id +
            ' was claimed at ' +
            claim.seq +
            ' while held: ' +
            show(same.map((lease) => lease.claim)),
        )
      const cap = claim.cap
      if (cap !== undefined && cap !== null) {
        const max = typeof cap === 'number' ? cap : cap.max
        const counted = holding.filter(
          (lease) =>
            lease.claim.id !== claim.id &&
            (typeof cap === 'number' ||
              byId.get(lease.claim.id!)?.labels[cap.label[0]] === cap.label[1]),
        )
        if (counted.length >= max)
          violations.push(
            claim.id +
              ' was claimed at ' +
              partition +
              '/' +
              claim.seq +
              ' under cap ' +
              show(cap) +
              ' with ' +
              counted.length +
              ' held: ' +
              show(counted.map((lease) => lease.claim.id)),
          )
      }
      const lane = byId.get(claim.id!)?.labels.lane
      if (lane === 'capA' || lane === 'capB') {
        const sharing = holding.filter(
          (lease) => lease.claim.id !== claim.id && byId.get(lease.claim.id!)?.labels.lane === lane,
        )
        if (sharing.length >= lanes[lane])
          violations.push(
            'lane ' +
              lane +
              ' held ' +
              (sharing.length + 1) +
              ' > ' +
              lanes[lane] +
              ' at ' +
              partition +
              '/' +
              claim.seq +
              ' by ' +
              show(
                [...sharing.map((lease) => lease.claim), claim].map(
                  (entry) => (entry.pool ? 'pool:' : 'raw:') + entry.id,
                ),
              ),
          )
      }
      if (journaled(claim) && !cancelled(claim.id!))
        active.push({
          claim,
          ended: Math.min(...(ends.get(leaseKey(claim)) ?? []).map((entry) => entry.seq!)),
          renews: (renews.get(leaseKey(claim)) ?? []).toSorted(bySeq),
        })
    }
  }
  for (const refused of entries.filter(
    (entry) => entry.kind === 'claim' && entry.rejection === 'Full',
  ))
    if (!util.isDeepStrictEqual(refused.rejectionCap, refused.cap))
      violations.push('claim refused Full names another cap: ' + show(refused))

  for (const run of group(entries, (entry) => entry.child + '/' + entry.generation).values()) {
    const until = new Map<string, number>()
    for (const entry of run) {
      if (entry.kind === 'claim' && entry.ok && !cancelled(entry.id!))
        until.set(leaseKey(entry), entry.untilMs!)
      const untilMs = entry.claim === undefined ? undefined : until.get(leaseKey(entry))
      if (untilMs === undefined || entry.kind === 'claim') continue
      if (entry.ok && (entry.kind === 'finish' || entry.kind === 'release'))
        until.delete(leaseKey(entry))
      else if (entry.ok && entry.kind === 'renew')
        until.set(leaseKey(entry), entry.t0! + entry.leaseMs!)
      else if (
        !entry.ok &&
        (entry.rejection === 'NotHolder' || entry.rejection === 'Terminal') &&
        entry.t1! < untilMs
      )
        violations.push(
          entry.id +
            ' ' +
            entry.kind +
            ' was refused ' +
            entry.rejection +
            ' at ' +
            entry.t1 +
            ' though its lease ran to ' +
            untilMs,
        )
    }
  }

  for (const [key, posts] of group(of('post'), (entry) => entry.key ?? undefined)) {
    if (new Set(posts.map((entry) => entry.seq)).size > 1)
      violations.push('keyed post ' + key + ' landed at several seqs: ' + show(posts))
  }
  for (const [key, made] of group(tasks, (task) => task.labels.key))
    if (made.length > 1)
      violations.push('keyed post ' + key + ' made ' + show(made.map((task) => task.id)))

  for (const [body, sent] of group(of('steer'), (entry) => where(entry) + '/' + entry.body))
    if (new Set(sent.map((entry) => entry.seq)).size > 1)
      violations.push('steer ' + body + ' landed at several seqs: ' + show(sent))

  const heeded = new Map<string, number>()
  for (const [id, heeds] of group(of('heed'), (entry) => entry.id)) {
    const ordered = heeds.toSorted((a, b) => a.seq! - b.seq!)
    for (let i = 1; i < ordered.length; i++)
      if (ordered[i]!.upto! <= ordered[i - 1]!.upto!)
        violations.push(id + ' heed ran backwards: ' + show([ordered[i - 1], ordered[i]]))
    heeded.set(id, Math.max(...ordered.map((entry) => entry.upto!)))
  }

  const bodies = new Map<string, number>()
  for (const [id, list] of steers) {
    for (let i = 0; i < list.length; i++) {
      const steer = list[i]!
      if (i > 0 && steer.seq <= list[i - 1]!.seq)
        violations.push(id + ' steers out of order: ' + show(list))
      if (steer.seq <= (heeded.get(id) ?? 0))
        violations.push(
          id + ' still holds steer ' + steer.seq + ' after a heed up to ' + heeded.get(id),
        )
      const body = where({ id } as Entry) + '/' + steer.body
      if (bodies.has(body))
        violations.push(
          'steer ' + body + ' is pending twice: ' + bodies.get(body) + ' and ' + steer.seq,
        )
      bodies.set(body, steer.seq)
    }
  }
  for (const sent of of('steer')) {
    const task = byId.get(sent.id!)
    if (sent.key || unjournaled.has(sent.id!) || sent.seq! <= (heeded.get(sent.id!) ?? 0)) continue
    if (task?.phase !== 'open' && task?.phase !== 'claimed') continue
    if (
      !(steers.get(sent.id!) ?? []).some(
        (steer) => steer.seq === sent.seq && steer.body === sent.body,
      )
    )
      violations.push(sent.id + ' lost steer ' + show(sent) + ': ' + show(steers.get(sent.id!)))
  }

  return violations
}

test('the fuzz checker flags broken histories', () => {
  const task = (id: string, fields: Partial<Board.Task>) =>
    ({
      attempts: 1,
      id,
      labels: {},
      needs: [],
      parent: null,
      phase: 'done',
      outcome: { done: 'x' },
      worker: 'agent:c0',
      ...fields,
    }) as Board.Task
  const entry = (fields: Partial<Entry>): Entry => ({
    child: 0,
    generation: 0,
    ok: true,
    kind: 'post',
    ...fields,
  })
  const open = { attempts: 0, phase: 'open', outcome: undefined, worker: undefined } as const
  const tasks = [
    task('a', { outcome: { done: 'second' } }),
    task('b', { needs: ['c'], phase: 'claimed', outcome: undefined }),
    task('c', open),
    task('k1', { ...open, labels: { key: 'agent:c0#post-1' } }),
    task('k2', { ...open, labels: { key: 'agent:c0#post-1' } }),
    task('w', { outcome: { done: 'c1g0o4' }, worker: 'agent:c2' }),
    task('p0', { ...open, labels: { project: 'proj-0' } }),
    task('p1', { ...open, labels: { project: 'proj-1' } }),
    task('x', { ...open, labels: { project: 'proj-1' }, needs: ['p0'] }),
    task('l1', { ...open, labels: { lane: 'capA', project: 'proj-2' } }),
    task('l2', { ...open, labels: { lane: 'capA', project: 'proj-2' } }),
    task('l3', open),
    task('m1', { ...open, labels: { lane: 'capB', project: 'proj-3' } }),
    task('m2', { ...open, labels: { lane: 'capB', project: 'proj-3' } }),
    task('m3', { ...open, labels: { lane: 'capB', project: 'proj-3' } }),
    task('gone2', { ...open, phase: 'cancelled' }),
  ]
  const lease = (id: string, claim: string, seq: number, cap?: Board.Cap) =>
    entry({ kind: 'claim', id, claim, seq, cap: cap ?? null, leaseMs: 100, untilMs: 1_100 + seq })
  const entries = [
    entry({ kind: 'finish', id: 'a', seq: 5, outcome: { done: 'first' } }),
    entry({ kind: 'finish', id: 'a', seq: 6, outcome: { done: 'second' } }),
    entry({ kind: 'steer', id: 'c', body: 's1', seq: 7 }),
    entry({ kind: 'steer', id: 'c', body: 's1', seq: 8 }),
    entry({ kind: 'heed', id: 'c', upto: 8, seq: 9 }),
    entry({ kind: 'heed', id: 'c', upto: 7, seq: 10 }),
    entry({ kind: 'renew', id: 'b', seq: 13 }),
    entry({ kind: 'post', id: 'gone', seq: 11 }),
    entry({ kind: 'post', id: 'x', seq: 12, labels: { project: 'proj-1' }, needs: ['p0'] }),
    lease('l1', 'agent:c0.20', 20, { max: 1, label: ['lane', 'capA'] }),
    lease('l2', 'agent:c0.21', 21, { max: lanes.capA, label: ['lane', 'capA'] }),
    lease('l1', 'agent:c0.22', 22),
    lease('l3', 'agent:c0.30', 30),
    lease('m1', 'agent:c0.50', 50, { max: lanes.capB, label: ['lane', 'capB'] }),
    {
      ...lease('m2', 'agent:c0p.51', 51, { max: lanes.capB, label: ['lane', 'capB'] }),
      pool: true,
    },
    { ...lease('m3', 'agent:c0p.52', 52), pool: true },
    entry({ kind: 'renew', id: 'l3', claim: 'agent:c0.30', seq: 31, t0: 1_000, leaseMs: 10 }),
    lease('l3', 'agent:c0.32', 32),
    lease('gone2', 'agent:c0.40', 40),
    entry({
      kind: 'finish',
      id: 'gone2',
      claim: 'agent:c0.40',
      ok: false,
      rejection: 'Terminal',
      t1: 1_041,
    }),
    entry({
      kind: 'claim',
      id: 'l2',
      ok: false,
      cap: { max: 1, label: ['lane', 'capA'] },
      rejection: 'Full',
      rejectionCap: { label: ['lane', 'capA'], max: 1 },
    }),
    entry({
      kind: 'release',
      id: 'l1',
      claim: 'agent:c0.20',
      ok: false,
      rejection: 'NotHolder',
      t1: 1_050,
    }),
    entry({ kind: 'release', id: 'l1', claim: 'agent:c0.20', seq: 23 }),
  ]
  const violations = check({
    entries,
    tasks,
    steers: new Map(),
    unjournaled: new Set(),
    killedRuns: new Set(),
    partitions: 4,
  })
  for (const expected of [
    'a finished more than once',
    'b was claimed with needs ["c"] not done',
    'keyed post agent:c0#post-1 made ["k1","k2"]',
    'steer ' + partitionOf('c', 4) + '/s1 landed at several seqs',
    'c heed ran backwards',
    'post gone was acknowledged but made no task',
    'w was finished by agent:c1 but names "agent:c2"',
    'post x in partition 0 took needs ["p0"] from others',
    'l2 was claimed at ',
    'l1 was claimed at 22 while held',
    'l1 release was refused NotHolder',
    'lane capA held 2 > 1',
    'lane capB held 3 > 2 at ' + partitionOf('proj-3', 4) + '/52 by ["raw:m1","pool:m2","pool:m3"]',
  ])
    assert.ok(
      violations.some((violation) => violation.startsWith(expected)),
      expected + ' in ' + JSON.stringify(violations),
    )
  for (const unexpected of [
    'seq ',
    'l3 was claimed at 32',
    'claim refused Full',
    'gone2 ',
    'lane capB held 2',
    'm3 was claimed',
  ])
    assert.ok(
      !violations.some((violation) => violation.startsWith(unexpected)),
      unexpected + ' in ' + JSON.stringify(violations),
    )
  const single = check({
    entries: [
      entry({ kind: 'renew', id: 'p0', seq: 3 }),
      entry({ kind: 'renew', id: 'p1', seq: 3 }),
    ],
    tasks,
    steers: new Map(),
    unjournaled: new Set(),
    killedRuns: new Set(),
    partitions: 1,
  })
  assert.ok(
    single.some((violation) => violation.startsWith('seq 0/3 went to several writes')),
    JSON.stringify(single),
  )
})

test(
  'Board fuzz: ' +
    children +
    (postgres ? ' processes on one Postgres schema, ' : ' processes on one SQLite file, ') +
    partitions +
    ' partitions, FUZZ_SEED=' +
    seed,
  { timeout: runMs + 120_000 },
  async (t) => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'nc-board-fuzz-'))
    const file = postgres
      ? 'nc_board_fuzz_' + seed + '_' + process.pid + '_' + Date.now().toString(36)
      : path.join(dir, 'board.sqlite')
    const random = generator(seed)
    const sql = openSql(file)
    const live = await Board.open({ sql, scope, actor: 'agent:checker', partitions })
    let passed = false
    try {
      const granted = await live.grant(
        { who: [{ kind: 'kind', id: 'agent' }], can: { '*': ['manage'] } },
        { key: 'grant-agents' },
      )
      assert.ok(granted.ok, JSON.stringify(granted))

      const untilMs = Date.now() + runMs
      const runs: Run[] = []
      const current = Array.from({ length: children }, (_, child) =>
        start(dir, file, child, 0, untilMs, random() < 0.75),
      )
      runs.push(...current)
      const watched: Entry[] = []
      while (Date.now() < untilMs - 1_000) {
        await sleep(100 + Math.floor(random() * 200))
        await live.refresh()
        for (const task of await live.tasks())
          if ((task.phase === 'done' || task.phase === 'failed') && random() < 0.05)
            watched.push({
              child: -1,
              generation: 0,
              id: task.id,
              kind: 'watched',
              outcome: task.outcome!,
              phase: task.phase,
              worker: task.worker ?? null,
            })
        if (random() < 0.1) {
          const child = Math.floor(random() * children)
          const victim = current[child]!
          victim.killed = true
          victim.proc.kill('SIGKILL')
          await victim.exit
          await sleep(Math.floor(random() * 200))
          current[child] = start(dir, file, child, victim.generation + 1, untilMs, random() < 0.75)
          runs.push(current[child])
        }
      }

      const deadline = setTimeout(() => runs.forEach((run) => run.proc.kill('SIGKILL')), 60_000)
      const exits = await Promise.all(runs.map((run) => run.exit))
      clearTimeout(deadline)
      for (const [i, exit] of exits.entries())
        if (!runs[i]!.killed && exit.code !== 0)
          assert.fail(
            'child ' +
              runs[i]!.child +
              ' generation ' +
              runs[i]!.generation +
              ' exited ' +
              (exit.code ?? exit.signal) +
              ':\n' +
              exit.stderr,
          )

      await sleep(400)
      const atMs = Date.now()
      await live.refresh()
      const liveTasks = (await live.tasks(atMs)).toSorted((a, b) => a.id.localeCompare(b.id))
      const replaySql = openSql(file)
      const replay = await Board.open({
        sql: replaySql,
        scope,
        actor: 'agent:checker',
        partitions,
      })
      try {
        const tasks = (await replay.tasks(atMs)).toSorted((a, b) => a.id.localeCompare(b.id))
        assert.deepEqual(
          tasks,
          liveTasks,
          'replaying the log gives other tasks than the live handle',
        )
        const steers = new Map<string, Board.Steer[]>()
        for (const task of tasks) {
          const replayed = await replay.steers(task.id)
          assert.deepEqual(replayed, await live.steers(task.id), task.id + ' replays other steers')
          if (replayed.length > 0) steers.set(task.id, replayed)
        }

        const entries = [...journals(dir), ...watched]
        const killedRuns = new Set(
          runs.filter((run) => run.killed).map((run) => run.child + '/' + run.generation),
        )
        const unjournaled = new Set(
          entries
            .filter(
              (entry) =>
                entry.kind === 'pool-run' ||
                (entry.kind === 'claim' &&
                  entry.ok &&
                  killedRuns.has(entry.child + '/' + entry.generation)),
            )
            .map((entry) => entry.id!),
        )
        const violations = check({ entries, tasks, steers, unjournaled, killedRuns, partitions })
        for (const task of tasks.filter((task) => task.phase === 'claimed')) {
          if (task.stage === 'held' || task.untilMs! > atMs)
            violations.push(
              task.id + ' is still held after every lease lapsed: ' + JSON.stringify(task),
            )
          const taken = await replay.claim(task.id, { leaseMs: 60_000, atMs })
          if (!taken.ok || !taken.tookOver)
            violations.push(
              task.id + ' could not be taken over after its lease lapsed: ' + JSON.stringify(taken),
            )
        }

        const count = (kind: string) =>
          entries.filter((entry) => entry.kind === kind && entry.ok).length
        t.diagnostic(
          'seed ' +
            seed +
            ': ' +
            partitions +
            ' partitions, ' +
            tasks.length +
            ' tasks, ' +
            runs.length +
            ' processes (' +
            runs.filter((run) => run.killed).length +
            ' killed), ' +
            count('claim') +
            ' claims, ' +
            count('finish') +
            ' finishes, ' +
            entries.filter((entry) => entry.kind === 'pool-run').length +
            ' pool runs (' +
            entries.filter((entry) => entry.kind === 'claim' && entry.ok && entry.pool).length +
            ' pool claims, ' +
            entries.filter((entry) => entry.kind === 'pool-start' && entry.lane !== 'pool').length +
            ' label-capped pools), ' +
            count('steer') +
            ' steers, ' +
            count('heed') +
            ' heeds, ' +
            tasks.filter((task) => task.phase === 'done').length +
            ' done, ' +
            tasks.filter((task) => task.phase === 'failed').length +
            ' failed',
        )
        const rejected = new Map<string, number>()
        for (const entry of entries.filter((entry) => entry.ok === false)) {
          const name =
            (entry.pool ? 'pool-' : '') +
            entry.kind +
            ':' +
            (entry.rejection ?? 'error') +
            (entry.rejection === 'Full' ? (typeof entry.cap === 'number' ? '#' : '@label') : '')
          rejected.set(name, (rejected.get(name) ?? 0) + 1)
        }
        t.diagnostic('rejections ' + JSON.stringify(Object.fromEntries([...rejected].toSorted())))
        assert.deepEqual(violations.slice(0, 20), [], violations.length + ' invariant violations')
        assert.ok(count('finish') > 0 && count('heed') > 0, 'the run finished and heeded nothing')
        passed = true
      } finally {
        await replay.close()
        await replaySql.close()
      }
    } catch (error) {
      throw new Error(
        'FUZZ_SEED=' +
          seed +
          ' FUZZ_MS=' +
          runMs +
          ' FUZZ_PARTITIONS=' +
          partitions +
          ' failed; journals and board kept in ' +
          dir +
          (postgres ? ' and Postgres schema ' + file : '') +
          '\n' +
          String((error as Error)?.stack ?? error),
      )
    } finally {
      await live.close()
      await sql.close()
      if (passed) fs.rmSync(dir, { recursive: true, force: true })
      if (passed && postgres) {
        const admin = Postgres.open(postgres)
        try {
          await admin.transaction(
            'log',
            'drop',
            [['DROP SCHEMA IF EXISTS ' + file + ' CASCADE']],
            'schema',
          )
        } finally {
          await admin.close()
        }
      }
    }
  },
)
