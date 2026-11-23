import * as Board from './Board.ts'

export * from './Board.ts'

export type PoolLease = Board.Lease & {
  readonly tookOver: boolean
  steers(): Promise<readonly Board.Steer[]>
  heed(seq: number): Promise<void>
}

export type PoolOptions = {
  label?: [string, string] | undefined
  cap?: Board.Cap | ((task: Board.Task) => Board.Cap | undefined) | undefined
  concurrency: number
  leaseMs: number
  pollMs: number
  idleMs?: number | undefined
  run: (task: Board.Task, lease: PoolLease) => Promise<Board.Outcome>
  /**
   * Subscribes to new work on the board: the pool polls as soon as notify fires, and still every
   * pollMs as a fallback. A lapse writes nothing, so while it has a free slot the pool also polls
   * 5 ms after the soonest lease it sees on another holder's task ends, when that comes before
   * pollMs. Returns the unsubscribe the pool calls when it stops or retires. Postgres hosts give
   * one with nanocode-ts/postgres Sql.wake; node:sqlite has none and keeps polling.
   */
  wake?: ((notify: () => void) => PoolWake) | undefined
}

/**
 * Stops a wake subscription. own, when given, runs one of the pool's own writes so that it wakes
 * no pool (a claim, renewal or heed, which never makes work ready) or every pool but this one (a
 * finish or release: this pool polls when a run ends anyway). Each write makes its board call
 * before its first await, which is how the board tells the host its reach.
 */
export type PoolWake = (() => void) & {
  own?<T>(reach: 'none' | 'others', write: () => Promise<T>): Promise<T>
}

export type PoolState = 'running' | 'stopping' | 'stopped' | 'retired'

export type PoolStats = {
  state: PoolState
  running: number
  claimed: number
  done: number
  failed: number
  lost: number
  released: number
  delivered: number
  unheard: number
  retries: number
  polls: number
  errors: number
  lastError?: string
}

export type Pool = {
  stop(): Promise<void>
  idle(): Promise<void>
  stats(): PoolStats
}

type Phase =
  | { phase: 'running' }
  | { phase: 'finishing' }
  | { phase: 'stopped' }
  | { phase: 'lost'; lost: Board.LeaseLost }

type Worker = {
  readonly task: Board.Task
  readonly claim: string
  readonly abort: AbortController
  untilMs: number
  state: Phase
  delivered: number
  timer: ReturnType<typeof setInterval>
  renewing: Promise<void>
  settled: Promise<void>
}

const holding = (kind: Board.RejectionKind) =>
  kind === 'NotHolder' || kind === 'Held' || kind === 'Terminal'

const failure = (error: unknown) =>
  error instanceof Error ? error.message : typeof error === 'string' ? error : JSON.stringify(error)

const unknown = (error: unknown): Board.Rejection => ({ kind: 'Unknown', value: failure(error) })

const refused = (error: unknown): Board.Verdict => ({ ok: false, rejection: unknown(error) })

function capped(cap: PoolOptions['cap'], task: Board.Task): Board.Cap | undefined {
  const chosen = typeof cap === 'function' ? cap(task) : cap
  if (typeof chosen === 'object' && task.labels[chosen.label[0]] !== chosen.label[1])
    return undefined
  return chosen
}

function outcome(value: unknown): Board.Outcome {
  const record = value as Record<string, unknown> | null
  if (record && typeof record.done === 'string') return { done: record.done }
  if (record && typeof record.failed === 'string') return { failed: record.failed }
  return { failed: 'run returned ' + JSON.stringify(value) + ', not { done } or { failed }' }
}

export function pool(board: Board.Board, options: PoolOptions): Pool {
  const { label, cap, concurrency, leaseMs, pollMs, idleMs, run } = options
  if (!Number.isSafeInteger(concurrency) || concurrency < 1)
    throw new Error('concurrency must be a positive integer')
  if (!Number.isSafeInteger(leaseMs) || leaseMs < 1)
    throw new Error('leaseMs must be a positive integer')
  if (!Number.isSafeInteger(pollMs) || pollMs < 1)
    throw new Error('pollMs must be a positive integer')
  if (idleMs !== undefined && (!Number.isSafeInteger(idleMs) || idleMs < 0))
    throw new Error('idleMs must be a non-negative integer')

  const stats: PoolStats = {
    state: 'running',
    running: 0,
    claimed: 0,
    done: 0,
    failed: 0,
    lost: 0,
    released: 0,
    delivered: 0,
    unheard: 0,
    retries: 0,
    polls: 0,
    errors: 0,
  }
  const running = new Map<string, Worker>()
  let waiters: (() => void)[] = []
  let idleSince: number | undefined
  let timer: ReturnType<typeof setTimeout> | undefined
  // With wake, when the next lease another holder has here ends: a lapse writes nothing, so no
  // notification would announce it.
  let lapseAt: number | undefined
  let ticking: Promise<void> | undefined
  let again = false
  let stopping: Promise<void> | undefined

  const own = <T>(reach: 'none' | 'others', write: () => Promise<T>) =>
    unwake?.own ? unwake.own(reach, write) : write()

  const error = (cause: unknown) => {
    stats.errors++
    stats.lastError = failure(cause)
  }

  const wake = () => {
    const woken = waiters
    waiters = []
    for (const resolve of woken) resolve()
  }

  const lose = (worker: Worker, rejection?: Board.Rejection) => {
    if (worker.state.phase === 'lost' || worker.state.phase === 'stopped') return
    const lost = new Board.LeaseLost(worker.task.id, rejection)
    worker.state = { phase: 'lost', lost }
    stats.lost++
    worker.abort.abort(lost)
  }

  const gone = (worker: Worker) => (worker.state.phase === 'lost' ? worker.state.lost : undefined)

  const renew = async (worker: Worker) => {
    if (worker.state.phase !== 'running') return
    const at = Date.now()
    let renewed: Board.Verdict
    try {
      renewed = await own('none', () => board.renew(worker.task.id, worker.claim, leaseMs))
    } catch (cause) {
      error(cause)
      if (Date.now() >= worker.untilMs) lose(worker, unknown(cause))
      return
    }
    if (renewed.ok) worker.untilMs = at + leaseMs
    else lose(worker, renewed.rejection)
  }

  const pending = async (worker: Worker) => {
    const before = gone(worker)
    if (before) throw before
    try {
      return await board.steers(worker.task.id, worker.claim)
    } catch (cause) {
      throw gone(worker) ?? cause
    }
  }

  const heed = async (worker: Worker, seq: number) => {
    const before = gone(worker)
    if (before) throw before
    const heeded = await own('none', () => board.heed(worker.task.id, worker.claim, seq))
    if (heeded.ok) return
    if (holding(heeded.rejection.kind)) lose(worker, heeded.rejection)
    throw (
      gone(worker) ??
      new Error('heed ' + worker.task.id + ' refused: ' + JSON.stringify(heeded.rejection))
    )
  }

  const finish = async (worker: Worker, result: Board.Outcome) => {
    for (;;) {
      if (worker.state.phase !== 'finishing') return
      const finished = await own('others', () =>
        board.finish(worker.task.id, worker.claim, result),
      ).catch(refused)
      if (finished.ok) {
        if ('done' in result) stats.done++
        else stats.failed++
        return
      }
      if (finished.rejection.kind !== 'Steer') {
        if (finished.rejection.kind === 'Unknown') error(finished.rejection.value)
        lose(worker, finished.rejection)
        return
      }
      stats.retries++
      const steers = await pending(worker)
      const last = steers.at(-1)
      if (!last) continue
      stats.unheard += steers.filter((steer) => steer.seq > worker.delivered).length
      worker.delivered = Math.max(worker.delivered, last.seq)
      await heed(worker, last.seq)
    }
  }

  const start = (
    task: Board.Task,
    claimed: { claim: string; untilMs: number; tookOver: boolean },
  ) => {
    const abort = new AbortController()
    const worker: Worker = {
      task,
      claim: claimed.claim,
      abort,
      untilMs: claimed.untilMs,
      state: { phase: 'running' },
      delivered: 0,
      timer: setInterval(
        () => {
          worker.renewing = worker.renewing.then(() => renew(worker))
        },
        Math.max(1, Math.floor(leaseMs / 3)),
      ),
      renewing: Promise.resolve(),
      settled: Promise.resolve(),
    }
    const lease: PoolLease = {
      id: task.id,
      claim: claimed.claim,
      get untilMs() {
        return worker.untilMs
      },
      signal: abort.signal,
      tookOver: claimed.tookOver,
      async steers() {
        const steers = await pending(worker)
        const fresh = steers.filter((steer) => steer.seq > worker.delivered)
        const last = fresh.at(-1)
        if (last) {
          stats.delivered += fresh.length
          worker.delivered = last.seq
        }
        return steers
      },
      heed: (seq) => heed(worker, seq),
    }
    running.set(task.id, worker)
    stats.running = running.size
    worker.settled = (async () => {
      let result: Board.Outcome | undefined
      try {
        result = outcome(await run(task, lease))
      } catch (cause) {
        if (cause instanceof Board.LeaseLost) lose(worker, cause.rejection)
        else result = { failed: failure(cause) }
      }
      if (worker.state.phase !== 'running') return
      worker.state = { phase: 'finishing' }
      clearInterval(worker.timer)
      await worker.renewing
      if (result) await finish(worker, result)
    })()
      .catch((cause: unknown) => {
        if (!(cause instanceof Board.LeaseLost)) error(cause)
      })
      .finally(() => {
        clearInterval(worker.timer)
        running.delete(task.id)
        stats.running = running.size
        if (worker.state.phase !== 'stopped') kick()
      })
  }

  const tick = async () => {
    stats.polls++
    try {
      await board.refresh()
      if (running.size < concurrency) {
        // Lapsed tasks first: a dead holder's task waits behind no fresh one for a cap slot.
        const listed = await board.ready(label ? { label } : undefined)
        const ready = [
          ...listed.filter((task) => task.stage === 'lapsed'),
          ...listed.filter((task) => task.stage !== 'lapsed'),
        ]
        for (const task of ready) {
          if (stats.state !== 'running' || running.size >= concurrency) break
          if (running.has(task.id)) continue
          const limit = capped(cap, task)
          const claimed = await own('none', () =>
            board.claim(task.id, limit === undefined ? { leaseMs } : { cap: limit, leaseMs }),
          )
          if (!claimed.ok) continue
          stats.claimed++
          start(task, claimed)
        }
      }
      lapseAt = undefined
      if (options.wake && stats.state === 'running' && running.size < concurrency) {
        const now = Date.now()
        for (const task of await board.held()) {
          // A lease already past is ready work, which this tick claimed or a cap holds back.
          if (running.has(task.id) || task.untilMs === undefined || task.untilMs <= now) continue
          if (label && task.labels[label[0]] !== label[1]) continue
          if (lapseAt === undefined || task.untilMs < lapseAt) lapseAt = task.untilMs
        }
      }
    } catch (cause) {
      error(cause)
    }
    if (running.size > 0 || stats.state !== 'running') {
      idleSince = undefined
      return
    }
    const now = Date.now()
    idleSince ??= now
    if (idleMs === undefined) wake()
    else if (now - idleSince >= idleMs) void shut('retired')
  }

  const unschedule = () => {
    if (timer !== undefined) clearTimeout(timer)
    timer = undefined
  }

  const schedule = () => {
    unschedule()
    if (stats.state !== 'running') return
    // 5 ms past the lease's end, so the claim lands after the lapse rather than on a live lease.
    const lapse = lapseAt === undefined ? pollMs : Math.max(0, lapseAt + 5 - Date.now())
    timer = setTimeout(kick, Math.min(pollMs, lapse))
  }

  function kick() {
    if (stats.state !== 'running') return
    if (ticking) {
      again = true
      return
    }
    unschedule()
    ticking = tick().finally(() => {
      ticking = undefined
      if (again) {
        again = false
        kick()
      } else schedule()
    })
  }

  function shut(state: 'stopped' | 'retired') {
    if (stopping) return stopping
    stats.state = 'stopping'
    unschedule()
    unwake?.()
    stopping = (async () => {
      await ticking
      const held = [...running.values()].filter((worker) => worker.state.phase === 'running')
      for (const worker of held) {
        worker.state = { phase: 'stopped' }
        clearInterval(worker.timer)
        worker.abort.abort(new Error('the pool stopped'))
      }
      await Promise.all(
        held.map(async (worker) => {
          await worker.renewing
          const released = await own('others', () =>
            board.release(worker.task.id, worker.claim, 'pool stopped'),
          ).catch(refused)
          if (released.ok) stats.released++
          else if (!holding(released.rejection.kind)) error(released.rejection)
        }),
      )
      await Promise.allSettled([...running.values()].map((worker) => worker.settled))
      stats.state = state
      wake()
    })()
    return stopping
  }

  // A full pool ignores wakes: a run that ends polls anyway.
  const unwake = options.wake?.(() => {
    if (running.size < concurrency) kick()
  })

  kick()

  return {
    stop: () => shut('stopped'),
    idle() {
      if (stats.state === 'stopped' || stats.state === 'retired') return Promise.resolve()
      return new Promise<void>((resolve) => waiters.push(resolve))
    },
    stats: () => ({ ...stats }),
  }
}
