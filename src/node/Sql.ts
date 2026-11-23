import * as sqlite from 'node:sqlite'
import type * as Contracts from '../Contracts.ts'

/** A SQLite file as a Contracts.SqlHost, shareable by several processes at once. */
export type Host = Omit<Contracts.SqlHost, 'transaction'> & {
  readonly path: string
  transaction(
    kind: Contracts.SqlKind,
    operation: string,
    statements: Contracts.Statement[],
    access?: Contracts.SqlAccess,
  ): Contracts.SqlValue[][][]
  close(): void
}

export type Options = {
  /** How long a write waits for another connection's write lock before failing (default 10 s). */
  busyTimeoutMs?: number
  /** The largest bound string or blob, in bytes, the runtime may store (default unlimited). */
  valueLimit?: number
}

type Row = Record<string, unknown> | unknown[]

type Prepared = {
  all(...values: unknown[]): Row[]
  setReturnArrays?(enabled: boolean): void
}

type Database = {
  exec(query: string): void
  prepare(query: string): Prepared
  close(): void
  readonly isTransaction?: boolean
}

/**
 * Opens (creating it if missing) the SQLite database at `path` in WAL mode. Writes take the
 * write lock up front (BEGIN IMMEDIATE), so a writer in another process waits up to
 * `busyTimeoutMs` instead of failing midway; reads run in one snapshot. `':memory:'` gives a
 * private in-memory database. Needs node:sqlite: Node 22.5+ (with --experimental-sqlite
 * before 22.13) or Node 23+.
 */
export function open(path: string, options: Options = {}): Host {
  const busyTimeoutMs = options.busyTimeoutMs ?? 10_000
  if (!Number.isSafeInteger(busyTimeoutMs) || busyTimeoutMs < 0)
    throw new Error('busyTimeoutMs must be a non-negative integer')
  const valueLimit = options.valueLimit
  if (valueLimit !== undefined && (!Number.isSafeInteger(valueLimit) || valueLimit <= 0))
    throw new Error('valueLimit must be a positive integer')

  const db = new sqlite.DatabaseSync(path) as unknown as Database
  try {
    db.exec('PRAGMA busy_timeout = ' + busyTimeoutMs)
    if (path !== ':memory:' && path !== '') wal(db, busyTimeoutMs)
    db.exec('PRAGMA synchronous = NORMAL')
  } catch (error) {
    db.close()
    throw error
  }

  type Entry = { statement: Prepared; arrays: boolean; order: number[] | undefined }
  const prepared = new Map<string, Entry>()
  const prepare = (query: string) => {
    let entry = prepared.get(query)
    if (!entry) {
      let statement = db.prepare(query)
      const arrays = typeof statement.setReturnArrays === 'function'
      let order: number[] | undefined
      if (arrays) statement.setReturnArrays!(true)
      else {
        // Node before 22.16 has no setReturnArrays and cannot bind a numbered parameter used
        // twice (?1 ... ?1), so number the parameters anew and repeat their values, and run a
        // row-returning query as a subquery, which makes SQLite rename repeated column names
        // (a, a:1) in order instead of dropping them from the object rows.
        const numbered = renumber(query)
        let text = numbered?.query ?? query
        order = numbered?.order
        if (/^\s*(select|with|values)\b/i.test(text))
          text = 'SELECT * FROM (' + text.replace(/;\s*$/, '') + ')'
        if (text !== query) statement = db.prepare(text)
      }
      entry = { statement, arrays, order }
      prepared.set(query, entry)
    }
    return entry
  }

  const run = (statements: Contracts.Statement[]) =>
    statements.map(([query, ...values]) => {
      const { statement, arrays, order } = prepare(query)
      const bound = order ? order.map((index) => values[index - 1]!) : values
      return statement
        .all(...bound.map(bind))
        .map((row) =>
          (arrays ? (row as unknown[]) : Object.values(row as Record<string, unknown>)).map(read),
        )
    })

  let closed = false
  return {
    path,
    ...(valueLimit === undefined ? {} : { value_limit: valueLimit }),
    transaction(_kind, _operation, statements, access = 'write') {
      if (closed) throw new Error('the SQLite database ' + path + ' is closed')
      db.exec(access === 'read' ? 'BEGIN DEFERRED' : 'BEGIN IMMEDIATE')
      let rows: Contracts.SqlValue[][][]
      try {
        rows = run(statements)
      } catch (error) {
        if (db.isTransaction !== false) {
          try {
            db.exec('ROLLBACK')
          } catch {}
        }
        throw error
      }
      db.exec('COMMIT')
      return rows
    },
    close() {
      if (closed) return
      closed = true
      prepared.clear()
      db.close()
    },
  }
}

function wal(db: Database, busyTimeoutMs: number) {
  // Switching a fresh file to WAL takes an exclusive lock that busy_timeout does not always
  // wait for, so a second process opening the same new file at once retries here.
  const deadline = Date.now() + busyTimeoutMs
  for (;;) {
    try {
      const row = db.prepare('PRAGMA journal_mode = WAL').all()[0]
      const mode = Array.isArray(row) ? row[0] : row && Object.values(row)[0]
      if (String(mode).toLowerCase() !== 'wal')
        throw new Error('SQLite would not switch to WAL mode (journal_mode is ' + mode + ')')
      return
    } catch (error) {
      if (!busy(error) || Date.now() >= deadline) throw error
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 5)
    }
  }
}

/** Rewrites ?N parameters to plain ?, returning which value each one takes, or undefined. */
function renumber(query: string): { query: string; order: number[] } | undefined {
  if (!/\?\d/.test(query)) return undefined
  const order: number[] = []
  let out = ''
  let next = 1
  for (let i = 0; i < query.length; ) {
    const c = query[i]!
    const close = c === "'" || c === '"' || c === '`' ? c : c === '[' ? ']' : undefined
    if (close) {
      const end = query.indexOf(close, i + 1)
      const stop = end === -1 ? query.length : end + 1
      out += query.slice(i, stop)
      i = stop
    } else if (c === '-' && query[i + 1] === '-') {
      const end = query.indexOf('\n', i)
      const stop = end === -1 ? query.length : end
      out += query.slice(i, stop)
      i = stop
    } else if (c === '/' && query[i + 1] === '*') {
      const end = query.indexOf('*/', i + 2)
      const stop = end === -1 ? query.length : end + 2
      out += query.slice(i, stop)
      i = stop
    } else if (c === '?') {
      const digits = /^\d+/.exec(query.slice(i + 1))?.[0]
      const index = digits ? Number(digits) : next
      order.push(index)
      next = Math.max(next, index + 1)
      out += '?'
      i += 1 + (digits?.length ?? 0)
    } else {
      out += c
      i++
    }
  }
  return { query: out, order }
}

function busy(error: unknown) {
  return /SQLITE_BUSY|database is locked/i.test(
    String((error as { message?: unknown } | null)?.message ?? error),
  )
}

function bind(value: Contracts.SqlValue): unknown {
  return value instanceof ArrayBuffer ? new Uint8Array(value) : value
}

function read(value: unknown): Contracts.SqlValue {
  if (value instanceof Uint8Array)
    return value.buffer.slice(value.byteOffset, value.byteOffset + value.byteLength) as ArrayBuffer
  if (typeof value === 'bigint') return Number(value)
  return value as Contracts.SqlValue
}
