import type * as Contracts from './Contracts.ts'

let next: Contracts.SqlReach | undefined

/**
 * Runs write, tagging the board call it makes synchronously (before its first await) with reach:
 * the board hands reach to its host as transaction's fifth argument for every transaction that
 * call's request runs. A board call write makes after an await carries no reach, so it wakes
 * every pool. Passed explicitly rather than through an AsyncLocalStorage, which turns on promise
 * hooks for the whole process and carries into the timers a write starts.
 */
export function reaching<T>(reach: Contracts.SqlReach, write: () => Promise<T>): Promise<T> {
  const outer = next
  next = reach
  try {
    return write()
  } finally {
    next = outer
  }
}

/** The reach of the board call being made right now, if any; a board reads it as it queues one. */
export function current(): Contracts.SqlReach | undefined {
  return next
}
