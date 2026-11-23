import { Sql } from 'nanocode-ts/node'
import { Sql as Postgres } from 'nanocode-ts/postgres'

export function openSql(file: string) {
  const url = process.env.NC_BOARD_POSTGRES
  return url ? Postgres.open(url, { schema: file }) : Sql.open(file)
}

export function generator(seed: number) {
  let state = seed >>> 0
  return () => {
    state = (state + 0x6d2b79f5) >>> 0
    let t = state
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}

export const projects = ['proj-0', 'proj-1', 'proj-2', 'proj-3', 'proj-4', 'proj-5'] as const

export const lanes = { capA: 1, capB: 2 } as const
export type Lane = keyof typeof lanes

const mask = (1n << 64n) - 1n

export function partitionOf(key: string, count: number) {
  let h = 0xcbf29ce484222325n
  for (const byte of new TextEncoder().encode(key)) {
    h ^= BigInt(byte)
    h = (h * 0x100000001b3n) & mask
  }
  h ^= h >> 33n
  h = (h * 0xff51afd7ed558ccdn) & mask
  h ^= h >> 33n
  h = (h * 0xc4ceb9fe1a85ec53n) & mask
  h ^= h >> 33n
  return Number(h % BigInt(count))
}

export type Placed = { id: string; labels: Record<string, string>; parent?: string | null }

export function placer(count: number, lookup: (id: string) => Placed | undefined) {
  const place = (task: Placed, depth = 0): number => {
    if (count <= 1) return 0
    if (task.labels.project !== undefined) return partitionOf(task.labels.project, count)
    const parent = task.parent ? lookup(task.parent) : undefined
    if (parent && depth < 64) return place(parent, depth + 1)
    return partitionOf(task.id, count)
  }
  return place
}
