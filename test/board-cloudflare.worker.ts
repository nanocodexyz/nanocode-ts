// Worker that test/board-cloudflare.test.ts bundles and runs in workerd. One Durable Object per
// URL path holds any number of Board handles (one per actor), each opened with Board.open over
// Sql.create(ctx.storage), the way a swarm's object opens its board. The test drives them with
// POST bodies { actor, method, args } and reads back what the call returned.

import { DurableObject } from 'cloudflare:workers'
import { Board } from 'nanocode-ts'
import { Sql } from 'nanocode-ts/cloudflare'
import wasm from 'nanocode-ts/nanocode_wasm.wasm'

type Env = { BOARD: DurableObjectNamespace<BoardObject> }

type Call = { actor: string; method: string; args?: unknown[]; scope?: string }

const methods = new Set([
  'post',
  'claim',
  'renew',
  'release',
  'finish',
  'cancel',
  'reply',
  'steer',
  'steers',
  'heed',
  'ready',
  'task',
  'tasks',
  'held',
  'refresh',
  'charter',
  'grant',
  'group',
  'opened',
  'checkpoint',
  'close',
])

export class BoardObject extends DurableObject<Env> {
  // A fresh id per instance, so the test can tell a restarted object from the one it replaced.
  readonly boot = crypto.randomUUID()
  readonly boards = new Map<string, Promise<Board.Board>>()

  board(actor: string, scope: string): Promise<Board.Board> {
    let opened = this.boards.get(actor)
    if (!opened) {
      opened = Board.open({
        sql: Sql.create(this.ctx.storage),
        scope,
        actor: 'agent:' + actor,
        module: wasm,
      })
      opened.catch(() => this.boards.delete(actor))
      this.boards.set(actor, opened)
    }
    return opened
  }

  override async fetch(request: Request): Promise<Response> {
    const call = (await request.json()) as Call
    // Drops this instance; the next request constructs a new one over the same storage.
    if (call.method === 'abort') this.ctx.abort('board-cloudflare test restart')
    if (call.method === 'boot') return Response.json({ boot: this.boot, value: null })
    // Drops every fold checkpoint, so the next open replays the whole log.
    if (call.method === 'drop_checkpoints') {
      this.ctx.storage.sql.exec('DELETE FROM nanocode_checkpoint')
      return Response.json({ boot: this.boot, value: null })
    }
    if (!methods.has(call.method))
      return Response.json(
        { boot: this.boot, error: 'unknown method ' + call.method },
        { status: 400 },
      )
    try {
      const board = await this.board(call.actor, call.scope ?? 'cloudflare')
      const method = (board as unknown as Record<string, (...args: unknown[]) => unknown>)[
        call.method
      ]!
      const value = await method.apply(board, call.args ?? [])
      if (call.method === 'close') this.boards.delete(call.actor)
      return Response.json({ boot: this.boot, value: value ?? null })
    } catch (error) {
      const message = error instanceof Error ? (error.stack ?? error.message) : String(error)
      return Response.json({ boot: this.boot, error: message }, { status: 500 })
    }
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const stub = env.BOARD.get(env.BOARD.idFromName(new URL(request.url).pathname))
    try {
      return await stub.fetch(request)
    } catch (error) {
      // ctx.abort() fails the request that called it; report that as the restart it was.
      return Response.json({ aborted: String(error) })
    }
  },
}
