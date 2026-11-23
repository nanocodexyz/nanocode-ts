// A Cloudflare Worker whose Durable Object holds a Board over its own SQLite storage
// (nanocode-ts/cloudflare Sql.create(ctx.storage)). Every request goes to one object, and each
// client is an actor with its own Board handle inside it:
//
//   POST /tasks   { actor, task, key? }          posts a task
//   POST /claim   { actor, id, leaseMs, cap? }   claims it under a lease (cap: number or { max, label })
//   POST /finish  { actor, id, claim, outcome }  finishes it with { done } or { failed }
//   GET  /tasks                                  lists every task, with the worker that finished it
//
// A refused write is not an error: the reply is the verdict, { ok: false, rejection }.
// Run it with: npm run example:board-durable-object (index.ts serves it with workerd).

import { DurableObject } from 'cloudflare:workers'
import { Board } from 'nanocode-ts'
import { Sql } from 'nanocode-ts/cloudflare'
import wasm from 'nanocode-ts/nanocode_wasm.wasm'

type Env = { BOARD: DurableObjectNamespace<BoardObject> }

type Body = {
  actor?: string
  key?: string
  task?: Board.NewTask
  id?: string
  leaseMs?: number
  cap?: Board.Cap
  claim?: string
  outcome?: Board.Outcome
}

export class BoardObject extends DurableObject<Env> {
  readonly boards = new Map<string, Promise<Board.Board>>()

  // One handle per actor, all over the object's one storage. The first to open charters the
  // scope and lets every agent: actor manage it; the grant is refused for everyone else.
  board(actor: string): Promise<Board.Board> {
    let opened = this.boards.get(actor)
    if (!opened) {
      opened = (async () => {
        const board = await Board.open({
          sql: Sql.create(this.ctx.storage),
          scope: 'example',
          actor: 'agent:' + actor,
          module: wasm,
        })
        await board.grant(
          { who: [{ kind: 'kind', id: 'agent' }], can: { '*': ['manage'] } },
          { key: 'grant-agents' },
        )
        return board
      })()
      opened.catch(() => this.boards.delete(actor))
      this.boards.set(actor, opened)
    }
    return opened
  }

  override async fetch(request: Request): Promise<Response> {
    const route = request.method + ' ' + new URL(request.url).pathname
    try {
      if (route === 'GET /tasks') {
        const board = await this.board('reader')
        await board.refresh() // reads come from the handle's fold: catch up on the others' writes
        const tasks = (await board.tasks()).map(({ id, labels, stage, worker, outcome }) => ({
          id,
          labels,
          stage,
          worker: worker ?? null,
          outcome: outcome ?? null,
        }))
        return Response.json(tasks)
      }
      const body = (await request.json()) as Body
      if (!body.actor) return Response.json({ error: 'actor is required' }, { status: 400 })
      const board = await this.board(body.actor)
      await board.refresh()
      if (route === 'POST /tasks' && body.task)
        return Response.json(await board.post(body.task, body.key ? { key: body.key } : {}))
      if (route === 'POST /claim' && body.id && body.leaseMs)
        return Response.json(
          await board.claim(body.id, {
            leaseMs: body.leaseMs,
            ...(body.cap === undefined ? {} : { cap: body.cap }),
          }),
        )
      if (route === 'POST /finish' && body.id && body.claim && body.outcome)
        return Response.json(await board.finish(body.id, body.claim, body.outcome))
      return Response.json({ error: 'unknown route ' + route }, { status: 404 })
    } catch (error) {
      const message = error instanceof Error ? (error.stack ?? error.message) : String(error)
      return Response.json({ error: message }, { status: 500 })
    }
  }
}

// Every board request goes to the one object named board.
export default {
  fetch(request: Request, env: Env): Promise<Response> {
    return env.BOARD.get(env.BOARD.idFromName('board')).fetch(request)
  },
} satisfies ExportedHandler<Env>
