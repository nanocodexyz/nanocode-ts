use std::{cell::RefCell, collections::BTreeMap, num::NonZeroU32, sync::Arc};

use nanocode_board::{
    Applied, Board, BoardError, Cap, ClaimId, EventId, Fold, Op, Phase, Rejection, Retain, Task,
    TaskId,
    access::{Identity, Verifier},
    checkpoint::{self as checkpoints, Fallback, Opened},
    messages,
    shards::{self, Merged, Route},
    steer::{self, Steer},
    tasks,
};
use nanocode_partitions::Partition;
use nanocode_turnlog::{LogHost, SqlLog};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use super::store::WorkerStore;

thread_local! {
    static BOARDS: RefCell<BTreeMap<u32, Option<Vec<Board>>>> = const { RefCell::new(BTreeMap::new()) };
    /// Per open board, each partition's next seq when it last opened from or saved a checkpoint,
    /// so a close does not rewrite a checkpoint the fold has not moved past.
    static SAVED: RefCell<BTreeMap<u32, Vec<u64>>> = const { RefCell::new(BTreeMap::new()) };
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Request {
    Checkpoint {
        board: u32,
    },
    Close {
        board: u32,
    },
    Group {
        board: u32,
        writes: Vec<Write>,
    },
    Open {
        actor: String,
        partitions: NonZeroU32,
        scope: String,
        /// Events a partition folds past its last checkpoint before it saves another; 0 saves
        /// none on its own. Unset keeps nanocode's default.
        #[serde(default)]
        checkpoint_every: Option<u64>,
        #[serde(default)]
        retain: Option<Retain>,
    },
    Opened {
        board: u32,
    },
    Ready {
        at_ms: u64,
        board: u32,
        #[serde(default)]
        label: Option<(String, String)>,
    },
    Refresh {
        board: u32,
    },
    RenewsPerLease {},
    Steers {
        board: u32,
        #[serde(default)]
        claim: Option<ClaimId>,
        task: TaskId,
    },
    Task {
        at_ms: u64,
        board: u32,
        task: TaskId,
    },
    Tasks {
        at_ms: u64,
        board: u32,
    },
    TextLimit {
        board: u32,
    },
    Write {
        board: u32,
        write: Write,
    },
}

#[derive(Deserialize)]
pub(crate) struct Write {
    at_ms: u64,
    #[serde(default)]
    by: Option<String>,
    #[serde(default)]
    key: Option<String>,
    op: Op,
}

struct Trusted;

impl Verifier for Trusted {
    fn verify(&self, credential: &str) -> Option<String> {
        Some(credential.to_owned())
    }
}

pub(crate) async fn execute(request: Request) -> Result<Value, String> {
    match request {
        Request::Checkpoint { board } => {
            let mut boards = take(board)?;
            let saved = checkpoint(board, &mut boards, true).await;
            put(board, boards);
            saved.map(Value::Bool)
        }
        Request::Close { board } => {
            // A closing board saves its fold for the next open; a failed save only means that
            // open replays more, so the close still succeeds.
            if let Some(Some(mut boards)) = BOARDS.with_borrow_mut(|boards| boards.remove(&board)) {
                let _ = checkpoint(board, &mut boards, false).await;
            }
            SAVED.with_borrow_mut(|saved| saved.remove(&board));
            Ok(Value::Null)
        }
        Request::Group { board, writes } => {
            let mut boards = take(board)?;
            let verdicts = group(&mut boards, writes).await;
            put(board, boards);
            Ok(Value::Array(verdicts?))
        }
        Request::Open {
            actor,
            partitions,
            scope,
            checkpoint_every,
            retain,
        } => {
            // Each Board.open instantiates its own module, so this setting is this board's.
            checkpoints::checkpoint_every(checkpoint_every.unwrap_or(checkpoints::CHECKPOINT_EVERY));
            open(&scope, &actor, partitions, retain).await
        }
        Request::Opened { board } => BOARDS.with_borrow(|boards| {
            let boards = boards
                .get(&board)
                .ok_or_else(|| unknown(board))?
                .as_ref()
                .ok_or_else(|| busy(board))?;
            Ok(Value::Array(
                boards.iter().map(|board| opened(board.opened())).collect(),
            ))
        }),
        Request::Ready {
            at_ms,
            board,
            label,
        } => read(board, |merged| {
            merged
                .ready(at_ms)
                .into_iter()
                .filter_map(|(_, id)| merged.task(id))
                .filter(|task| {
                    label
                        .as_ref()
                        .is_none_or(|(key, value)| task.spec.labels.get(key) == Some(value))
                })
                .map(|task| view(merged, task, at_ms))
                .collect()
        }),
        Request::Refresh { board } => {
            let mut boards = take(board)?;
            let refreshed = shards::refresh(&mut boards)
                .await
                .map(|()| Value::Null)
                .map_err(|error| error.to_string());
            put(board, boards);
            refreshed
        }
        Request::RenewsPerLease {} => Ok(json!(tasks::RENEWS_PER_LEASE)),
        Request::Steers { board, claim, task } => BOARDS.with_borrow(|boards| {
            let boards = boards
                .get(&board)
                .ok_or_else(|| unknown(board))?
                .as_ref()
                .ok_or_else(|| busy(board))?;
            let merged = Merged::new(boards.iter().map(Board::fold).collect());
            let Some(fold) = merged.home(&task).and_then(|home| merged.fold(home)) else {
                return Ok(Value::Array(Vec::new()));
            };
            let steers: Vec<Value> = match &claim {
                Some(claim) => fold
                    .steers_for(&task, claim)
                    .map_err(|rejection| format!("{rejection:?}"))?
                    .map(steer)
                    .collect(),
                None => fold.steers(&task).map(steer).collect(),
            };
            Ok(Value::Array(steers))
        }),
        Request::Task { at_ms, board, task } => read(board, |merged| {
            merged
                .task(&task)
                .map(|found| view(merged, found, at_ms))
                .into_iter()
                .collect()
        })
        .map(|found| match found {
            Value::Array(mut list) => list.pop().unwrap_or(Value::Null),
            _ => Value::Null,
        }),
        Request::Tasks { at_ms, board } => read(board, |merged| {
            merged
                .tasks()
                .into_values()
                .map(|task| view(merged, task, at_ms))
                .collect()
        }),
        Request::TextLimit { board } => BOARDS.with_borrow(|boards| {
            let boards = boards
                .get(&board)
                .ok_or_else(|| unknown(board))?
                .as_ref()
                .ok_or_else(|| busy(board))?;
            Ok(text_limit(boards))
        }),
        Request::Write { board, write } => {
            let mut boards = take(board)?;
            let verdict = submit(&mut boards, write).await;
            put(board, boards);
            verdict
        }
    }
}

async fn open(
    scope: &str,
    actor: &str,
    partitions: NonZeroU32,
    retain: Option<Retain>,
) -> Result<Value, String> {
    let (principal, profile) = match actor.split_once('+') {
        Some((principal, profile)) => (principal, Some(profile)),
        None => (actor, None),
    };
    let identity = Identity::verify(&Trusted, principal)
        .and_then(|identity| match profile {
            Some(profile) => identity.profile(profile),
            None => Ok(identity),
        })
        .map_err(|denied| format!("{actor}: {denied:?}"))?;
    let migrated = super::store::log_migrated()
        .await
        .map_err(|error| error.to_string())?;
    let host: Arc<dyn LogHost> = Arc::new(SqlLog::new(Arc::new(WorkerStore::log()), &migrated));
    let mut boards = Vec::with_capacity(partitions.get() as usize);
    for index in 0..partitions.get() {
        let fold = retain.clone().map_or_else(Fold::new, Fold::retaining);
        let scope = shards::scope_of(scope, partitions, Partition(index));
        let board = Board::open_into(Arc::clone(&host), &scope, identity.clone(), fold)
            .await
            .map_err(|error| error.to_string())?;
        boards.push(board);
    }
    let saved = boards
        .iter()
        .map(|board| match board.opened() {
            Opened::Checkpoint { .. } => board.fold().next(),
            Opened::Resumed | Opened::Replay(_) => 0,
        })
        .collect();
    let handle = BOARDS.with_borrow_mut(|open| {
        let handle = open.last_key_value().map_or(0, |(last, _)| last + 1);
        open.insert(handle, Some(boards));
        handle
    });
    SAVED.with_borrow_mut(|all| all.insert(handle, saved));
    Ok(json!(handle))
}

/// Saves each partition's fold as its scope's checkpoint; unless forced, only the partitions
/// whose fold moved past the checkpoint this handle opened from or saved last. Returns whether
/// any partition saved one.
async fn checkpoint(handle: u32, boards: &mut [Board], force: bool) -> Result<bool, String> {
    let mut marks = SAVED.with_borrow(|saved| saved.get(&handle).cloned().unwrap_or_default());
    marks.resize(boards.len(), 0);
    let mut any = false;
    let mut failed = None;
    for (board, mark) in boards.iter_mut().zip(&mut marks) {
        let next = board.fold().next();
        if !force && next == *mark {
            continue;
        }
        match board.checkpoint().await {
            Ok(true) => {
                *mark = next;
                any = true;
            }
            Ok(false) => {}
            Err(error) => failed = Some(error.to_string()),
        }
    }
    SAVED.with_borrow_mut(|saved| {
        if let Some(slot) = saved.get_mut(&handle) {
            *slot = marks;
        }
    });
    failed.map_or(Ok(any), Err)
}

fn text_limit(boards: &[Board]) -> Value {
    let chars = boards
        .iter()
        .filter_map(|board| board.fold().text_limit())
        .min();
    match (chars, super::store::value_limit()) {
        (Some(chars), _) => json!({ "by": "board", "chars": chars }),
        (None, Some(bytes)) => json!({ "by": "host", "bytes": bytes }),
        (None, None) => json!({ "by": "none" }),
    }
}

fn opened(opened: &Opened) -> Value {
    match opened {
        Opened::Resumed => json!({ "from": "resumed" }),
        Opened::Checkpoint { seq, tail } => json!({ "from": "checkpoint", "seq": seq, "tail": tail }),
        Opened::Replay(fallback) => {
            let (kind, detail) = match fallback {
                Fallback::Unsupported => ("unsupported", None),
                Fallback::Missing => ("missing", None),
                Fallback::Unread(detail) => ("unread", Some(detail)),
                Fallback::Version(detail) => ("version", Some(detail)),
                Fallback::Corrupt(detail) => ("corrupt", Some(detail)),
                Fallback::Tail => ("tail", None),
            };
            let mut out = json!({ "fallback": kind, "from": "replay" });
            if let (Value::Object(out), Some(detail)) = (&mut out, detail) {
                out.insert("detail".into(), Value::String(detail.clone()));
            }
            out
        }
    }
}

fn take(board: u32) -> Result<Vec<Board>, String> {
    BOARDS.with_borrow_mut(|boards| {
        boards
            .get_mut(&board)
            .ok_or_else(|| unknown(board))?
            .take()
            .ok_or_else(|| busy(board))
    })
}

fn put(board: u32, partitions: Vec<Board>) {
    BOARDS.with_borrow_mut(|boards| {
        if let Some(slot) = boards.get_mut(&board) {
            *slot = Some(partitions);
        }
    });
}

fn read(board: u32, query: impl FnOnce(&Merged<'_>) -> Vec<Value>) -> Result<Value, String> {
    BOARDS.with_borrow(|boards| {
        let boards = boards
            .get(&board)
            .ok_or_else(|| unknown(board))?
            .as_ref()
            .ok_or_else(|| busy(board))?;
        Ok(Value::Array(query(&Merged::new(
            boards.iter().map(Board::fold).collect(),
        ))))
    })
}

#[cold]
fn busy(board: u32) -> String {
    format!("board {board} is busy with another command")
}

#[cold]
fn unknown(board: u32) -> String {
    format!("board {board} is not open")
}

fn route(boards: &[Board], by: Option<&str>, op: Op) -> Result<(Vec<usize>, Op), String> {
    let writer = boards.first().ok_or("the board has no partitions")?.writer();
    let merged = Merged::new(boards.iter().map(Board::fold).collect());
    match merged.place(by.unwrap_or(writer), op) {
        (Route::One(partition), op) => Ok((vec![partition.0 as usize], op)),
        (Route::Every, op) => Ok(((0..boards.len()).collect(), op)),
        (Route::Cross { .. }, _) => Err("a move across partitions is not supported".into()),
        (Route::Refused(reason), _) => Err(reason),
    }
}

fn merge(verdicts: Vec<Value>) -> Value {
    let mut first = Value::Null;
    for verdict in verdicts {
        if verdict.get("ok") == Some(&Value::Bool(false)) {
            return verdict;
        }
        if first.is_null() {
            first = verdict;
        }
    }
    first
}

async fn submit(boards: &mut [Board], write: Write) -> Result<Value, String> {
    let (partitions, op) = route(boards, write.by.as_deref(), write.op)?;
    let mut verdicts = Vec::with_capacity(partitions.len());
    for partition in partitions {
        let board = boards
            .get_mut(partition)
            .ok_or("the write routes to a missing partition")?;
        let op = op.clone();
        let found = keyed(board, write.by.as_deref(), write.key.as_deref());
        let post = matches!(op, Op::Post { .. });
        let applied = match (write.by.clone(), write.key.clone()) {
            (None, Some(key)) => board.submit_keyed(&key, write.at_ms, op).await,
            (None, None) => board.submit(write.at_ms, op).await,
            (by, key) => board
                .submit_group(vec![nanocode_board::Write {
                    at_ms: write.at_ms,
                    by,
                    key,
                    op,
                }])
                .await
                .0
                .pop()
                .unwrap_or_else(|| {
                    Err(BoardError::Dropped("the group returned no verdict".into()))
                }),
        };
        verdicts.push(verdict(board, applied, found, post)?);
    }
    Ok(merge(verdicts))
}

async fn group(boards: &mut [Board], writes: Vec<Write>) -> Result<Vec<Value>, String> {
    let count = writes.len();
    let mut placed: BTreeMap<usize, Vec<(Placed, nanocode_board::Write)>> = BTreeMap::new();
    for (index, write) in writes.into_iter().enumerate() {
        let (partitions, op) = route(boards, write.by.as_deref(), write.op)?;
        let post = matches!(op, Op::Post { .. });
        for partition in partitions {
            let board = boards
                .get(partition)
                .ok_or("the write routes to a missing partition")?;
            let found = keyed(board, write.by.as_deref(), write.key.as_deref());
            placed.entry(partition).or_default().push((
                Placed { found, index, post },
                nanocode_board::Write {
                    at_ms: write.at_ms,
                    by: write.by.clone(),
                    key: write.key.clone(),
                    op: op.clone(),
                },
            ));
        }
    }
    let mut verdicts: Vec<Vec<Value>> = vec![Vec::new(); count];
    for (partition, members) in placed {
        let board = boards
            .get_mut(partition)
            .ok_or("the write routes to a missing partition")?;
        let (places, members): (Vec<Placed>, Vec<nanocode_board::Write>) =
            members.into_iter().unzip();
        let (results, _) = board.submit_group(members).await;
        for (place, result) in places.into_iter().zip(results) {
            if let Some(slot) = verdicts.get_mut(place.index) {
                slot.push(verdict(board, result, place.found, place.post)?);
            }
        }
    }
    Ok(verdicts.into_iter().map(merge).collect())
}

struct Placed {
    found: bool,
    index: usize,
    post: bool,
}

fn keyed(board: &Board, by: Option<&str>, key: Option<&str>) -> bool {
    key.is_some_and(|key| {
        let by = by.unwrap_or_else(|| board.writer());
        board
            .fold()
            .outcome(&EventId(format!("{by}#{key}")))
            .is_some()
    })
}

fn verdict(
    board: &Board,
    applied: Result<Applied, BoardError>,
    found: bool,
    post: bool,
) -> Result<Value, String> {
    match applied {
        Ok(applied) => {
            let change = board
                .fold()
                .entries()
                .iter()
                .rev()
                .find(|entry| entry.event == applied.event)
                .map(|entry| &entry.change);
            Ok(json!({
                "change": change,
                "event": applied.event,
                "found": found || (post && change.is_none()),
                "ok": true,
                "seq": applied.seq,
            }))
        }
        Err(BoardError::Rejected(rejection)) => Ok(json!({
            "ok": false,
            "rejection": rejected(&rejection),
        })),
        Err(BoardError::Row { bytes, max }) => Ok(json!({
            "ok": false,
            "rejection": { "bytes": bytes, "kind": "Row", "max": max },
        })),
        Err(error) => Err(error.to_string()),
    }
}

fn rejected(rejection: &Rejection) -> Value {
    match rejection {
        Rejection::Full {
            cap: Cap::Label { max, key, value },
        } => return json!({ "cap": { "label": [key, value], "max": max }, "kind": "Full" }),
        Rejection::Said(messages::Refusal::Text { chars, max }) => {
            return json!({ "chars": chars, "kind": "Said", "max": max, "reason": "Text" });
        }
        Rejection::Steer(steer::Refusal::Text { chars, max }) => {
            return json!({ "chars": chars, "kind": "Steer", "max": max, "reason": "Text" });
        }
        _ => {}
    }
    match serde_json::to_value(rejection) {
        Ok(Value::String(kind)) => json!({ "kind": kind }),
        Ok(Value::Object(tagged)) => {
            let mut out = Map::new();
            for (kind, fields) in tagged {
                out.insert("kind".into(), Value::String(kind));
                match fields {
                    Value::Object(fields) => {
                        for (name, value) in fields {
                            out.insert(camel(&name), value);
                        }
                    }
                    value => {
                        out.insert("value".into(), value);
                    }
                }
            }
            Value::Object(out)
        }
        Ok(value) => json!({ "kind": "Unknown", "value": value }),
        Err(error) => json!({ "kind": "Unknown", "value": error.to_string() }),
    }
}

fn camel(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut upper = false;
    for character in name.chars() {
        if character == '_' {
            upper = true;
        } else if upper {
            out.extend(character.to_uppercase());
            upper = false;
        } else {
            out.push(character);
        }
    }
    out
}

fn steer(steer: &Steer) -> Value {
    json!({
        "atMs": steer.at_ms,
        "body": steer.body,
        "by": steer.by,
        "seq": steer.seq,
        "to": steer.to,
    })
}

fn view(merged: &Merged<'_>, task: &Task, at_ms: u64) -> Value {
    let mut out = json!({
        "assignee": task.assignee,
        "attempts": task.attempts,
        "dueAtMs": task.due_at_ms,
        "id": task.spec.id,
        "labels": task.spec.labels,
        "needs": task.spec.needs,
        "parent": merged.parent(&task.spec.id),
        "rank": task.rank,
        "replies": task
            .replies
            .iter()
            .map(|(by, body)| json!({ "body": body, "by": by }))
            .collect::<Vec<_>>(),
        "spec": task.spec.spec,
        "stage": task.stage(at_ms).to_string(),
    });
    let phase = match &task.phase {
        Phase::Blocked { kind, reason } => {
            json!({ "blocked": { "kind": kind, "reason": reason }, "phase": "blocked" })
        }
        Phase::Cancelled { reason } => json!({ "phase": "cancelled", "reason": reason }),
        Phase::Claimed {
            claim,
            until_ms,
            worker,
        } => {
            let mut claimed = json!({
                "claim": claim,
                "phase": "claimed",
                "untilMs": until_ms,
                "worker": worker,
            });
            if let (Value::Object(claimed), Some(seq)) =
                (&mut claimed, claim_seq(merged, task, claim))
            {
                claimed.insert("claimSeq".into(), seq.into());
            }
            claimed
        }
        Phase::Done { result, worker } => {
            finished(json!({ "outcome": { "done": result }, "phase": "done" }), worker.as_deref())
        }
        Phase::Failed { error, worker } => finished(
            json!({ "outcome": { "failed": error }, "phase": "failed" }),
            worker.as_deref(),
        ),
        Phase::Open => json!({ "phase": "open" }),
    };
    if let (Value::Object(out), Value::Object(phase)) = (&mut out, phase) {
        out.extend(phase);
    }
    out
}

fn claim_seq(merged: &Merged<'_>, task: &Task, claim: &ClaimId) -> Option<u64> {
    let fold = merged.fold(merged.home(&task.spec.id)?)?;
    fold.outcome(&EventId(claim.0.clone()))?
        .as_ref()
        .ok()
        .copied()
}

fn finished(mut phase: Value, worker: Option<&str>) -> Value {
    if let (Value::Object(phase), Some(worker)) = (&mut phase, worker) {
        phase.insert("worker".into(), worker.into());
    }
    phase
}
