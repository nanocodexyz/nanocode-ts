use std::{cell::RefCell, num::NonZeroU32, sync::Arc};

use nanocode::{
    durability::{StoreError, StoreFuture},
    scope::{
        Checkpoints, Committed, Fence, Leases, Listing, Manifest, Policy, Route, ScopeError,
        Wiring, Written,
    },
    turnlog::{
        FinishedFilter, LogHost, SqlLog, ThreadCommand, ThreadFold, ThreadLogs, ThreadPath,
        ThreadPaths, ThreadPolicy, ThreadReply, parted,
    },
};
use nanocode_partitions::{Partition, Row};

use super::{bridge::monotonic_ms, store};

const NODE: &str = "nanocode-ts";

thread_local! {
    static THREADS: RefCell<Option<Arc<ThreadLogs>>> = const { RefCell::new(None) };
}

pub async fn threads() -> Result<Arc<ThreadLogs>, StoreError> {
    if let Some(threads) = THREADS.with_borrow(Clone::clone) {
        return Ok(threads);
    }
    let host: Arc<dyn LogHost> = Arc::new(SqlLog::new(
        Arc::new(store::WorkerStore::log()),
        &store::log_migrated().await?,
    ));
    let threads = Arc::new(
        ThreadLogs::new(Wiring {
            fold: Arc::new(ThreadFold::new(ThreadPolicy {
                part_rows: 1,
                finished_max: None,
                row_bytes_max: store::value_limit(),
                recent_rows: 1,
                events_ahead: 0,
                finished_filter: FinishedFilter {
                    bytes: 1,
                    hashes: 1,
                },
            })),
            log: host,
            leases: Arc::new(Unleased),
            checkpoints: Arc::new(Uncheckpointed),
            route: Arc::new(Unrouted),
            policy: Policy {
                checkpoint_rows: 1,
                checkpoint_ms: u64::MAX,
                resident_bytes_max: 0,
                forward_ms: 0,
                admit_ms: u64::MAX,
            },
            clock: Arc::new(|| monotonic_ms() as u64),
            claimed: None,
        })
        .with_paths(Arc::new(Legacy)),
    );
    Ok(THREADS.with_borrow_mut(|cached| Arc::clone(cached.get_or_insert(threads))))
}

struct Legacy;

impl ThreadPaths for Legacy {
    fn path(&self, _thread: &str) -> ThreadPath {
        ThreadPath::Legacy
    }
}

struct Uncheckpointed;

impl Checkpoints for Uncheckpointed {
    fn latest<'a>(
        &'a self,
        _scope: &'a str,
    ) -> StoreFuture<'a, Result<Option<Manifest>, StoreError>> {
        Box::pin(std::future::ready(Ok(None)))
    }

    fn parts<'a>(
        &'a self,
        _digests: &'a [String],
    ) -> StoreFuture<'a, Result<Vec<(String, String)>, StoreError>> {
        Box::pin(std::future::ready(Ok(Vec::new())))
    }

    fn write<'a>(
        &'a self,
        _fence: &'a Fence,
        _manifest: &'a Manifest,
        _listing: &'a Listing,
        _parts: &'a [(String, String)],
    ) -> StoreFuture<'a, Result<Written, StoreError>> {
        Box::pin(std::future::ready(Ok(Written::Stale)))
    }
}

struct Unleased;

impl Leases for Unleased {
    fn node(&self) -> &str {
        NODE
    }

    fn partition(&self, scope: &str) -> Partition {
        parted::place(scope, NonZeroU32::MIN)
    }

    fn held(&self, _part: Partition) -> Option<u64> {
        None
    }

    fn row(&self, _part: Partition) -> StoreFuture<'_, Result<Row<String>, StoreError>> {
        Box::pin(std::future::ready(Ok(Row::Released {
            epoch: 0,
            cursor: 0,
        })))
    }

    fn claim(&self, _part: Partition) -> StoreFuture<'_, Result<u64, StoreError>> {
        Box::pin(std::future::ready(Err(StoreError::Fenced)))
    }

    fn release(&self, _part: Partition) -> StoreFuture<'_, Result<(), StoreError>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn renew(&self) -> StoreFuture<'_, Result<Vec<Partition>, StoreError>> {
        Box::pin(std::future::ready(Ok(Vec::new())))
    }
}

struct Unrouted;

impl Route<ThreadFold> for Unrouted {
    fn forward<'a>(
        &'a self,
        holder: &'a str,
        _scope: &'a str,
        _command: ThreadCommand,
    ) -> StoreFuture<'a, Result<Committed<ThreadReply>, ScopeError>> {
        Box::pin(std::future::ready(Err(ScopeError::Routed {
            holder: holder.to_owned(),
        })))
    }
}
