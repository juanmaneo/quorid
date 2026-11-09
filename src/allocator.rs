use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::oneshot;

use crate::backend::Backend;
use crate::command::{Command, Outcome};
use crate::error::{Error, Result};
use crate::id::{Id, IdRange, Namespace};

#[derive(Clone, Debug, Serialize)]
pub struct AllocatorConfig {
    /// IDs leased from the backend per round trip.
    pub block_size: u64,
    /// When fewer IDs than this remain in the current block, the next block is
    /// leased in the background so requests rarely wait on the backend.
    pub low_watermark: u64,
}

impl AllocatorConfig {
    pub fn with_block_size(block_size: u64) -> Self {
        AllocatorConfig {
            block_size,
            low_watermark: block_size / 5,
        }
    }
}

impl Default for AllocatorConfig {
    fn default() -> Self {
        Self::with_block_size(1000)
    }
}

#[derive(Default)]
struct Segment {
    current: IdRange,
    /// In-flight (or completed) background lease of the next block. Requests
    /// wait on it instead of leasing their own block, so blocks are consumed
    /// in the order the backend granted them and IDs stay increasing.
    next: Option<oneshot::Receiver<Result<IdRange>>>,
}

/// Serves IDs from blocks leased from a [`Backend`] (segment / Hi-Lo scheme).
///
/// IDs from one allocator are strictly increasing per namespace. IDs from
/// different allocators (servers) interleave by block. IDs still unused in a
/// leased block when the process stops are never handed out.
pub struct Allocator<B> {
    backend: Arc<B>,
    config: AllocatorConfig,
    segments: Mutex<HashMap<Namespace, Arc<AsyncMutex<Segment>>>>,
}

impl<B: Backend> Allocator<B> {
    pub fn new(backend: Arc<B>, config: AllocatorConfig) -> Self {
        assert!(config.block_size > 0, "block_size must be at least 1");
        Allocator {
            backend,
            config,
            segments: Mutex::new(HashMap::new()),
        }
    }

    pub fn backend(&self) -> &Arc<B> {
        &self.backend
    }

    pub fn config(&self) -> &AllocatorConfig {
        &self.config
    }

    /// Reserves a raw block of `size` IDs directly from the backend, for
    /// clients that run their own local allocation.
    pub async fn lease(&self, namespace: &Namespace, size: u64) -> Result<IdRange> {
        lease(&*self.backend, namespace, size).await
    }

    /// Like [`Allocator::lease`], for a request that another node forwarded
    /// here: a node that cannot reserve the block itself fails with
    /// [`Error::NotLeader`] instead of forwarding it once more, so a request
    /// never bounces between nodes that disagree on who leads.
    pub async fn lease_here(&self, namespace: &Namespace, size: u64) -> Result<IdRange> {
        let outcome = self.backend.execute_here(reserve(namespace, size)).await?;
        Ok(reserved(outcome))
    }

    pub async fn next_id(&self, namespace: &Namespace) -> Result<Id> {
        let ranges = self.allocate(namespace, 1).await?;
        Ok(ranges[0].start)
    }

    /// Allocates `count` IDs, returned as increasing, non-overlapping ranges
    /// (usually one, two when crossing a block boundary).
    pub async fn allocate(&self, namespace: &Namespace, count: u64) -> Result<Vec<IdRange>> {
        if count == 0 {
            return Err(Error::InvalidRequest("count must be at least 1".into()));
        }
        if count > self.config.block_size {
            return Ok(vec![self.lease(namespace, count).await?]);
        }

        let segment = self.segment(namespace);
        let mut seg = segment.lock().await;
        let mut out: Vec<IdRange> = Vec::with_capacity(2);
        let mut needed = count as u128;
        while needed > 0 {
            if seg.current.is_empty() {
                seg.current = self.next_block(namespace, &mut seg).await?;
            }
            let taken = seg.current.take_front(needed);
            needed -= taken.len();
            match out.last_mut() {
                Some(last) if last.end == taken.start => last.end = taken.end,
                _ => out.push(taken),
            }
        }

        if seg.current.len() < self.config.low_watermark as u128 && seg.next.is_none() {
            seg.next = Some(self.spawn_prefetch(namespace.clone()));
        }
        Ok(out)
    }

    async fn next_block(&self, namespace: &Namespace, seg: &mut Segment) -> Result<IdRange> {
        if let Some(prefetch) = seg.next.take() {
            match prefetch.await {
                Ok(Ok(block)) => return Ok(block),
                Ok(Err(e)) => {
                    tracing::warn!(%namespace, error = %e, "background block lease failed, retrying")
                }
                Err(_) => {
                    tracing::warn!(%namespace, "background block lease was dropped, retrying")
                }
            }
        }
        self.lease(namespace, self.config.block_size).await
    }

    fn segment(&self, namespace: &Namespace) -> Arc<AsyncMutex<Segment>> {
        self.segments
            .lock()
            .unwrap()
            .entry(namespace.clone())
            .or_default()
            .clone()
    }

    fn spawn_prefetch(&self, namespace: Namespace) -> oneshot::Receiver<Result<IdRange>> {
        let (tx, rx) = oneshot::channel();
        let backend = self.backend.clone();
        let size = self.config.block_size;
        tokio::spawn(async move {
            let _ = tx.send(lease(&*backend, &namespace, size).await);
        });
        rx
    }
}

async fn lease<B: Backend>(backend: &B, namespace: &Namespace, count: u64) -> Result<IdRange> {
    let outcome = backend.execute(reserve(namespace, count)).await?;
    Ok(reserved(outcome))
}

fn reserve(namespace: &Namespace, count: u64) -> Command {
    Command::Reserve {
        namespace: namespace.clone(),
        count,
    }
}

fn reserved(outcome: Outcome) -> IdRange {
    match outcome {
        Outcome::Reserved(range) => range,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::backend::{BackendStatus, MemoryBackend};

    fn ns(s: &str) -> Namespace {
        Namespace::new(s).unwrap()
    }

    #[derive(Clone, Copy)]
    enum Fault {
        Error,
        Panic,
    }

    /// Counts round trips to the wrapped backend, optionally failing some.
    #[derive(Default)]
    struct Counting {
        inner: MemoryBackend,
        calls: AtomicUsize,
        /// Faults to inject, keyed by 1-based call number.
        faults: Mutex<HashMap<usize, Fault>>,
    }

    impl Counting {
        fn failing_on(call: usize, fault: Fault) -> Self {
            let backend = Counting::default();
            backend.faults.lock().unwrap().insert(call, fault);
            backend
        }

        fn and_on(self, call: usize, fault: Fault) -> Self {
            self.faults.lock().unwrap().insert(call, fault);
            self
        }
    }

    impl Backend for Counting {
        async fn execute(&self, command: Command) -> Result<Outcome> {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            let fault = self.faults.lock().unwrap().get(&call).copied();
            match fault {
                Some(Fault::Error) => Err(Error::Unavailable("injected".into())),
                Some(Fault::Panic) => panic!("injected backend panic"),
                None => self.inner.execute(command).await,
            }
        }
        fn status(&self) -> BackendStatus {
            self.inner.status()
        }
    }

    #[tokio::test]
    async fn serves_from_blocks() {
        let backend = Arc::new(Counting::default());
        let alloc = Allocator::new(backend.clone(), AllocatorConfig::with_block_size(10));
        let users = ns("users");
        assert_eq!(
            alloc.allocate(&users, 4).await.unwrap(),
            [IdRange::new(1, 5)]
        );
        assert_eq!(
            alloc.allocate(&users, 4).await.unwrap(),
            [IdRange::new(5, 9)]
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
        // Crosses into the next block; contiguous blocks merge into one range.
        let ranges = alloc.allocate(&users, 4).await.unwrap();
        let ids: Vec<Id> = ranges.iter().flat_map(IdRange::iter).collect();
        assert_eq!(ids, [9, 10, 11, 12]);
        assert_eq!(alloc.next_id(&ns("orders")).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn large_requests_bypass_blocks() {
        let backend = Arc::new(MemoryBackend::new());
        let alloc = Allocator::new(backend, AllocatorConfig::with_block_size(10));
        let users = ns("users");
        assert_eq!(
            alloc.allocate(&users, 1).await.unwrap(),
            [IdRange::new(1, 2)]
        );
        assert_eq!(
            alloc.allocate(&users, 50).await.unwrap(),
            [IdRange::new(11, 61)]
        );
        assert_eq!(
            alloc.allocate(&users, 1).await.unwrap(),
            [IdRange::new(2, 3)]
        );
        assert!(alloc.allocate(&users, 0).await.is_err());
    }

    #[test]
    fn default_config() {
        let config = AllocatorConfig::default();
        assert_eq!(config.block_size, 1000);
        assert_eq!(config.low_watermark, 200);
    }

    #[tokio::test]
    async fn exposes_backend() {
        let backend = Arc::new(Counting::default());
        let alloc = Allocator::new(backend.clone(), AllocatorConfig::default());
        assert!(Arc::ptr_eq(alloc.backend(), &backend));
        assert_eq!(alloc.backend().status().kind, "memory");
    }

    /// A failed or lost background lease is retried in the foreground, without
    /// skipping or repeating IDs.
    async fn recovers_from_prefetch(fault: Fault) {
        // Call 1 leases the first block; call 2 is the prefetch.
        let backend = Arc::new(Counting::failing_on(2, fault));
        let alloc = Allocator::new(backend.clone(), AllocatorConfig::with_block_size(10));
        let users = ns("users");
        assert_eq!(
            alloc.allocate(&users, 9).await.unwrap(),
            [IdRange::new(1, 10)]
        );
        assert_eq!(
            alloc.allocate(&users, 3).await.unwrap(),
            [IdRange::new(10, 13)]
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn backend_errors_reach_callers() {
        let backend = Arc::new(Counting::failing_on(1, Fault::Error).and_on(2, Fault::Error));
        let alloc = Allocator::new(backend.clone(), AllocatorConfig::with_block_size(10));
        let users = ns("users");
        // Leasing a block for the segment fails...
        let err = alloc.next_id(&users).await.unwrap_err();
        assert_eq!(err.to_string(), "backend unavailable: injected");
        // ...and so does a direct lease for a large request.
        assert!(alloc.allocate(&users, 50).await.is_err());
        // Invalid requests never reach the backend.
        assert!(alloc.allocate(&users, 0).await.is_err());
        // Nothing was consumed by the failures.
        assert_eq!(alloc.next_id(&users).await.unwrap(), 1);
        assert_eq!(backend.calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retries_failed_prefetch() {
        recovers_from_prefetch(Fault::Error).await;
    }

    #[tokio::test]
    async fn retries_dropped_prefetch() {
        recovers_from_prefetch(Fault::Panic).await;
    }

    #[tokio::test]
    async fn prefetches_next_block() {
        let backend = Arc::new(Counting::default());
        let alloc = Allocator::new(backend.clone(), AllocatorConfig::with_block_size(10));
        alloc.allocate(&ns("users"), 9).await.unwrap();
        for _ in 0..100 {
            if backend.calls.load(Ordering::SeqCst) == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            backend.calls.load(Ordering::SeqCst),
            2,
            "prefetch should have run"
        );
        assert_eq!(
            alloc.allocate(&ns("users"), 3).await.unwrap(),
            [IdRange::new(10, 13)]
        );
        assert_eq!(backend.calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_allocations_are_unique() {
        let backend = Arc::new(MemoryBackend::new());
        let alloc = Arc::new(Allocator::new(
            backend,
            AllocatorConfig::with_block_size(64),
        ));
        let tasks: Vec<_> = (0..32)
            .map(|i| {
                let alloc = alloc.clone();
                tokio::spawn(async move {
                    let mut ids = Vec::new();
                    for _ in 0..200 {
                        let ranges = alloc.allocate(&ns("users"), 1 + i % 5).await.unwrap();
                        ids.extend(ranges.iter().flat_map(IdRange::iter));
                    }
                    ids
                })
            })
            .collect();
        let mut all = HashSet::new();
        let mut total = 0;
        for task in tasks {
            let ids = task.await.unwrap();
            assert!(
                ids.windows(2).all(|w| w[0] < w[1]),
                "per-caller ids must increase"
            );
            total += ids.len();
            all.extend(ids);
        }
        assert_eq!(all.len(), total, "duplicate ids handed out");
        assert!(!all.contains(&0));
    }
}
