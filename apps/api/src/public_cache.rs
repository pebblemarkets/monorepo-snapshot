use std::{
    collections::HashMap,
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};

use tokio::sync::{Mutex, Notify};

use crate::ApiError;

#[derive(Debug)]
struct CacheEntry<T> {
    value: T,
    expires_at: Instant,
    last_accessed_at: Instant,
}

#[derive(Clone)]
pub(crate) struct CoalescingCache<T> {
    entries: Arc<Mutex<HashMap<String, CacheEntry<T>>>>,
    in_flight: Arc<Mutex<HashMap<String, Arc<Notify>>>>,
    max_entries: usize,
}

impl<T> CoalescingCache<T>
where
    T: Clone + Send + Sync + 'static,
{
    pub(crate) fn new(max_entries: usize) -> Self {
        Self {
            entries: Arc::new(Mutex::new(HashMap::new())),
            in_flight: Arc::new(Mutex::new(HashMap::new())),
            max_entries: max_entries.max(1),
        }
    }

    pub(crate) async fn get_or_load<F, Fut>(
        &self,
        cache_key: String,
        ttl: Duration,
        loader: F,
    ) -> Result<T, ApiError>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<T, ApiError>> + Send,
    {
        let mut loader = Some(loader);

        loop {
            if let Some(cached) = self.get_if_fresh(&cache_key).await {
                return Ok(cached);
            }

            let maybe_waiter = {
                let mut in_flight = self.in_flight.lock().await;
                if let Some(notify) = in_flight.get(&cache_key) {
                    Some(Arc::clone(notify))
                } else {
                    in_flight.insert(cache_key.clone(), Arc::new(Notify::new()));
                    None
                }
            };

            if let Some(waiter) = maybe_waiter {
                waiter.notified().await;
                continue;
            }

            let load = loader
                .take()
                .ok_or_else(|| ApiError::internal("cache loader unavailable"))?;
            let loaded = load().await;

            let notify = {
                let mut in_flight = self.in_flight.lock().await;
                in_flight.remove(&cache_key)
            };
            if let Some(notify) = notify {
                notify.notify_waiters();
            }

            match loaded {
                Ok(value) => {
                    self.insert(cache_key.clone(), value.clone(), ttl).await;
                    return Ok(value);
                }
                Err(err) => {
                    return Err(err);
                }
            }
        }
    }

    async fn get_if_fresh(&self, cache_key: &str) -> Option<T> {
        let now = Instant::now();
        let mut entries = self.entries.lock().await;

        match entries.get_mut(cache_key) {
            Some(entry) if entry.expires_at > now => {
                entry.last_accessed_at = now;
                Some(entry.value.clone())
            }
            Some(_) => {
                entries.remove(cache_key);
                None
            }
            None => None,
        }
    }

    async fn insert(&self, cache_key: String, value: T, ttl: Duration) {
        let now = Instant::now();
        let expires_at = now.checked_add(ttl).unwrap_or(now);
        let mut entries = self.entries.lock().await;
        entries.retain(|_, entry| entry.expires_at > now);

        while entries.len() >= self.max_entries {
            let oldest_key = entries
                .iter()
                .min_by_key(|(_, entry)| entry.last_accessed_at)
                .map(|(key, _)| key.clone());
            let Some(oldest_key) = oldest_key else {
                break;
            };
            let _ = entries.remove(&oldest_key);
        }

        entries.insert(
            cache_key,
            CacheEntry {
                value,
                expires_at,
                last_accessed_at: now,
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::CoalescingCache;
    use crate::ApiError;
    use std::{
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        time::Duration,
    };
    use tokio::sync::Barrier;

    #[tokio::test]
    async fn get_or_load_reuses_fresh_cache_entry() {
        let cache = CoalescingCache::new(8);
        let calls = AtomicUsize::new(0);

        let first = cache
            .get_or_load("k".to_string(), Duration::from_secs(1), || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(7_u64)
            })
            .await
            .expect("first load should succeed");
        let second = cache
            .get_or_load("k".to_string(), Duration::from_secs(1), || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(9_u64)
            })
            .await
            .expect("second load should succeed");

        assert_eq!(first, 7_u64);
        assert_eq!(second, 7_u64);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn get_or_load_coalesces_concurrent_requests() {
        let cache = Arc::new(CoalescingCache::new(8));
        let calls = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(3));

        let first_task = {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                cache
                    .get_or_load(
                        "shared".to_string(),
                        Duration::from_secs(1),
                        || async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            Ok(11_u64)
                        },
                    )
                    .await
            })
        };

        let second_task = {
            let cache = Arc::clone(&cache);
            let calls = Arc::clone(&calls);
            let barrier = Arc::clone(&barrier);
            tokio::spawn(async move {
                barrier.wait().await;
                cache
                    .get_or_load(
                        "shared".to_string(),
                        Duration::from_secs(1),
                        || async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            tokio::time::sleep(Duration::from_millis(50)).await;
                            Ok(11_u64)
                        },
                    )
                    .await
            })
        };

        barrier.wait().await;

        let first = first_task.await.expect("first task should join");
        let second = second_task.await.expect("second task should join");
        assert_eq!(first.expect("first result should succeed"), 11_u64);
        assert_eq!(second.expect("second result should succeed"), 11_u64);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn get_or_load_does_not_cache_errors() {
        let cache = CoalescingCache::new(8);
        let calls = AtomicUsize::new(0);

        let first = cache
            .get_or_load("err".to_string(), Duration::from_secs(1), || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(ApiError::internal("boom"))
            })
            .await;
        assert!(first.is_err());

        let second = cache
            .get_or_load("err".to_string(), Duration::from_secs(1), || async {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(5_u64)
            })
            .await
            .expect("second load should succeed");

        assert_eq!(second, 5_u64);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
