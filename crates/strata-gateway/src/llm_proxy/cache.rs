//! Semantic response cache — caches LLM responses keyed by prompt similarity.
//!
//! Uses a simple in-memory hash map with exact-match keys (normalized prompts).
//! For production, this would use a USearch index for approximate matching.

use dashmap::DashMap;
use std::time::{Duration, Instant};

/// Cached LLM response.
struct CachedResponse {
    response: String,
    created_at: Instant,
}

/// Cache for LLM responses keyed by normalized prompt text.
pub struct SemanticCache {
    entries: DashMap<String, CachedResponse>,
    ttl: Duration,
    max_entries: usize,
}

impl SemanticCache {
    /// Create a new cache with the given TTL and max entries.
    pub fn new() -> Self {
        Self {
            entries: DashMap::new(),
            ttl: Duration::from_secs(3600), // 1 hour default
            max_entries: 10_000,
        }
    }

    /// Create with custom TTL.
    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            entries: DashMap::new(),
            ttl,
            max_entries: 10_000,
        }
    }

    /// Look up a cached response by prompt text.
    /// Returns None on cache miss or expired entry.
    pub async fn get(&self, query: &str) -> Option<String> {
        let key = normalize_key(query);

        if let Some(entry) = self.entries.get(&key) {
            if entry.created_at.elapsed() < self.ttl {
                return Some(entry.response.clone());
            }
            // Expired — remove it
            drop(entry);
            self.entries.remove(&key);
        }

        None
    }

    /// Store a response in the cache.
    pub async fn put(&self, query: &str, response: &str) {
        // Evict oldest entries if at capacity
        if self.entries.len() >= self.max_entries {
            self.evict_oldest();
        }

        let key = normalize_key(query);
        self.entries.insert(
            key,
            CachedResponse {
                response: response.to_string(),
                created_at: Instant::now(),
            },
        );
    }

    /// Number of cached entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the cache is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Remove expired entries.
    pub fn evict_expired(&self) {
        self.entries
            .retain(|_, v| v.created_at.elapsed() < self.ttl);
    }

    fn evict_oldest(&self) {
        // Simple eviction: remove entries that are past 75% of TTL
        let threshold = self.ttl * 3 / 4;
        self.entries
            .retain(|_, v| v.created_at.elapsed() < threshold);
    }
}

impl Default for SemanticCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Normalize a prompt for cache key matching.
/// Lowercases, trims whitespace, collapses multiple spaces.
fn normalize_key(query: &str) -> String {
    query
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cache_miss() {
        let cache = SemanticCache::new();
        assert!(cache.get("hello").await.is_none());
    }

    #[tokio::test]
    async fn cache_hit() {
        let cache = SemanticCache::new();
        cache.put("hello", "world").await;
        assert_eq!(cache.get("hello").await.unwrap(), "world");
    }

    #[tokio::test]
    async fn cache_normalized_key() {
        let cache = SemanticCache::new();
        cache.put("Hello  World", "response").await;
        // Different whitespace/case should match
        assert_eq!(cache.get("hello world").await.unwrap(), "response");
    }

    #[tokio::test]
    async fn cache_expiry() {
        let cache = SemanticCache::with_ttl(Duration::from_millis(50));
        cache.put("key", "value").await;
        assert!(cache.get("key").await.is_some());

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(cache.get("key").await.is_none());
    }

    #[tokio::test]
    async fn cache_overwrite() {
        let cache = SemanticCache::new();
        cache.put("key", "v1").await;
        cache.put("key", "v2").await;
        assert_eq!(cache.get("key").await.unwrap(), "v2");
    }

    #[tokio::test]
    async fn cache_len() {
        let cache = SemanticCache::new();
        assert!(cache.is_empty());
        cache.put("a", "1").await;
        cache.put("b", "2").await;
        assert_eq!(cache.len(), 2);
    }

    #[tokio::test]
    async fn evict_expired() {
        let cache = SemanticCache::with_ttl(Duration::from_millis(50));
        cache.put("old", "stale").await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        cache.put("new", "fresh").await;

        cache.evict_expired();
        assert_eq!(cache.len(), 1);
        assert!(cache.get("new").await.is_some());
    }

    #[test]
    fn normalize_key_fn() {
        assert_eq!(normalize_key("Hello  World"), "hello world");
        assert_eq!(normalize_key("  a  b  c  "), "a b c");
        assert_eq!(normalize_key("ABC"), "abc");
    }
}
