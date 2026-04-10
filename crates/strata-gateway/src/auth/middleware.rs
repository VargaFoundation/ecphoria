//! Authentication middleware — Tower layer for request authentication.

use std::collections::HashSet;
use std::sync::Arc;

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::Response;

/// Authentication context injected into request extensions.
#[derive(Debug, Clone)]
pub struct AuthContext {
    pub identity: String,
    pub role: Role,
}

/// User roles for RBAC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Admin,
    Writer,
    Reader,
    Agent,
}

/// Shared set of valid API keys.
#[derive(Debug, Clone)]
pub struct ApiKeyStore {
    keys: Arc<HashSet<String>>,
}

impl ApiKeyStore {
    pub fn new(keys: Vec<String>) -> Self {
        Self {
            keys: Arc::new(keys.into_iter().collect()),
        }
    }

    pub fn validate(&self, key: &str) -> bool {
        self.keys.contains(key)
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }
}

/// Axum middleware that validates API keys from the `Authorization: Bearer <key>` header.
///
/// If the key is valid, an `AuthContext` is injected into request extensions.
/// If invalid, returns 401 Unauthorized.
pub async fn require_auth(
    axum::extract::State(store): axum::extract::State<ApiKeyStore>,
    mut req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let auth_header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok());

    let key = match auth_header {
        Some(header) if header.starts_with("Bearer ") => &header[7..],
        _ => return Err(StatusCode::UNAUTHORIZED),
    };

    if !store.validate(key) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    // Inject auth context for downstream handlers
    req.extensions_mut().insert(AuthContext {
        identity: "api-key-user".into(),
        role: Role::Writer,
    });

    Ok(next.run(req).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn role_equality() {
        assert_eq!(Role::Admin, Role::Admin);
        assert_ne!(Role::Admin, Role::Reader);
        assert_ne!(Role::Writer, Role::Agent);
    }

    #[test]
    fn auth_context_clone() {
        let ctx = AuthContext {
            identity: "user-1".into(),
            role: Role::Admin,
        };
        let cloned = ctx.clone();
        assert_eq!(cloned.identity, "user-1");
        assert_eq!(cloned.role, Role::Admin);
    }

    #[test]
    fn auth_context_debug() {
        let ctx = AuthContext {
            identity: "agent-bot".into(),
            role: Role::Agent,
        };
        let debug = format!("{:?}", ctx);
        assert!(debug.contains("agent-bot"));
        assert!(debug.contains("Agent"));
    }

    #[test]
    fn all_roles_are_distinct() {
        let roles = [Role::Admin, Role::Writer, Role::Reader, Role::Agent];
        for (i, a) in roles.iter().enumerate() {
            for (j, b) in roles.iter().enumerate() {
                if i == j {
                    assert_eq!(a, b);
                } else {
                    assert_ne!(a, b);
                }
            }
        }
    }

    #[test]
    fn api_key_store_validates() {
        let store = ApiKeyStore::new(vec!["secret-123".into(), "key-456".into()]);
        assert!(store.validate("secret-123"));
        assert!(store.validate("key-456"));
        assert!(!store.validate("invalid"));
        assert!(!store.validate(""));
    }

    #[test]
    fn api_key_store_empty() {
        let store = ApiKeyStore::new(vec![]);
        assert!(store.is_empty());
        assert!(!store.validate("anything"));
    }
}
