//! HTTP Basic authentication with three coarse roles.
//!
//! * `admin`    - everything
//! * `write`    - read + register/delete schemas + subject-level config/mode
//! * `readonly` - GET requests plus the side-effect-free POSTs
//!                (schema lookup and compatibility tests)
//!
//! A role can be held registry-wide or bound to subject patterns; which roles
//! apply to a given request, and how listings are filtered, is [`crate::authz`].
//!
//! Passwords may be stored as bcrypt hashes (`$2a$`/`$2b$`/`$2y$`, produced by
//! `schema-registry hash-password`) or plaintext. bcrypt is deliberately slow
//! (~100ms), and serializers can hit the registry often, so successful
//! verifications are cached in memory keyed by SHA-256(user:password).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

use axum::extract::{Request, State};
use axum::http::{HeaderValue, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::Engine;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;


use crate::authz::Principal;
use crate::config::AuthConfig;
use crate::error::ApiError;

pub struct Auth {
    enabled: bool,
    realm: String,
    users: HashMap<String, (String, Arc<Principal>)>,
    verified: RwLock<HashSet<[u8; 32]>>,
}

impl Auth {
    pub fn new(cfg: &AuthConfig) -> Self {
        let users =
            cfg.users.iter().map(|u| (u.username.clone(), (u.password.clone(), Arc::new(Principal::from_user(u))))).collect();
        Self { enabled: cfg.enabled, realm: cfg.realm.clone(), users, verified: RwLock::new(HashSet::new()) }
    }

    pub fn disabled() -> Self {
        Self { enabled: false, realm: String::new(), users: HashMap::new(), verified: RwLock::new(HashSet::new()) }
    }

    /// Fast path: credentials already verified (no bcrypt). `None` = unknown, not "invalid".
    fn cached(&self, header_value: &str) -> Option<Arc<Principal>> {
        let (user, password) = decode_basic(header_value)?;
        let (stored, principal) = self.users.get(&user)?;
        let key = cache_key(&user, &password, stored);
        self.verified.read().ok()?.contains(&key).then(|| principal.clone())
    }

    /// Returns what the caller may do, if the credentials are valid.
    fn authenticate(&self, header_value: &str) -> Option<Arc<Principal>> {
        let (user, password) = decode_basic(header_value)?;
        let (stored, principal) = self.users.get(&user)?;
        let cache_key = cache_key(&user, &password, stored);
        if self.verified.read().map(|s| s.contains(&cache_key)).unwrap_or(false) {
            return Some(principal.clone());
        }
        let ok = if stored.starts_with("$2a$") || stored.starts_with("$2b$") || stored.starts_with("$2y$") {
            bcrypt::verify(&password, stored).unwrap_or(false)
        } else {
            bool::from(stored.as_bytes().ct_eq(password.as_bytes()))
        };
        if ok {
            if let Ok(mut s) = self.verified.write() {
                if s.len() > 10_000 {
                    s.clear();
                }
                s.insert(cache_key);
            }
            Some(principal.clone())
        } else {
            None
        }
    }
}

fn decode_basic(header_value: &str) -> Option<(String, String)> {
    let encoded = header_value.strip_prefix("Basic ").or_else(|| header_value.strip_prefix("basic "))?;
    let decoded = base64::engine::general_purpose::STANDARD.decode(encoded.trim()).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (user, password) = decoded.split_once(':')?;
    Some((user.to_string(), password.to_string()))
}

fn cache_key(user: &str, password: &str, stored: &str) -> [u8; 32] {
    Sha256::digest(format!("{user}\0{password}\0{stored}").as_bytes()).into()
}

pub async fn middleware(State(shared): State<crate::api::Shared>, mut req: Request, next: Next) -> Response {
    let auth = &shared.auth;
    if !auth.enabled {
        return next.run(req).await;
    }
    let creds = req.headers().get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).map(String::from);
    let Some(creds) = creds else { return challenge(auth) };
    let principal = match auth.cached(&creds) {
        Some(p) => Some(p),
        None => {
            // First sight of these credentials: bcrypt is CPU-heavy, keep it off the async workers.
            let auth2 = shared.auth.clone();
            tokio::task::spawn_blocking(move || auth2.authenticate(&creds)).await.ok().flatten()
        }
    };
    let Some(principal) = principal else { return challenge(auth) };
    // Which container this request reached was decided before authentication;
    // a user who is not bound to it gets nothing, whatever the Host said.
    if let Some(reg) = req.extensions().get::<std::sync::Arc<crate::registry::Registry>>()
        && !principal.may_use(reg.container())
    {
        return ApiError::forbidden("User is denied operation on this resource").into_response();
    }
    if !crate::authz::authorized(&principal, req.method(), req.uri().path()) {
        return ApiError::forbidden("User is denied operation on this resource").into_response();
    }
    // Handlers filter what they return to what this caller may see.
    req.extensions_mut().insert(principal);
    next.run(req).await
}

fn challenge(auth: &Auth) -> Response {
    let mut resp = ApiError::unauthorized().into_response();
    if let Ok(v) = HeaderValue::from_str(&format!("Basic realm=\"{}\"", auth.realm)) {
        resp.headers_mut().insert(header::WWW_AUTHENTICATE, v);
    }
    resp
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verifies_plain_and_bcrypt() {
        let cfg = AuthConfig {
            enabled: true,
            realm: "r".into(),
            users: vec![
                crate::config::UserConfig {
                    username: "a".into(),
                    password: "pw".into(),
                    roles: vec![crate::config::Role::Admin],
                    bindings: vec![],
                    containers: vec![],
                },
                crate::config::UserConfig {
                    username: "b".into(),
                    password: bcrypt::hash("secret", 4).unwrap(),
                    roles: vec![crate::config::Role::Readonly],
                    bindings: vec![],
                    containers: vec![],
                },
            ],
        };
        let auth = Auth::new(&cfg);
        let h = |u: &str, p: &str| format!("Basic {}", base64::engine::general_purpose::STANDARD.encode(format!("{u}:{p}")));
        assert!(auth.authenticate(&h("a", "pw")).is_some());
        assert!(auth.authenticate(&h("a", "nope")).is_none());
        assert!(auth.authenticate(&h("b", "secret")).is_some());
        assert!(auth.authenticate(&h("b", "secret")).is_some()); // cached path
        assert!(auth.authenticate(&h("c", "x")).is_none());
    }
}
