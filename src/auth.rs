//! HTTP Basic authentication with three coarse roles.
//!
//! * `admin`    - everything
//! * `write`    - read + register/delete schemas + subject-level config/mode
//! * `readonly` - GET requests plus the side-effect-free POSTs
//!                (schema lookup and compatibility tests)
//!
//! Passwords may be stored as bcrypt hashes (`$2a$`/`$2b$`/`$2y$`, produced by
//! `schema-registry hash-password`) or plaintext. bcrypt is deliberately slow
//! (~100ms), and serializers can hit the registry often, so successful
//! verifications are cached in memory keyed by SHA-256(user:password).

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use axum::extract::{Request, State};
use axum::http::{HeaderValue, Method, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::Engine;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::api::AppState;
use crate::config::{AuthConfig, Role};
use crate::error::ApiError;

pub struct Auth {
    enabled: bool,
    realm: String,
    users: HashMap<String, (String, Vec<Role>)>,
    verified: RwLock<HashSet<[u8; 32]>>,
}

impl Auth {
    pub fn new(cfg: &AuthConfig) -> Self {
        let users = cfg.users.iter().map(|u| (u.username.clone(), (u.password.clone(), u.roles.clone()))).collect();
        Self { enabled: cfg.enabled, realm: cfg.realm.clone(), users, verified: RwLock::new(HashSet::new()) }
    }

    pub fn disabled() -> Self {
        Self { enabled: false, realm: String::new(), users: HashMap::new(), verified: RwLock::new(HashSet::new()) }
    }

    /// Fast path: credentials already verified (no bcrypt). `None` = unknown, not "invalid".
    fn cached(&self, header_value: &str) -> Option<&[Role]> {
        let (user, password) = decode_basic(header_value)?;
        let (stored, roles) = self.users.get(&user)?;
        let key = cache_key(&user, &password, stored);
        self.verified.read().ok()?.contains(&key).then_some(roles.as_slice())
    }

    /// Returns the user's roles if the credentials are valid.
    fn authenticate(&self, header_value: &str) -> Option<&[Role]> {
        let (user, password) = decode_basic(header_value)?;
        let (stored, roles) = self.users.get(&user)?;
        let cache_key = cache_key(&user, &password, stored);
        if self.verified.read().map(|s| s.contains(&cache_key)).unwrap_or(false) {
            return Some(roles);
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
            Some(roles)
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

/// Is this request allowed for the given roles?
fn authorized(roles: &[Role], method: &Method, path: &str) -> bool {
    if roles.contains(&Role::Admin) {
        return true;
    }
    // The admin UI shows every subject, schema and setting at once: admins only.
    if path.starts_with("/_admin") {
        return false;
    }
    let read_only_request = matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS)
        || (*method == Method::POST
            && (path.starts_with("/compatibility/")
                || (path.starts_with("/subjects/") && !path.ends_with("/versions"))));
    if read_only_request {
        return !roles.is_empty();
    }
    if roles.contains(&Role::Write) {
        // Writers manage schemas and subject-scoped settings, not global ones or exporters.
        // `/config/{subject}` is subject-scoped; `/config` (global) and `/config/:.ctx:` (context) are not.
        let subject_scoped = (path.starts_with("/config/") || path.starts_with("/mode/")) && !path.ends_with(':');
        return path.starts_with("/subjects/") || subject_scoped;
    }
    false
}

pub async fn middleware(State(st): State<AppState>, req: Request, next: Next) -> Response {
    let auth = &st.auth;
    if !auth.enabled {
        return next.run(req).await;
    }
    let creds = req.headers().get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()).map(String::from);
    let Some(creds) = creds else { return challenge(auth) };
    let roles = match auth.cached(&creds) {
        Some(r) => Some(r.to_vec()),
        None => {
            // First sight of these credentials: bcrypt is CPU-heavy, keep it off the async workers.
            let auth2 = st.auth.clone();
            tokio::task::spawn_blocking(move || auth2.authenticate(&creds).map(|r| r.to_vec())).await.ok().flatten()
        }
    };
    let Some(roles) = roles else { return challenge(auth) };
    if !authorized(&roles, req.method(), req.uri().path()) {
        return ApiError::forbidden("User is denied operation on this resource").into_response();
    }
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
    fn role_rules() {
        let ro = [Role::Readonly];
        let w = [Role::Write];
        assert!(authorized(&ro, &Method::GET, "/subjects"));
        // The admin UI is for admins, whatever the method.
        assert!(!authorized(&ro, &Method::GET, "/_admin"));
        assert!(!authorized(&w, &Method::GET, "/_admin/api/overview"));
        assert!(authorized(&[Role::Admin], &Method::GET, "/_admin"));
        assert!(authorized(&ro, &Method::POST, "/subjects/foo"));
        assert!(authorized(&ro, &Method::POST, "/compatibility/subjects/foo/versions/latest"));
        assert!(!authorized(&ro, &Method::POST, "/subjects/foo/versions"));
        assert!(authorized(&w, &Method::POST, "/subjects/foo/versions"));
        assert!(authorized(&w, &Method::PUT, "/config/foo"));
        assert!(!authorized(&w, &Method::PUT, "/config"));
        assert!(!authorized(&w, &Method::POST, "/exporters"));
        assert!(authorized(&[Role::Admin], &Method::POST, "/exporters"));
    }

    #[test]
    fn verifies_plain_and_bcrypt() {
        let cfg = AuthConfig {
            enabled: true,
            realm: "r".into(),
            users: vec![
                crate::config::UserConfig { username: "a".into(), password: "pw".into(), roles: vec![Role::Admin] },
                crate::config::UserConfig {
                    username: "b".into(),
                    password: bcrypt::hash("secret", 4).unwrap(),
                    roles: vec![Role::Readonly],
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
