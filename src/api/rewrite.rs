//! `/contexts/{ctx}/...` URL form.
//!
//! Confluent accepts both `/subjects/:.ctx:foo/versions` and
//! `/contexts/.ctx/subjects/foo/versions`. Rather than duplicating every route,
//! we rewrite the second form into the first before routing (axum's
//! `Router::layer` middleware runs after routing, so this wraps the router).


/// `QualifiedSubject.normalizeContext`: `""` for the default context, else `:.ctx:`.
fn normalize_context(ctx: &str) -> Result<String, String> {
    let c = ctx.strip_prefix(':').unwrap_or(ctx);
    let c = c.strip_suffix(':').unwrap_or(c);
    if c.contains(':') {
        return Err("Context name cannot contain a colon".into());
    }
    let c = if c.starts_with('.') { c.to_string() } else { format!(".{c}") };
    Ok(if c == "." { String::new() } else { format!(":{c}:") })
}

fn qualified(s: &str) -> bool {
    s.starts_with(":.") || s.starts_with(":*:")
}

/// Port of Confluent's `ContextFilter`: `/contexts/{ctx}/...` becomes the
/// context-qualified form of the same request. `path` has no leading slash.
/// Returns the new path (no leading slash) and the context, or an error for a
/// context name containing a colon.
fn context_filter_path(path: &str) -> Result<(String, String), String> {
    let mut context_path_found = false;
    let mut context = ".".to_string();
    let mut config_or_mode_found = false;
    let mut subject_path_found = false;
    let mut out = String::new();
    let mut is_first = true;
    // Java's String#split drops trailing empty strings.
    let mut segs: Vec<&str> = path.split('/').collect();
    while segs.len() > 1 && segs.last() == Some(&"") {
        segs.pop();
    }
    for seg in segs {
        if context_path_found {
            context = seg.to_string();
            context_path_found = false;
            continue;
        }
        if seg == "contexts" {
            context_path_found = true;
            continue;
        }
        let mut modified = seg.to_string();
        if subject_path_found {
            if !qualified(seg) {
                modified = format!("{}{seg}", normalize_context(&context)?);
            }
            subject_path_found = false;
        }
        let root_config_or_mode = is_first && (seg == "config" || seg == "mode");
        if seg == "subjects" || seg == "deks" || root_config_or_mode {
            subject_path_found = true;
            if root_config_or_mode {
                config_or_mode_found = true;
            }
        }
        out.push_str(&modified);
        out.push('/');
        if is_first && !seg.is_empty() {
            is_first = false;
        }
    }
    if config_or_mode_found && subject_path_found {
        let nc = normalize_context(&context)?;
        if !nc.is_empty() {
            out.push_str(&nc);
            out.push('/');
        }
    } else if context_path_found {
        out.push_str("contexts/");
    }
    Ok((out, context))
}

/// The query parameters `ContextFilter` rewrites.
fn context_filter_query(path: &str, context: &str, pairs: &mut Vec<(String, String)>) -> Result<(), String> {
    let p = path.trim_end_matches('/').trim_start_matches('/');
    let prefix = normalize_context(context)?;
    if p.starts_with("schemas/ids") {
        let subject = pairs.iter().find(|(k, _)| k == "subject").map(|(_, v)| v.clone()).unwrap_or_default();
        if !qualified(&subject) {
            set_param(pairs, "subject", &[format!("{prefix}{subject}")]);
        }
    } else if p == "schemas" || p == "subjects" || p.starts_with("keks") {
        let mut prefixes: Vec<String> = pairs.iter().filter(|(k, _)| k == "subjectPrefix").map(|(_, v)| v.clone()).collect();
        if prefixes.is_empty() {
            prefixes.push(String::new());
        }
        let prefixes: Vec<String> =
            prefixes.into_iter().map(|x| if qualified(&x) { x } else { format!("{prefix}{x}") }).collect();
        set_param(pairs, "subjectPrefix", &prefixes);
    }
    Ok(())
}

fn set_param(pairs: &mut Vec<(String, String)>, name: &str, values: &[String]) {
    pairs.retain(|(k, _)| k != name);
    pairs.extend(values.iter().map(|v| (name.to_string(), v.clone())));
}

/// Port of Confluent's `AliasFilter`: the path segment after `subjects`, and
/// the `subject` parameter of `/schemas/ids/...`, are replaced by the
/// subject's configured alias. `alias_of` gets the decoded subject.
fn alias_filter(path: &str, pairs: &mut Vec<(String, String)>, alias_of: &dyn Fn(&str) -> Option<String>) -> String {
    let replace = |raw: &str, decoded: &str| -> Option<String> {
        if raw.is_empty() {
            return None;
        }
        let alias = alias_of(decoded).filter(|a| !a.is_empty())?;
        // qualifySubjectWithParent: an unqualified alias lives in the parent's
        // context. (The parent is the raw, still-encoded segment, as in Jersey.)
        let a = crate::context::QualifiedSubject::parse(&alias).ok()?;
        let parent = crate::context::QualifiedSubject::parse(raw).ok()?;
        let target = if a.context == "." && parent.context != "." {
            crate::context::QualifiedSubject::new(&parent.context, &alias)
        } else {
            a
        };
        Some(target.qualified())
    };
    let mut out = String::new();
    let mut subject_path_found = false;
    for seg in path.split('/').filter(|s| !s.is_empty()) {
        out.push('/');
        let mut m = seg.to_string();
        if subject_path_found {
            if let Some(a) = replace(seg, &super::percent_decode(seg)) {
                m = encode_segment(&a);
            }
            subject_path_found = false;
        }
        if seg == "subjects" || seg == "deks" {
            subject_path_found = true;
        }
        out.push_str(&m);
    }
    if out.is_empty() {
        out.push('/');
    }
    let p = path.trim_matches('/');
    if p.starts_with("schemas/ids") {
        let subject = pairs.iter().find(|(k, _)| k == "subject").map(|(_, v)| v.clone()).unwrap_or_default();
        if let Some(a) = replace(&subject, &subject) {
            set_param(pairs, "subject", &[a]);
        }
    }
    out
}

fn encode_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~:*@!$&'()+,;=".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

fn encode_query_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~:*".contains(&b) {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Everything Confluent's pre-matching filters do to a request URI, in their
/// order: `ContextFilter` (priority 4000), then `AliasFilter` (5100).
pub fn prematch(path: &str, query: Option<&str>, is_delete_context: bool, alias_of: &dyn Fn(&str) -> Option<String>) -> Result<String, String> {
    let path = clean_path(path).unwrap_or_else(|| path.to_string());
    let mut pairs: Vec<(String, String)> = query.map(super::query_pairs).unwrap_or_default();
    let raw_query = query.map(String::from);
    let mut changed_query = false;
    let mut path = path;
    let bare = path.trim_start_matches('/').to_string();
    // `DELETE /contexts/{ctx}` is ours (not Confluent's); leave it alone.
    if bare.starts_with("contexts/") && !is_delete_context {
        let (p, ctx) = context_filter_path(&bare)?;
        context_filter_query(&p, &ctx, &mut pairs)?;
        path = format!("/{p}");
        changed_query = true;
    }
    let before = pairs.clone();
    let new_path = alias_filter(&path, &mut pairs, alias_of);
    changed_query |= before != pairs;
    let query = if changed_query {
        let q: Vec<String> = pairs.iter().map(|(k, v)| format!("{}={}", encode_query_value(k), encode_query_value(v))).collect();
        (!q.is_empty()).then(|| q.join("&"))
    } else {
        raw_query
    };
    Ok(match query {
        Some(q) => format!("{new_path}?{q}"),
        None => new_path,
    })
}

/// Jersey ignores empty path segments: `/subjects/`, `/subjects/a//versions`
/// and `/subjects/a/versions/1/` all route like their clean forms.
fn clean_path(path: &str) -> Option<String> {
    if !path.contains("//") && (path.len() <= 1 || !path.ends_with('/')) {
        return None;
    }
    let segs: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    Some(format!("/{}", segs.join("/")))
}

#[cfg(test)]
mod tests {

    #[test]
    fn context_filter() {
        let none = |_: &str| None;
        let pm = |p: &str, q: Option<&str>| super::prematch(p, q, false, &none).unwrap();
        assert_eq!(pm("/contexts/.dev/subjects/foo/versions", None), "/subjects/:.dev:foo/versions");
        assert_eq!(pm("/contexts/dev/subjects", Some("deleted=true")), "/subjects?deleted=true&subjectPrefix=:.dev:");
        assert_eq!(pm("/contexts/.dev/schemas/ids/3", None), "/schemas/ids/3?subject=:.dev:");
        assert_eq!(pm("/contexts/.dev/config", None), "/config/:.dev:");
        assert_eq!(pm("/contexts/.dev/config/s", None), "/config/:.dev:s");
        assert_eq!(pm("/contexts/.dev/subjects/:.other:s/versions", None), "/subjects/:.other:s/versions");
        assert!(super::prematch("/contexts/a:b/subjects", None, false, &none).is_err());
    }

    #[test]
    fn alias_filter() {
        let alias = |s: &str| (s == "a").then(|| "t".to_string());
        let pm = |p: &str, q: Option<&str>| super::prematch(p, q, false, &alias).unwrap();
        assert_eq!(pm("/subjects/a/versions", None), "/subjects/t/versions");
        assert_eq!(pm("/compatibility/subjects/a/versions/1", None), "/compatibility/subjects/t/versions/1");
        assert_eq!(pm("/schemas/ids/1", Some("subject=a")), "/schemas/ids/1?subject=t");
        assert_eq!(pm("/config/a", None), "/config/a");
    }

    #[test]
    fn empty_segments_are_ignored() {
        use super::clean_path;
        assert_eq!(clean_path("/subjects/").as_deref(), Some("/subjects"));
        assert_eq!(clean_path("/subjects/a//versions").as_deref(), Some("/subjects/a/versions"));
        assert_eq!(clean_path("/subjects/a/versions/1/").as_deref(), Some("/subjects/a/versions/1"));
        assert_eq!(clean_path("/"), None);
        assert_eq!(clean_path("/subjects"), None);
    }
}
