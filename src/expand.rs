//! Spawn-time env-var expansion (post-v0 item c).
//!
//! The gist says paths use `$CONVOY_NET`-style vars "expanded by pty at spawn". Verified empirically:
//! that only holds for pty's OWN pty.toml manifest path. st2 spawns via `pty run` with env passed
//! through the child process env — which pty does **not** shell-expand — so an env VALUE like
//! `ST_ROOT = "$CONVOY_NET/smalltalk"` reaches the child verbatim. The `command` is fine (st2 runs it
//! under `sh -c`, which expands at spawn), but env values, `cwd`, and tag values do NOT pass through a
//! shell. st2 therefore expands those itself, here, against its own ambient environment.
//!
//! Supported forms: `$VAR`, `${VAR}`, and `$$` → literal `$`. An **unset** variable is left as its
//! literal token (`$VAR`) rather than blanked — a spawn-time path with an undefined var is a
//! misconfiguration worth seeing, not one to silently turn into a wrong path like `/smalltalk`.

/// Expand `$VAR` / `${VAR}` / `$$` in `input`, resolving names via `lookup`. Unknown names are left
/// as their literal token.
pub fn expand_vars(input: &str, lookup: impl Fn(&str) -> Option<String>) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();

    while let Some(c) = chars.next() {
        if c != '$' {
            out.push(c);
            continue;
        }
        match chars.peek().copied() {
            Some('$') => {
                chars.next();
                out.push('$'); // $$ → literal $
            }
            Some('{') => {
                chars.next(); // consume '{'
                let mut name = String::new();
                let mut closed = false;
                while let Some(ch) = chars.peek().copied() {
                    chars.next();
                    if ch == '}' {
                        closed = true;
                        break;
                    }
                    name.push(ch);
                }
                match (closed, lookup(&name)) {
                    (true, Some(v)) => out.push_str(&v),
                    (true, None) => {
                        out.push_str("${");
                        out.push_str(&name);
                        out.push('}');
                    }
                    (false, _) => {
                        // Unterminated `${…` → emit literally.
                        out.push_str("${");
                        out.push_str(&name);
                    }
                }
            }
            Some(ch) if ch.is_ascii_alphabetic() || ch == '_' => {
                let mut name = String::new();
                while let Some(ch) = chars.peek().copied() {
                    if ch.is_ascii_alphanumeric() || ch == '_' {
                        name.push(ch);
                        chars.next();
                    } else {
                        break;
                    }
                }
                match lookup(&name) {
                    Some(v) => out.push_str(&v),
                    None => {
                        out.push('$');
                        out.push_str(&name);
                    }
                }
            }
            // `$` not starting a var (e.g. `$/`, or trailing `$`) → literal `$`.
            _ => out.push('$'),
        }
    }
    out
}

/// Expand against st2's ambient process environment (the production lookup).
pub fn expand_env(input: &str) -> String {
    expand_vars(input, |k| std::env::var(k).ok())
}

/// Expand against the ambient env plus `$CATALOG` = the catalog root (spec.md §2 / R11).
pub fn expand_catalog(input: &str, catalog_root: &std::path::Path) -> String {
    let catalog = catalog_root.display().to_string();
    expand_vars(input, move |k| {
        if k == "CATALOG" { Some(catalog.clone()) } else { std::env::var(k).ok() }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn lookup<'a>(map: &'a HashMap<&'a str, &'a str>) -> impl Fn(&str) -> Option<String> + 'a {
        move |k| map.get(k).map(|v| v.to_string())
    }

    #[test]
    fn expands_bare_and_braced_vars() {
        let env = HashMap::from([("CONVOY_NET", "/net/foo")]);
        assert_eq!(expand_vars("$CONVOY_NET/smalltalk", lookup(&env)), "/net/foo/smalltalk");
        assert_eq!(expand_vars("${CONVOY_NET}/pty", lookup(&env)), "/net/foo/pty");
        assert_eq!(expand_vars("pre-${CONVOY_NET}-post", lookup(&env)), "pre-/net/foo-post");
    }

    #[test]
    fn bare_var_stops_at_non_identifier_chars() {
        let env = HashMap::from([("A", "X"), ("B", "Y")]);
        assert_eq!(expand_vars("$A/$B", lookup(&env)), "X/Y");
        assert_eq!(expand_vars("$A.$B", lookup(&env)), "X.Y");
        assert_eq!(expand_vars("$A_B", lookup(&env)), "$A_B"); // A_B is one (unset) name, left literal
    }

    #[test]
    fn unset_vars_are_left_as_literal_tokens() {
        let env: HashMap<&str, &str> = HashMap::new();
        assert_eq!(expand_vars("$CONVOY_NET/x", lookup(&env)), "$CONVOY_NET/x");
        assert_eq!(expand_vars("${MISSING}/x", lookup(&env)), "${MISSING}/x");
    }

    #[test]
    fn dollar_dollar_is_a_literal_dollar() {
        let env: HashMap<&str, &str> = HashMap::new();
        assert_eq!(expand_vars("cost is $$5", lookup(&env)), "cost is $5");
    }

    #[test]
    fn lone_or_trailing_dollar_is_literal() {
        let env: HashMap<&str, &str> = HashMap::new();
        assert_eq!(expand_vars("a$/b", lookup(&env)), "a$/b");
        assert_eq!(expand_vars("trailing$", lookup(&env)), "trailing$");
        assert_eq!(expand_vars("unterminated ${FOO", lookup(&env)), "unterminated ${FOO");
    }

    #[test]
    fn no_dollars_is_unchanged() {
        let env = HashMap::from([("A", "X")]);
        assert_eq!(expand_vars("/plain/path", lookup(&env)), "/plain/path");
    }
}
