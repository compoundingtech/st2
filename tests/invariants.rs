use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

fn declared_tests(source: &str) -> BTreeSet<String> {
    let mut tests = BTreeSet::new();
    let mut saw_test_attribute = false;

    for line in source.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("#[") {
            if trimmed.contains("test") {
                saw_test_attribute = true;
            }
            continue;
        }
        if !saw_test_attribute || trimmed.is_empty() {
            continue;
        }

        let declaration = trimmed.strip_prefix("pub ").unwrap_or(trimmed);
        let declaration = declaration.strip_prefix("async ").unwrap_or(declaration);
        if let Some(rest) = declaration.strip_prefix("fn ")
            && let Some((name, _)) = rest.split_once('(')
        {
            tests.insert(name.trim().to_owned());
        }
        saw_test_attribute = false;
    }

    tests
}

#[test]
fn qualified_proof_references_resolve() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let invariants = include_str!("../INVARIANTS.md");
    let mut checked = 0;

    for (index, span) in invariants.split('`').enumerate() {
        if index % 2 == 0 {
            continue;
        }
        let Some((relative_path, test_name)) = span.rsplit_once("::") else {
            if span.ends_with(".rs") {
                assert!(
                    root.join(span).is_file(),
                    "invariant proof source `{span}` does not exist"
                );
            }
            continue;
        };
        if !relative_path.ends_with(".rs") {
            continue;
        }

        let source = fs::read_to_string(root.join(relative_path))
            .unwrap_or_else(|error| panic!("proof source {relative_path} is unreadable: {error}"));
        let tests = declared_tests(&source);
        assert!(
            tests.contains(test_name),
            "invariant proof `{span}` does not name a declared test"
        );
        checked += 1;
    }

    assert!(
        checked >= 20,
        "expected a substantial qualified proof set, found only {checked}"
    );
}
