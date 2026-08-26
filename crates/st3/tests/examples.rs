use std::fs;
use std::path::PathBuf;

#[test]
fn every_tracked_st3_example_uses_the_normative_grammar() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("examples/st3");
    let mut files = walkdir::WalkDir::new(&root)
        .follow_links(false)
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("walk examples")
        .into_iter()
        .filter(|entry| entry.path().extension().and_then(|value| value.to_str()) == Some("kdl"))
        .map(|entry| entry.into_path())
        .collect::<Vec<_>>();
    files.sort();
    assert!(!files.is_empty());
    for file in files {
        let source = fs::read_to_string(&file).expect("read example");
        st3::parse_intent(&source, "local")
            .unwrap_or_else(|error| panic!("{}: {error}", file.display()));
    }
}
