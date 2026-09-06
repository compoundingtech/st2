use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

#[test]
fn root_requirement_ids_are_unique() {
    let requirements = include_str!("../docs/vrs/requirements.md");
    let mut seen = BTreeSet::new();

    for line in requirements.lines() {
        let Some(rest) = line.strip_prefix("- **R") else {
            continue;
        };
        let digits = rest.bytes().take_while(u8::is_ascii_digit).count();
        if digits == 0 || !rest[digits..].starts_with(' ') {
            continue;
        }
        let number = rest[..digits]
            .parse::<u32>()
            .expect("requirement ID digits must parse");
        assert!(
            seen.insert(number),
            "duplicate root requirement ID R{number}"
        );
    }
}

#[test]
fn root_decision_number_duplicates_match_recorded_history() {
    let decision_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/vrs/.decisions");
    let mut by_number = BTreeMap::<String, BTreeSet<String>>::new();

    for entry in fs::read_dir(decision_dir).expect("root decision directory must be readable") {
        let path = entry.expect("decision entry must be readable").path();
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let Some((number, _)) = stem.split_once('-') else {
            continue;
        };
        if number.len() == 4 && number.bytes().all(|byte| byte.is_ascii_digit()) {
            by_number
                .entry(number.to_owned())
                .or_default()
                .insert(stem.to_owned());
        }
    }

    let duplicates = by_number
        .into_iter()
        .filter(|(_, stems)| stems.len() > 1)
        .collect::<BTreeMap<_, _>>();
    let expected = [
        (
            "0005",
            [
                "0005-pi-delivers-natively-through-an-injected-extension",
                "0005-streams-are-agent-nested-and-stream-named",
            ]
            .as_slice(),
        ),
        (
            "0007",
            [
                "0007-child-output-capture-is-bounded-and-tail-preserving",
                "0007-omp-is-a-fifth-native-driver-with-its-own-channel-and-a-hard-version-gate",
            ]
            .as_slice(),
        ),
        (
            "0014",
            [
                "0014-harness-context-is-a-sibling-numeric-record",
                "0014-resource-profiles-are-state-first-read-and-observe-capabilities",
            ]
            .as_slice(),
        ),
        (
            "0015",
            [
                "0015-catalog-commits-and-direct-edits-use-independent-wake-channels",
                "0015-immutable-agent-id-and-mutable-address",
            ]
            .as_slice(),
        ),
    ]
    .into_iter()
    .map(|(number, stems)| {
        (
            number.to_owned(),
            stems.iter().map(|stem| (*stem).to_owned()).collect(),
        )
    })
    .collect::<BTreeMap<_, _>>();

    assert_eq!(duplicates, expected);
}

#[test]
fn root_delta_ids_are_unique() {
    let delta_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("docs/vrs/.delta");
    let mut seen = BTreeSet::new();

    for entry in fs::read_dir(delta_dir).expect("root delta directory must be readable") {
        let path = entry.expect("delta entry must be readable").path();
        let Some(stem) = path.file_stem().and_then(|stem| stem.to_str()) else {
            continue;
        };
        let Some(rest) = stem.strip_prefix("DELTA-") else {
            continue;
        };
        let Some((number, _)) = rest.split_once('-') else {
            continue;
        };
        if number.len() == 3 && number.bytes().all(|byte| byte.is_ascii_digit()) {
            assert!(
                seen.insert(number.to_owned()),
                "duplicate root delta ID DELTA-{number}"
            );
        }
    }
}
