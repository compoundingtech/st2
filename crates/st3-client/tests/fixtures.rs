use st3_client::*;
use std::path::{Path, PathBuf};

fn fixture(name: &str) -> Vec<u8> {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/st3/client-v0/fixtures")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn decode<T: serde::de::DeserializeOwned>(name: &str) -> T {
    serde_json::from_slice(&fixture(name)).unwrap_or_else(|error| panic!("decode {name}: {error}"))
}

#[test]
fn generated_models_decode_every_stream_fixture() {
    let capabilities: Envelope<Capabilities> = decode("capabilities.json");
    assert_eq!(capabilities.value.limits.max_page_items, 200);
    let events: Envelope<EventPage> = decode("events.json");
    assert_eq!(events.value.items.len(), 2);
    let timeline: Envelope<TimelinePage> = decode("timeline.json");
    assert_eq!(timeline.value.items.len(), 10);
    let screen: Envelope<TerminalScreen> = decode("terminal-screen.json");
    assert_eq!(screen.value.lines.len(), 3);
    let frames: Envelope<TerminalFramePage> = decode("terminal-frames.json");
    assert_eq!(frames.value.frames.len(), 2);
    let pairing: Envelope<PairedSession> = decode("pairing.json");
    assert!(pairing.value.credential.len() >= 32);
}

#[test]
fn generated_resource_union_decodes_all_kinds() {
    let resources: Vec<Resource> = serde_json::from_slice(&fixture("resources.json")).unwrap();
    assert_eq!(resources.len(), 13);
    assert!(
        resources
            .iter()
            .all(|resource| resource.header().id.contains('/'))
    );
    let launch = resources
        .iter()
        .find_map(|resource| match resource {
            Resource::Launch(launch) => Some(launch),
            _ => None,
        })
        .unwrap();
    let visualization = launch.visualization.as_ref().unwrap();
    assert_eq!(visualization.nodes[0].goals, ["Build artifacts"]);
    assert_eq!(
        visualization.decisions[0].decision_type,
        DecisionType::SingleChoice
    );
    assert_eq!(visualization.diffs[0].changes.len(), 1);
    assert_eq!(visualization.swimlanes[0].nodes, ["step/build"]);
}

#[test]
fn generated_action_union_and_machine_manifest_stay_in_lockstep() {
    let action: ActionRequest = decode("action.json");
    assert_eq!(action.action_type(), &ActionType::WorkComplete);
    assert_eq!(ACTION_NAMES.len(), 30);
    assert_eq!(READ_OPERATIONS.len(), 27);
    assert_eq!(CONTRACT_SHA256.len(), 64);
}

#[test]
fn crate_exposes_both_transport_constructors() {
    let _ = Client::unix(Path::new("/tmp/st3.sock"));
    let _ = Client::fabric_loopback("http://127.0.0.1:8787", "credential");
}

#[test]
fn authority_error_codes_are_typed_and_round_trip() {
    for (raw, expected) in [
        ("runtime-not-local", ErrorCode::RuntimeNotLocal),
        (
            "runtime-authority-indeterminate",
            ErrorCode::RuntimeAuthorityIndeterminate,
        ),
    ] {
        let decoded: ErrorCode = serde_json::from_str(&format!("\"{raw}\"")).unwrap();
        assert_eq!(decoded, expected);
        assert_eq!(
            serde_json::to_string(&decoded).unwrap(),
            format!("\"{raw}\"")
        );
    }
}
