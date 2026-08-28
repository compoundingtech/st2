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

#[test]
fn every_native_st3_eval_uses_the_normative_grammar() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("evals");
    let mut files = walkdir::WalkDir::new(&root)
        .max_depth(2)
        .follow_links(false)
        .into_iter()
        .collect::<Result<Vec<_>, _>>()
        .expect("walk evals")
        .into_iter()
        .filter(|entry| entry.file_name() == "eval.kdl")
        .map(|entry| entry.into_path())
        .collect::<Vec<_>>();
    files.sort();
    assert!(!files.is_empty());
    for file in files {
        let source = fs::read_to_string(&file).expect("read eval");
        st3::parse_intent(&source, "local")
            .unwrap_or_else(|error| panic!("{}: {error}", file.display()));
    }
}

#[test]
fn signal_rename_keeps_work_structure_in_the_plan_graph() {
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("evals/signal-rename-codex/eval.kdl");
    let source = fs::read_to_string(&file).expect("read Signal Rename eval");
    assert!(!source.contains("wait-team-done"));
    assert!(!source.contains("message \"kickoff"));

    let intent = st3::parse_intent(&source, "local").expect("parse Signal Rename eval");
    let plan = &intent.plans["eval/signal-rename-codex"];
    assert_eq!(
        plan.display_order,
        [
            "materialize",
            "start-team",
            "open-base-compatibility",
            "migrate-relay",
            "migrate-hub",
            "update-root-and-config",
            "close-base-compatibility",
            "integrate-and-verify",
            "held-out-judges",
            "publish-final-report",
            "cleanup",
        ]
    );

    let expected_assignments = [
        ("open-base-compatibility", "agent/sig.base"),
        ("migrate-relay", "agent/sig.relay"),
        ("migrate-hub", "agent/sig.hub"),
        ("update-root-and-config", "agent/sig.sup"),
        ("close-base-compatibility", "agent/sig.base"),
        ("integrate-and-verify", "agent/sig.sup"),
        ("publish-final-report", "agent/sig.sup"),
    ];
    for (step, assignee) in expected_assignments {
        let step = &plan.steps[step];
        assert_eq!(step.assigned_to.as_deref(), Some(assignee));
        assert!(step.nested_plan.is_some());
    }

    let dependencies = |step: &str| {
        plan.steps[step]
            .dependencies
            .iter()
            .filter_map(|dependency| match dependency {
                st3::model::DependencySpec::Step { step, state } if state == "completed" => {
                    Some(step.as_str())
                }
                _ => None,
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(dependencies("open-base-compatibility"), ["start-team"]);
    assert_eq!(dependencies("migrate-relay"), ["open-base-compatibility"]);
    assert_eq!(dependencies("migrate-hub"), ["open-base-compatibility"]);
    assert_eq!(
        dependencies("update-root-and-config"),
        ["migrate-relay", "migrate-hub"]
    );
    assert_eq!(
        dependencies("close-base-compatibility"),
        ["update-root-and-config"]
    );
    assert_eq!(
        dependencies("integrate-and-verify"),
        ["close-base-compatibility"]
    );
    assert_eq!(dependencies("held-out-judges"), ["integrate-and-verify"]);
    assert_eq!(dependencies("publish-final-report"), ["held-out-judges"]);

    let required_products = [
        (
            "open-base-compatibility",
            "resource/plan-run/${PLAN_RUN}/base-compatibility",
        ),
        (
            "migrate-relay",
            "resource/plan-run/${PLAN_RUN}/relay-revision",
        ),
        ("migrate-hub", "resource/plan-run/${PLAN_RUN}/hub-revision"),
        (
            "update-root-and-config",
            "resource/plan-run/${PLAN_RUN}/config-revision",
        ),
        (
            "close-base-compatibility",
            "resource/plan-run/${PLAN_RUN}/base-final-revision",
        ),
        (
            "integrate-and-verify",
            "resource/plan-run/${PLAN_RUN}/integrated-revision",
        ),
        (
            "publish-final-report",
            "resource/plan-run/${PLAN_RUN}/final-report",
        ),
    ];
    for (parent, product) in required_products {
        let nested = plan.steps[parent]
            .nested_plan
            .as_ref()
            .expect("nested plan");
        assert!(
            nested
                .steps
                .values()
                .flat_map(|step| &step.products)
                .any(|candidate| candidate.subject == product)
        );
    }
}
