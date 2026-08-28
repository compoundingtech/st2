use std::fs;
use std::path::PathBuf;

use agent_spec::spec::Driver;

fn authored_harness_counts(source: &str, source_name: &str) -> (usize, usize) {
    fn visit(document: &kdl::KdlDocument, source_name: &str, counts: &mut (usize, usize)) {
        for node in document.nodes() {
            if node.name().value() == "agent" {
                let children = node.children().expect("an agent must have children");
                assert!(
                    children.get("command").is_none(),
                    "{source_name} has a model agent with a raw command"
                );
                let harnesses = children
                    .nodes()
                    .iter()
                    .filter(|child| child.name().value() == "harness")
                    .collect::<Vec<_>>();
                assert_eq!(
                    harnesses.len(),
                    1,
                    "{source_name} agents must declare one native harness"
                );
                let provider = harnesses[0]
                    .entries()
                    .first()
                    .and_then(|entry| entry.value().as_string())
                    .expect("a harness must name its provider");
                match provider {
                    "claude" => counts.0 += 1,
                    "codex" => counts.1 += 1,
                    provider => panic!("{source_name} uses unexpected harness {provider}"),
                }
            }
            if let Some(children) = node.children() {
                visit(children, source_name, counts);
            }
        }
    }

    let document: kdl::KdlDocument = source.parse().expect("parse harness inventory KDL");
    let mut counts = (0, 0);
    visit(&document, source_name, &mut counts);
    counts
}

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
        .join("evals/st3");
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
fn every_selected_eval_has_one_st2_and_one_st3_form() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("evals");
    for name in ["license-mit", "ghost-bug", "signal-rename"] {
        let st2_file = root.join("st2").join(name).join("eval.kdl");
        let st3_file = root.join("st3").join(name).join("eval.kdl");
        let st2_source = fs::read_to_string(&st2_file).expect("read st2 eval");
        let st3_source = fs::read_to_string(&st3_file).expect("read st3 eval");
        let st2_document: kdl::KdlDocument = st2_source.parse().expect("parse st2 KDL");
        let st3_document: kdl::KdlDocument = st3_source.parse().expect("parse st3 KDL");

        assert_eq!(
            st2::kdl_version::document_version(&st2_document).unwrap(),
            1
        );
        assert_eq!(
            st2::kdl_version::document_version(&st3_document).unwrap(),
            2
        );
        let st2_spec = st2::eval_spec::parse_spec(&st2_source)
            .unwrap_or_else(|error| panic!("{}: {error:#}", st2_file.display()));
        st3::parse_intent(&st3_source, "local")
            .unwrap_or_else(|error| panic!("{}: {error}", st3_file.display()));
        for agent in st2_spec.agents.iter().chain(
            st2_spec
                .eval
                .iter()
                .flat_map(|evaluation| evaluation.agents.iter()),
        ) {
            assert!(
                agent.command.is_none() && agent.driver.is_some(),
                "{} agent {} must use a native harness",
                st2_file.display(),
                agent.id
            );
        }
        assert!(
            authored_harness_counts(&st3_source, &st3_file.display().to_string()) != (0, 0),
            "{} must contain native agent harnesses",
            st3_file.display()
        );
        assert!(st2::eval_spec::parse_spec(&st3_source).is_err());
        assert!(st3::parse_intent(&st2_source, "local").is_err());
    }
}

#[test]
fn selected_eval_harness_counts_match_the_inventory() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("evals");
    let expected = [
        ("license-mit", (2, 1), (0, 2)),
        ("ghost-bug", (0, 2), (0, 2)),
        ("signal-rename", (0, 4), (0, 4)),
    ];

    for (name, st2_expected, st3_expected) in expected {
        let st2_source = fs::read_to_string(root.join("st2").join(name).join("eval.kdl")).unwrap();
        let st2_spec = st2::eval_spec::parse_spec(&st2_source).unwrap();
        let mut st2_counts = (0, 0);
        for agent in st2_spec.agents.iter().chain(
            st2_spec
                .eval
                .iter()
                .flat_map(|evaluation| evaluation.agents.iter()),
        ) {
            match agent.driver.as_ref().expect("native st2 harness") {
                Driver::Claude(driver) => {
                    assert_eq!(
                        driver.model.as_deref(),
                        Some("claude-sonnet-5"),
                        "st2 {name} Claude seats must use Sonnet"
                    );
                    st2_counts.0 += 1;
                }
                Driver::Codex(_) => st2_counts.1 += 1,
                driver => panic!("unexpected st2 harness {}", driver.name()),
            }
        }
        assert_eq!(st2_counts, st2_expected, "st2 {name}");

        let st3_source = fs::read_to_string(root.join("st3").join(name).join("eval.kdl")).unwrap();
        let st3_counts = authored_harness_counts(&st3_source, &format!("st3 {name}"));
        assert_eq!(st3_counts, st3_expected, "st3 {name}");
    }
}

#[test]
fn signal_rename_keeps_work_structure_in_the_plan_graph() {
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("evals/st3/signal-rename/eval.kdl");
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
