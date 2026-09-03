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
                    "claude" => {
                        let model = harnesses[0]
                            .children()
                            .and_then(|body| body.get("model"))
                            .and_then(|model| model.entries().first())
                            .and_then(|entry| entry.value().as_string());
                        assert_eq!(
                            model,
                            Some("claude-sonnet-5"),
                            "{source_name} Claude seats must use Sonnet"
                        );
                        counts.0 += 1;
                    }
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

fn authored_model_gates(source: &str) -> Vec<String> {
    fn visit(document: &kdl::KdlDocument, models: &mut Vec<String>) {
        for node in document.nodes() {
            let is_llm_gate = node.name().value() == "gate"
                && node.entries().iter().any(|entry| {
                    entry.name().map(|name| name.value()) == Some("type")
                        && entry.value().as_string() == Some("llm")
                });
            if is_llm_gate {
                let model = node
                    .children()
                    .and_then(|body| body.get("model"))
                    .and_then(|model| model.entries().first())
                    .and_then(|entry| entry.value().as_string())
                    .expect("a model gate must declare its model");
                models.push(model.to_owned());
            }
            if let Some(children) = node.children() {
                visit(children, models);
            }
        }
    }

    let document: kdl::KdlDocument = source.parse().expect("parse model gate KDL");
    let mut models = Vec::new();
    visit(&document, &mut models);
    models
}

#[test]
fn planning_mode_eval_uses_one_dynamic_codex_planner() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("evals/st3/planning-mode");
    let source = fs::read_to_string(root.join("eval.kdl")).expect("read planning mode eval");
    assert_eq!(
        authored_harness_counts(&source, "planning-mode/eval.kdl"),
        (0, 0),
        "the planning API, not the eval KDL, owns the durable planner"
    );
    let controller =
        fs::read_to_string(root.join("controller.sh")).expect("read planning controller");
    assert!(controller.contains("plan start"));
    assert!(controller.contains("--model gpt-5.6-sol"));
    assert!(!controller.contains("claude"));
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
    let model_free = [
        "resource-cold-start",
        "resource-retarget",
        "resource-handoff",
        "context-resource-continuity",
        "crash-escalation",
        "pty-attach-machine-stream",
        "pty-attach-only",
        "pty-send-peek",
        "network-smoke",
        "network-isolation",
    ];
    for name in [
        "license-mit",
        "ghost-bug",
        "signal-rename",
        "restart-continuity",
        "fork-in-the-road",
        "poisoned-pr",
        "test-writing",
        "weird-git-setup",
        "claude-skill-inheritance",
        "resource-cold-start",
        "resource-retarget",
        "resource-handoff",
        "context-resource-continuity",
        "crash-escalation",
        "pty-attach-machine-stream",
        "pty-attach-only",
        "pty-send-peek",
        "network-smoke",
        "network-isolation",
    ] {
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
        let st2_agents = st2_spec.agents.iter().chain(
            st2_spec
                .eval
                .iter()
                .flat_map(|evaluation| evaluation.agents.iter()),
        );
        if model_free.contains(&name) {
            for agent in st2_agents {
                assert!(
                    agent.driver.is_none(),
                    "{} model-free agent {} must not use a harness",
                    st2_file.display(),
                    agent.id
                );
            }
            assert_eq!(
                authored_harness_counts(&st3_source, &st3_file.display().to_string()),
                (0, 0),
                "{} must remain model-free",
                st3_file.display()
            );
            assert!(
                authored_model_gates(&st3_source).is_empty(),
                "{} must not use an LLM gate",
                st3_file.display()
            );
        } else {
            for agent in st2_agents {
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
        }
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
        ("license-mit", (2, 1), (2, 0)),
        ("ghost-bug", (0, 2), (0, 2)),
        ("signal-rename", (0, 4), (0, 4)),
        ("restart-continuity", (2, 0), (2, 0)),
        ("fork-in-the-road", (0, 4), (0, 4)),
        ("poisoned-pr", (0, 2), (0, 2)),
        ("test-writing", (0, 2), (0, 2)),
        ("weird-git-setup", (0, 1), (0, 1)),
        ("claude-skill-inheritance", (1, 0), (1, 0)),
    ];
    let mut model_gates = Vec::new();

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
        model_gates.extend(authored_model_gates(&st3_source));
    }
    let plan_lift = fs::read_to_string(root.join("st3/plan-document-lift/eval.kdl")).unwrap();
    assert_eq!(
        authored_harness_counts(&plan_lift, "st3 plan-document-lift"),
        (0, 1)
    );
    model_gates.extend(authored_model_gates(&plan_lift));
    model_gates.sort();
    assert_eq!(
        model_gates,
        ["gpt-5.6-sol", "gpt-5.6-sol", "gpt-5.6-sol"],
        "the model gate inventory changed; update evals/README.md"
    );
}

#[test]
fn license_and_ghost_bug_keep_the_complete_team_loop_in_the_graph() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("evals/st3");
    let cases = [
        (
            "license-mit",
            "eval/license-mit",
            [
                ("delegate-license-change", "agent/lmc.sup"),
                ("implement-license-change", "agent/lmc.worker"),
                ("verify-and-confirm", "agent/lmc.sup"),
            ]
            .as_slice(),
            [
                "resource/plan-run/${ST_PLAN_RUN}/license-brief",
                "resource/plan-run/${ST_PLAN_RUN}/license-revision",
                "resource/plan-run/${ST_PLAN_RUN}/worker-report",
                "resource/plan-run/${ST_PLAN_RUN}/final-confirmation",
            ]
            .as_slice(),
        ),
        (
            "ghost-bug",
            "eval/ghost-bug-codex",
            [
                ("delegate-debug-brief", "agent/gbx.sup"),
                ("diagnose-and-fix", "agent/gbx.fix"),
                ("verify-and-confirm", "agent/gbx.sup"),
            ]
            .as_slice(),
            [
                "resource/plan-run/${ST_PLAN_RUN}/debug-brief",
                "resource/plan-run/${ST_PLAN_RUN}/fix-revision",
                "resource/plan-run/${ST_PLAN_RUN}/worker-report",
                "resource/plan-run/${ST_PLAN_RUN}/final-confirmation",
            ]
            .as_slice(),
        ),
    ];

    for (name, plan_id, assignments, products) in cases {
        let source = fs::read_to_string(root.join(name).join("eval.kdl")).unwrap();
        assert!(!source.contains("message \"kickoff"));
        assert!(!source.contains("wait-team-done"));
        let intent = st3::parse_intent(&source, "local").unwrap();
        let plan = &intent.plans[plan_id];
        for (step, assignee) in assignments {
            assert_eq!(plan.steps[*step].assigned_to.as_deref(), Some(*assignee));
            assert!(plan.steps[*step].nested_plan.is_some());
        }
        let declared = plan
            .steps
            .values()
            .filter_map(|step| step.nested_plan.as_ref())
            .flat_map(|nested| nested.steps.values())
            .flat_map(|step| &step.products)
            .map(|product| product.subject.as_str())
            .collect::<Vec<_>>();
        for product in products {
            assert!(declared.contains(product), "{name} lacks {product}");
        }
    }
}

#[test]
fn plan_document_lift_produces_and_uses_one_exact_plan_output() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("evals/st3/plan-document-lift");
    let source = fs::read_to_string(root.join("eval.kdl")).unwrap();
    let intent = st3::parse_intent(&source, "local").unwrap();
    let plan = &intent.plans["eval/plan-document-lift"];
    assert_eq!(plan.baselines.len(), 1);
    assert_eq!(
        plan.baselines[0].name,
        "the exact source plan exists before planning starts"
    );
    assert_eq!(
        plan.steps["lift-plan-document"].produces_plan.as_deref(),
        Some("eval/plan-document-lift/work")
    );
    assert_eq!(
        plan.steps["execute-lifted-plan"].uses_plan,
        Some(st3::model::UsedPlanSpec::StepOutput {
            step: "lift-plan-document".into()
        })
    );
    assert!(
        intent.document_refs.contains(
            "doc/evals/plan-document-lift/plan@963217e0eeac9f2350e034c8da411244f4090731606242bbac6dd0a8bd55d636"
        )
    );
}

#[test]
fn license_mit_st3_fixture_matches_its_claude_team() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("evals/st3/license-mit");
    for seat in ["sup", "worker"] {
        let workspace = root.join(seat);
        assert_eq!(
            fs::read_to_string(workspace.join("CLAUDE.md")).unwrap(),
            "@PERSONA.md\n"
        );
        assert!(workspace.join("PERSONA.md").is_file());
        assert!(!workspace.join("AGENTS.md").exists());
        assert!(!workspace.join(".codex/hooks.json").exists());
    }
}

#[test]
fn restart_continuity_fixtures_match_their_claude_teams() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("evals");
    for runtime in ["st2", "st3"] {
        for seat in ["sup", "worker"] {
            let workspace = if runtime == "st2" {
                root.join(runtime)
                    .join("restart-continuity/fixture")
                    .join(seat)
            } else {
                root.join(runtime).join("restart-continuity").join(seat)
            };
            assert_eq!(
                fs::read_to_string(workspace.join("CLAUDE.md")).unwrap(),
                "@PERSONA.md\n"
            );
            assert!(workspace.join("PERSONA.md").is_file());
            assert!(!workspace.join("AGENTS.md").exists());
        }
    }

    let st2_source = fs::read_to_string(root.join("st2/restart-continuity/eval.kdl")).unwrap();
    let st2_spec = st2::eval_spec::parse_spec(&st2_source).unwrap();
    let supervisor = st2_spec
        .agents
        .iter()
        .find(|agent| agent.id == "rc.sup")
        .expect("restart supervisor");
    assert!(
        supervisor
            .execs
            .iter()
            .any(|process| process.id == "rc.sup.injector")
    );
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
            "held-out-gates",
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
    assert_eq!(dependencies("held-out-gates"), ["integrate-and-verify"]);
    assert_eq!(dependencies("publish-final-report"), ["held-out-gates"]);

    let required_products = [
        (
            "open-base-compatibility",
            "resource/plan-run/${ST_PLAN_RUN}/base-compatibility",
        ),
        (
            "migrate-relay",
            "resource/plan-run/${ST_PLAN_RUN}/relay-revision",
        ),
        (
            "migrate-hub",
            "resource/plan-run/${ST_PLAN_RUN}/hub-revision",
        ),
        (
            "update-root-and-config",
            "resource/plan-run/${ST_PLAN_RUN}/config-revision",
        ),
        (
            "close-base-compatibility",
            "resource/plan-run/${ST_PLAN_RUN}/base-final-revision",
        ),
        (
            "integrate-and-verify",
            "resource/plan-run/${ST_PLAN_RUN}/integrated-revision",
        ),
        (
            "publish-final-report",
            "resource/plan-run/${ST_PLAN_RUN}/final-report",
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

#[test]
fn restart_continuity_keeps_recovery_state_in_the_plan_graph() {
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("evals/st3/restart-continuity/eval.kdl");
    let source = fs::read_to_string(&file).expect("read Restart continuity eval");
    assert!(!source.contains("message \"kickoff"));

    let intent = st3::parse_intent(&source, "local").expect("parse Restart continuity eval");
    let plan = &intent.plans["eval/restart-continuity"];
    assert_eq!(
        plan.display_order,
        [
            "start-team",
            "process-before-restart",
            "inject-cold-restart",
            "process-after-restart",
            "verify-and-confirm",
            "held-out-gates",
            "cleanup",
        ]
    );

    for (step, assignee) in [
        ("process-before-restart", "agent/rc.dev"),
        ("process-after-restart", "agent/rc.dev"),
        ("verify-and-confirm", "agent/rc.sup"),
    ] {
        assert_eq!(plan.steps[step].assigned_to.as_deref(), Some(assignee));
        assert!(plan.steps[step].nested_plan.is_some());
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
    assert_eq!(dependencies("process-before-restart"), ["start-team"]);
    assert_eq!(
        dependencies("inject-cold-restart"),
        ["process-before-restart"]
    );
    assert_eq!(
        dependencies("process-after-restart"),
        ["inject-cold-restart"]
    );
    assert_eq!(
        dependencies("verify-and-confirm"),
        ["process-after-restart"]
    );
    assert_eq!(dependencies("held-out-gates"), ["verify-and-confirm"]);

    let required_products = [
        "resource/plan-run/${ST_PLAN_RUN}/pre-restart",
        "resource/plan-run/${ST_PLAN_RUN}/restart",
        "resource/plan-run/${ST_PLAN_RUN}/batch",
        "resource/plan-run/${ST_PLAN_RUN}/worker-report",
        "resource/plan-run/${ST_PLAN_RUN}/verification",
    ];
    for product in required_products {
        assert!(plan.steps.values().any(|step| {
            step.products
                .iter()
                .any(|candidate| candidate.subject == product)
                || step.nested_plan.as_ref().is_some_and(|nested| {
                    nested
                        .steps
                        .values()
                        .flat_map(|nested_step| &nested_step.products)
                        .any(|candidate| candidate.subject == product)
                })
        }));
    }
}

#[test]
fn fork_in_the_road_keeps_parallel_debate_in_the_plan_graph() {
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("evals/st3/fork-in-the-road/eval.kdl");
    let source = fs::read_to_string(&file).expect("read Fork in the road eval");
    assert!(!source.contains("message \"kickoff"));

    let intent = st3::parse_intent(&source, "local").expect("parse Fork in the road eval");
    let plan = &intent.plans["eval/fork-in-the-road"];
    let team = st3::parse_intent(
        plan.steps["start-team"]
            .subgraph_kdl
            .as_deref()
            .expect("team subgraph"),
        "local",
    )
    .expect("parse Fork in the road team");
    for member in ["fd.a", "fd.b", "fd.c"] {
        let under = st3::graph::agent_under(&team.subjects[&format!("agent/{member}")].desired);
        assert_eq!(under.len(), 1);
        assert_eq!(under[0].agent, "agent/fd.sup");
        assert_eq!(
            under[0].reason.as_deref(),
            Some("the supervisor combines the panel recommendation")
        );
    }
    assert_eq!(
        plan.display_order,
        [
            "start-team",
            "draft-per-human",
            "draft-shared",
            "draft-federated",
            "critique-per-human",
            "critique-shared",
            "critique-federated",
            "revise-per-human",
            "revise-shared",
            "revise-federated",
            "synthesize",
            "held-out-gates",
            "cleanup",
        ]
    );

    for (step, assignee) in [
        ("draft-per-human", "agent/fd.a"),
        ("draft-shared", "agent/fd.b"),
        ("draft-federated", "agent/fd.c"),
        ("critique-per-human", "agent/fd.a"),
        ("critique-shared", "agent/fd.b"),
        ("critique-federated", "agent/fd.c"),
        ("revise-per-human", "agent/fd.a"),
        ("revise-shared", "agent/fd.b"),
        ("revise-federated", "agent/fd.c"),
        ("synthesize", "agent/fd.sup"),
    ] {
        assert_eq!(plan.steps[step].assigned_to.as_deref(), Some(assignee));
        assert!(plan.steps[step].nested_plan.is_some());
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
    assert_eq!(dependencies("draft-per-human"), ["start-team"]);
    assert_eq!(
        dependencies("critique-per-human"),
        ["draft-per-human", "draft-shared", "draft-federated"]
    );
    assert_eq!(
        dependencies("revise-per-human"),
        [
            "critique-per-human",
            "critique-shared",
            "critique-federated"
        ]
    );
    assert_eq!(
        dependencies("synthesize"),
        ["revise-per-human", "revise-shared", "revise-federated"]
    );
    assert_eq!(dependencies("held-out-gates"), ["synthesize"]);

    let products = plan
        .steps
        .values()
        .filter_map(|step| step.nested_plan.as_ref())
        .flat_map(|nested| nested.steps.values())
        .flat_map(|step| &step.products)
        .map(|product| product.subject.as_str())
        .collect::<Vec<_>>();
    for product in [
        "resource/plan-run/${ST_PLAN_RUN}/proposal-a-draft",
        "resource/plan-run/${ST_PLAN_RUN}/proposal-b-draft",
        "resource/plan-run/${ST_PLAN_RUN}/proposal-c-draft",
        "resource/plan-run/${ST_PLAN_RUN}/proposal-a-final",
        "resource/plan-run/${ST_PLAN_RUN}/proposal-b-final",
        "resource/plan-run/${ST_PLAN_RUN}/proposal-c-final",
        "resource/plan-run/${ST_PLAN_RUN}/recommendation",
        "resource/plan-run/${ST_PLAN_RUN}/final-report",
    ] {
        assert!(products.contains(&product));
    }
}

#[test]
fn poisoned_pr_keeps_review_state_in_the_plan_graph() {
    let file = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("evals/st3/poisoned-pr/eval.kdl");
    let source = fs::read_to_string(&file).expect("read Poisoned pull request eval");
    assert!(!source.contains("message \"kickoff"));

    let intent = st3::parse_intent(&source, "local").expect("parse Poisoned pull request eval");
    let plan = &intent.plans["eval/poisoned-pr"];
    assert_eq!(
        plan.display_order,
        [
            "start-team",
            "review-pull-request",
            "assess-review",
            "held-out-gates",
            "cleanup",
        ]
    );
    assert_eq!(
        plan.steps["review-pull-request"].assigned_to.as_deref(),
        Some("agent/prx.rev")
    );
    assert_eq!(
        plan.steps["assess-review"].assigned_to.as_deref(),
        Some("agent/prx.sup")
    );
    assert!(plan.steps["review-pull-request"].nested_plan.is_some());
    assert!(plan.steps["assess-review"].nested_plan.is_some());

    let products = plan
        .steps
        .values()
        .filter_map(|step| step.nested_plan.as_ref())
        .flat_map(|nested| nested.steps.values())
        .flat_map(|step| &step.products)
        .map(|product| product.subject.as_str())
        .collect::<Vec<_>>();
    assert!(products.contains(&"resource/plan-run/${ST_PLAN_RUN}/reviewer-report"));
    assert!(products.contains(&"resource/plan-run/${ST_PLAN_RUN}/final-verdict"));
}

#[test]
fn new_paid_evals_keep_work_and_products_in_the_plan_graph() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("evals/st3");
    let cases = [
        (
            "test-writing",
            "eval/test-writing",
            [
                ("prepare-test-brief", "agent/tw.sup"),
                ("write-regression-suite", "agent/tw.dev"),
                ("verify-test-suite", "agent/tw.sup"),
            ]
            .as_slice(),
            [
                "resource/plan-run/${ST_PLAN_RUN}/test-brief",
                "resource/plan-run/${ST_PLAN_RUN}/test-revision",
                "resource/plan-run/${ST_PLAN_RUN}/developer-report",
                "resource/plan-run/${ST_PLAN_RUN}/final-assessment",
            ]
            .as_slice(),
        ),
        (
            "weird-git-setup",
            "eval/weird-git-setup",
            [("repair-feature-worktree", "agent/wg.dev")].as_slice(),
            [
                "resource/plan-run/${ST_PLAN_RUN}/feature-revision",
                "resource/plan-run/${ST_PLAN_RUN}/final-report",
            ]
            .as_slice(),
        ),
        (
            "claude-skill-inheritance",
            "eval/claude-skill-inheritance",
            [("exercise-skill-union", "agent/si.agent")].as_slice(),
            ["resource/plan-run/${ST_PLAN_RUN}/skill-report"].as_slice(),
        ),
    ];

    for (name, plan_id, assignments, expected_products) in cases {
        let source = fs::read_to_string(root.join(name).join("eval.kdl")).unwrap();
        assert!(!source.contains("message \"kickoff"));
        let intent = st3::parse_intent(&source, "local").unwrap();
        let plan = &intent.plans[plan_id];
        for (step, assignee) in assignments {
            assert_eq!(plan.steps[*step].assigned_to.as_deref(), Some(*assignee));
            assert!(plan.steps[*step].nested_plan.is_some());
        }
        let products = plan
            .steps
            .values()
            .filter_map(|step| step.nested_plan.as_ref())
            .flat_map(|nested| nested.steps.values())
            .flat_map(|step| &step.products)
            .map(|product| product.subject.as_str())
            .collect::<Vec<_>>();
        for product in expected_products {
            assert!(products.contains(product), "{name} lacks {product}");
        }
    }
}

#[test]
fn new_paid_eval_fixtures_match_their_native_harnesses() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("evals");

    for runtime in ["st2", "st3"] {
        let test_root = if runtime == "st2" {
            root.join(runtime).join("test-writing/fixture")
        } else {
            root.join(runtime).join("test-writing")
        };
        for seat in ["sup", "worker"] {
            let workspace = test_root.join(seat);
            assert!(workspace.join("AGENTS.md").is_file());
            assert!(!workspace.join("CLAUDE.md").exists());
        }

        let weird_root = if runtime == "st2" {
            root.join(runtime).join("weird-git-setup/fixture")
        } else {
            root.join(runtime).join("weird-git-setup")
        };
        assert!(weird_root.join("persona/AGENTS.md").is_file());
        let setup = fs::read_to_string(weird_root.join("setup-megarepo.sh")).unwrap();
        assert!(setup.contains("persona/AGENTS.md"));
        assert!(!setup.contains("persona/CLAUDE.md"));

        let skill_root = if runtime == "st2" {
            root.join(runtime).join("claude-skill-inheritance/fixture")
        } else {
            root.join(runtime).join("claude-skill-inheritance")
        };
        assert_eq!(
            fs::read_to_string(skill_root.join("repo/CLAUDE.md")).unwrap(),
            "@PERSONA.md\n"
        );
        assert!(
            skill_root
                .join("repo/.claude/skills/evalskill-project/SKILL.md")
                .is_file()
        );
        assert!(
            skill_root
                .join("plugin/evalpkg/skills/evalskill-plugin/SKILL.md")
                .is_file()
        );
    }
}
