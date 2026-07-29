//! End-to-end coverage for the experimental JSON catalog transaction surface.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn run(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_st2"))
        .arg("--catalog")
        .arg(root)
        .args(["catalog"])
        .args(args)
        .output()
        .unwrap()
}

fn json(output: Output) -> serde_json::Value {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn write_spec(path: &Path, identity: &str) {
    fs::write(
        path,
        format!("agent \"{identity}\" {{\n  host \"h\"\n  command \"sleep 10\"\n}}\n"),
    )
    .unwrap();
}

#[test]
fn prepare_stage_then_atomic_multi_seat_admit_has_explicit_visibility_boundaries() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path().join("catalog");
    fs::create_dir_all(&root).unwrap();
    let a = tmp.path().join("a.kdl");
    let b = tmp.path().join("b.kdl");
    write_spec(&a, "a");
    write_spec(&b, "b");

    let prepared = json(run(&root, &["prepare", a.to_str().unwrap()]));
    assert!(Path::new(prepared["objectPath"].as_str().unwrap()).is_file());
    assert_eq!(json(run(&root, &["head"])), serde_json::Value::Null);

    let staged_a = json(run(
        &root,
        &[
            "stage",
            a.to_str().unwrap(),
            "--manager",
            "eval",
            "--state-relative",
            "agents/h/a",
            "--operation-id",
            "a",
        ],
    ));
    let staged_b = json(run(
        &root,
        &[
            "stage",
            b.to_str().unwrap(),
            "--manager",
            "eval",
            "--state-relative",
            "agents/h/b",
            "--operation-id",
            "b",
        ],
    ));
    assert_eq!(staged_a["rootChanged"], false);
    assert_eq!(staged_b["rootChanged"], false);
    assert_eq!(json(run(&root, &["head"])), serde_json::Value::Null);

    let request = tmp.path().join("admit.json");
    fs::write(
        &request,
        serde_json::to_vec_pretty(&serde_json::json!({
            "expectedRoot": null,
            "manager": "eval",
            "operationId": "root-ab",
            "selections": [
                {
                    "refCommit": staged_a["refCommit"],
                    "resourceBindingCommit": staged_a["resourceBindingCommit"],
                },
                {
                    "refCommit": staged_b["refCommit"],
                    "resourceBindingCommit": staged_b["resourceBindingCommit"],
                }
            ]
        }))
        .unwrap(),
    )
    .unwrap();
    let admitted = json(run(&root, &["admit", request.to_str().unwrap()]));
    assert_eq!(admitted["seats"], 2);
    let inspected = json(run(&root, &["inspect"]));
    assert_eq!(inspected["rootCommit"], admitted["rootCommit"]);
    assert_eq!(inspected["seats"].as_array().unwrap().len(), 2);
}
