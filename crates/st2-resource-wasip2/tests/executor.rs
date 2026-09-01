use std::fs;

use serde_json::json;
use st2_resource_protocol::ObservationResult;
use st2_resource_wasip2::{
    CacheDisposition, CacheRejection, CapabilityModule, Executor, InvocationStore, LimitKind,
    LoadError, NoCapabilities, ObservationRequest, ObserveError, PrivateArtifactCache,
    RuntimeConfig,
};
use wasmtime::component::{HasSelf, Linker};

mod fixture {
    wasmtime::component::bindgen!({
        path: "wit",
        world: "fixture-host",
    });
}

use fixture::compoundingtech::st2_resource_observer::observation::{
    Fact, FactValue, Host, ObservationError, Proposal, Publication, Request,
};

const OBSERVATION_IMPORT: &str =
    "compoundingtech:st2-resource-observer/observation@0.1.0";
const NO_EFFECT: &str = include_str!("fixtures/no-effect-component.wat");

#[derive(Default)]
struct FixtureInvocation {
    calls: u8,
}

impl Host for InvocationStore<FixtureInvocation> {
    fn observe(&mut self, request: Request) -> Result<Proposal, ObservationError> {
        self.capability_mut().calls += 1;
        let calls = self.capability().calls;
        let topics = if request.uri == "fixture://invalid" {
            vec!["duplicate".to_owned(), "duplicate".to_owned()]
        } else {
            vec!["fixture".to_owned()]
        };
        Ok(Proposal::Published(Publication {
            schema_id: "dev.compounding.fixture/v1".to_owned(),
            media_type: "application/octet-stream".to_owned(),
            bytes: vec![calls],
            topics,
            facts: vec![Fact {
                key: "calls".to_owned(),
                before: FactValue::Omitted,
                after: FactValue::Value(calls.to_string()),
            }],
        }))
    }
}

struct FixtureCapabilities;

impl CapabilityModule for FixtureCapabilities {
    type Invocation = FixtureInvocation;

    fn import_names(&self) -> &'static [&'static str] {
        &[OBSERVATION_IMPORT]
    }

    fn add_to_linker(
        &self,
        linker: &mut Linker<InvocationStore<Self::Invocation>>,
    ) -> Result<(), wasmtime::Error> {
        fixture::FixtureHost::add_to_linker::<_, HasSelf<_>>(linker, |state| state)
    }

    fn begin(&self, _request: &ObservationRequest) -> Self::Invocation {
        FixtureInvocation::default()
    }
}

fn executor(config: RuntimeConfig, cache: Option<PrivateArtifactCache>) -> Executor<FixtureCapabilities> {
    Executor::new(config, cache, FixtureCapabilities).unwrap()
}

fn request() -> ObservationRequest {
    ObservationRequest {
        uri: "fixture://resource".to_owned(),
        selector: json!({"region": "local"}),
        previous_digest: None,
    }
}

fn component(wat: &str) -> Vec<u8> {
    wat::parse_str(wat).unwrap()
}

fn with_core_behavior(core: &str) -> Vec<u8> {
    component(&NO_EFFECT.replacen("(component", &format!("(component\n{core}"), 1))
}

fn publication_bytes(result: ObservationResult) -> Vec<u8> {
    match result {
        ObservationResult::Published { publication } => publication.bytes.into_vec(),
        other => panic!("expected publication, got {other:?}"),
    }
}

#[test]
fn executes_one_typed_observation_proposal() {
    let executor = executor(RuntimeConfig::default(), None);
    let loaded = executor.load(&component(NO_EFFECT)).unwrap();
    let result = executor.observe(&loaded, &request(), None).unwrap();
    assert_eq!(publication_bytes(result), vec![1]);
}

#[test]
fn host_rejects_a_semantically_invalid_guest_proposal() {
    let executor = executor(RuntimeConfig::default(), None);
    let loaded = executor.load(&component(NO_EFFECT)).unwrap();
    let mut invalid = request();
    invalid.uri = "fixture://invalid".to_owned();
    assert!(matches!(
        executor.observe(&loaded, &invalid, None),
        Err(ObserveError::InvalidProposal(_))
    ));
}

#[test]
fn a_trap_is_contained_and_structured() {
    let bytes = with_core_behavior(
        "(core module $behavior (func $start unreachable) (start $start))\n\
         (core instance $running (instantiate $behavior))",
    );
    let executor = executor(RuntimeConfig::default(), None);
    let loaded = executor.load(&bytes).unwrap();
    assert!(matches!(
        executor.observe(&loaded, &request(), None),
        Err(ObserveError::Trap(_))
    ));
}

#[test]
fn fuel_exhaustion_is_distinct_from_guest_traps() {
    let bytes = with_core_behavior(
        "(core module $behavior\n\
           (func $start (loop $spin (br $spin)))\n\
           (start $start))\n\
         (core instance $running (instantiate $behavior))",
    );
    let mut config = RuntimeConfig::default();
    config.fuel_per_observation = 10_000;
    let executor = executor(config, None);
    let loaded = executor.load(&bytes).unwrap();
    assert!(matches!(
        executor.observe(&loaded, &request(), None),
        Err(ObserveError::FuelExhausted)
    ));
}

#[test]
fn linear_memory_limit_is_enforced_during_instantiation() {
    let bytes = with_core_behavior(
        "(core module $behavior (memory 2))\n\
         (core instance $running (instantiate $behavior))",
    );
    let mut config = RuntimeConfig::default();
    config.max_memory_bytes = 64 * 1024;
    let executor = executor(config, None);
    let loaded = executor.load(&bytes).unwrap();
    assert!(matches!(
        executor.observe(&loaded, &request(), None),
        Err(ObserveError::ResourceLimit(LimitKind::Memory))
    ));
}

#[test]
fn instance_count_limit_is_enforced_as_an_instantiation_error() {
    let mut config = RuntimeConfig::default();
    config.max_instances = 1;
    let executor = executor(config, None);
    let loaded = executor.load(&component(NO_EFFECT)).unwrap();
    assert!(matches!(
        executor.observe(&loaded, &request(), None),
        Err(ObserveError::Instantiation(_))
    ));
}

#[test]
fn deterministic_epoch_handle_classifies_timeout_and_cancel() {
    let bytes = with_core_behavior(
        "(core module $behavior\n\
           (func $start (loop $spin (br $spin)))\n\
           (start $start))\n\
         (core instance $running (instantiate $behavior))",
    );
    let mut config = RuntimeConfig::default();
    config.fuel_per_observation = u64::MAX;
    let executor = executor(config, None);
    let loaded = executor.load(&bytes).unwrap();

    let timeout = executor.interruption_handle();
    assert!(timeout.time_out());
    assert!(matches!(
        executor.observe(&loaded, &request(), Some(&timeout)),
        Err(ObserveError::TimedOut)
    ));

    let cancellation = executor.interruption_handle();
    assert!(cancellation.cancel());
    assert!(matches!(
        executor.observe(&loaded, &request(), Some(&cancellation)),
        Err(ObserveError::Cancelled)
    ));

    let healthy = executor.load(&component(NO_EFFECT)).unwrap();
    assert_eq!(
        publication_bytes(executor.observe(&healthy, &request(), None).unwrap()),
        vec![1]
    );
}

#[test]
fn every_observation_gets_fresh_store_state() {
    let executor = executor(RuntimeConfig::default(), None);
    let loaded = executor.load(&component(NO_EFFECT)).unwrap();
    assert_eq!(
        publication_bytes(executor.observe(&loaded, &request(), None).unwrap()),
        vec![1]
    );
    assert_eq!(
        publication_bytes(executor.observe(&loaded, &request(), None).unwrap()),
        vec![1]
    );
}

#[test]
fn imports_are_rejected_unless_an_explicit_capability_module_admits_them() {
    let executor = Executor::<NoCapabilities>::closed(RuntimeConfig::default(), None).unwrap();
    let result = executor.load(&component(NO_EFFECT));
    assert!(matches!(
        result,
        Err(LoadError::ForbiddenImports(imports)) if imports == [OBSERVATION_IMPORT]
    ));
}

#[test]
fn verified_aot_artifact_is_reused_by_a_new_executor() {
    let temporary = tempfile::tempdir().unwrap();
    let cache = PrivateArtifactCache::open(temporary.path().join("cache")).unwrap();
    let bytes = component(NO_EFFECT);
    let first = executor(RuntimeConfig::default(), Some(cache.clone()));
    let loaded = first.load(&bytes).unwrap();
    assert_eq!(loaded.cache_disposition(), &CacheDisposition::CompiledAndStored);
    drop(first);

    let second = executor(RuntimeConfig::default(), Some(cache));
    let loaded = second.load(&bytes).unwrap();
    assert_eq!(loaded.cache_disposition(), &CacheDisposition::DiskHit);
}

#[test]
fn changed_runtime_identity_gets_a_clean_cache_miss_and_new_manifest() {
    let temporary = tempfile::tempdir().unwrap();
    let cache = PrivateArtifactCache::open(temporary.path().join("cache")).unwrap();
    let bytes = component(NO_EFFECT);
    let first = executor(RuntimeConfig::default(), Some(cache.clone()));
    let first_loaded = first.load(&bytes).unwrap();
    assert_eq!(
        first_loaded.cache_disposition(),
        &CacheDisposition::CompiledAndStored
    );
    drop(first);

    let mut changed_config = RuntimeConfig::default();
    changed_config.fuel_per_observation += 1;
    let second = executor(changed_config, Some(cache.clone()));
    let second_loaded = second.load(&bytes).unwrap();
    assert_eq!(
        second_loaded.cache_disposition(),
        &CacheDisposition::CompiledAndStored
    );

    let manifests = fs::read_dir(
        cache
            .root()
            .join("manifests")
            .join(second_loaded.digest().to_string()),
    )
    .unwrap()
    .count();
    assert_eq!(manifests, 2);
}

#[test]
fn corrupt_artifact_is_never_deserialized() {
    let temporary = tempfile::tempdir().unwrap();
    let cache = PrivateArtifactCache::open(temporary.path().join("cache")).unwrap();
    let bytes = component(NO_EFFECT);
    let first = executor(RuntimeConfig::default(), Some(cache.clone()));
    first.load(&bytes).unwrap();
    drop(first);

    let artifact = fs::read_dir(cache.root().join("objects"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut corrupt = fs::read(&artifact).unwrap();
    corrupt[0] ^= 0xff;
    fs::write(artifact, corrupt).unwrap();

    let second = executor(RuntimeConfig::default(), Some(cache));
    let loaded = second.load(&bytes).unwrap();
    assert_eq!(
        loaded.cache_disposition(),
        &CacheDisposition::RejectedAndCompiled(CacheRejection::ArtifactDigest)
    );
    assert_eq!(
        publication_bytes(second.observe(&loaded, &request(), None).unwrap()),
        vec![1]
    );
}

#[test]
fn manifest_engine_and_config_identity_mismatches_invalidate_aot() {
    for field in ["engineCompatibility", "configIdentity"] {
        let temporary = tempfile::tempdir().unwrap();
        let cache = PrivateArtifactCache::open(temporary.path().join("cache")).unwrap();
        let bytes = component(NO_EFFECT);
        let first = executor(RuntimeConfig::default(), Some(cache.clone()));
        let loaded = first.load(&bytes).unwrap();
        let manifest_directory = cache
            .root()
            .join("manifests")
            .join(loaded.digest().to_string());
        drop(first);
        let manifest = fs::read_dir(manifest_directory)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let mut value: serde_json::Value =
            serde_json::from_slice(&fs::read(&manifest).unwrap()).unwrap();
        value[field] = json!("mismatched");
        fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();

        let second = executor(RuntimeConfig::default(), Some(cache));
        let loaded = second.load(&bytes).unwrap();
        let expected = if field == "engineCompatibility" {
            CacheRejection::EngineCompatibility
        } else {
            CacheRejection::ConfigIdentity
        };
        assert_eq!(
            loaded.cache_disposition(),
            &CacheDisposition::RejectedAndCompiled(expected)
        );
    }
}
