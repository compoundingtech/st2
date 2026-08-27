//! Durable, bounded diagnostics published by native driver cores.
//!
//! A driver owns one current record per agent. Internally the publisher retains one failure per
//! stage and projects the earliest failing stage, so a later transport symptom cannot hide an
//! unresolved admission failure. Recovering a stage clears only that stage and immediately reveals
//! the next outstanding failure; recovering the final stage removes the record.

use std::array;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

const SCHEMA: &str = "st2.driver-diagnostic.v1";
const RECOVERY: &str = "clearsOnStageRecovery";
const FUTURE_SKEW_MS: u64 = 60_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Stage {
    VersionGate,
    ApiGate,
    Sse,
    Seed,
    Delivery,
    ReadBack,
    #[serde(other)]
    Unknown,
}

impl Stage {
    pub const ALL: [Self; 6] = [
        Self::VersionGate,
        Self::ApiGate,
        Self::Sse,
        Self::Seed,
        Self::Delivery,
        Self::ReadBack,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::VersionGate => "versionGate",
            Self::ApiGate => "apiGate",
            Self::Sse => "sse",
            Self::Seed => "seed",
            Self::Delivery => "delivery",
            Self::ReadBack => "readBack",
            Self::Unknown => "unknown",
        }
    }

    const fn index(self) -> Option<usize> {
        match self {
            Self::VersionGate => Some(0),
            Self::ApiGate => Some(1),
            Self::Sse => Some(2),
            Self::Seed => Some(3),
            Self::Delivery => Some(4),
            Self::ReadBack => Some(5),
            Self::Unknown => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Driver {
    #[serde(rename = "opencode")]
    OpenCode,
    #[serde(other)]
    Unknown,
}

impl Driver {
    pub const ALL: [Self; 1] = [Self::OpenCode];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::OpenCode => "opencode",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Reason {
    VersionProbeFailed,
    UnsupportedVersion,
    ApiUnavailable,
    IncompatibleApi,
    SseConnectFailed,
    SseDisconnected,
    UnknownEvent,
    StatusUnavailable,
    MalformedStatus,
    UnknownStatus,
    PermissionUnavailable,
    MalformedPermissions,
    QuestionUnavailable,
    MalformedQuestions,
    MissingAskId,
    DeliveryUnavailable,
    DeliveryRejected,
    ReadBackUnavailable,
    NotDurable,
    #[serde(other)]
    Unknown,
}

impl Reason {
    pub const ALL: [Self; 19] = [
        Self::VersionProbeFailed,
        Self::UnsupportedVersion,
        Self::ApiUnavailable,
        Self::IncompatibleApi,
        Self::SseConnectFailed,
        Self::SseDisconnected,
        Self::UnknownEvent,
        Self::StatusUnavailable,
        Self::MalformedStatus,
        Self::UnknownStatus,
        Self::PermissionUnavailable,
        Self::MalformedPermissions,
        Self::QuestionUnavailable,
        Self::MalformedQuestions,
        Self::MissingAskId,
        Self::DeliveryUnavailable,
        Self::DeliveryRejected,
        Self::ReadBackUnavailable,
        Self::NotDurable,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::VersionProbeFailed => "versionProbeFailed",
            Self::UnsupportedVersion => "unsupportedVersion",
            Self::ApiUnavailable => "apiUnavailable",
            Self::IncompatibleApi => "incompatibleApi",
            Self::SseConnectFailed => "sseConnectFailed",
            Self::SseDisconnected => "sseDisconnected",
            Self::UnknownEvent => "unknownEvent",
            Self::StatusUnavailable => "statusUnavailable",
            Self::MalformedStatus => "malformedStatus",
            Self::UnknownStatus => "unknownStatus",
            Self::PermissionUnavailable => "permissionUnavailable",
            Self::MalformedPermissions => "malformedPermissions",
            Self::QuestionUnavailable => "questionUnavailable",
            Self::MalformedQuestions => "malformedQuestions",
            Self::MissingAskId => "missingAskId",
            Self::DeliveryUnavailable => "deliveryUnavailable",
            Self::DeliveryRejected => "deliveryRejected",
            Self::ReadBackUnavailable => "readBackUnavailable",
            Self::NotDurable => "notDurable",
            Self::Unknown => "unknown",
        }
    }

    pub const fn stage(self) -> Stage {
        match self {
            Self::VersionProbeFailed | Self::UnsupportedVersion => Stage::VersionGate,
            Self::ApiUnavailable | Self::IncompatibleApi => Stage::ApiGate,
            Self::SseConnectFailed | Self::SseDisconnected | Self::UnknownEvent => Stage::Sse,
            Self::StatusUnavailable
            | Self::MalformedStatus
            | Self::UnknownStatus
            | Self::PermissionUnavailable
            | Self::MalformedPermissions
            | Self::QuestionUnavailable
            | Self::MalformedQuestions
            | Self::MissingAskId => Stage::Seed,
            Self::DeliveryUnavailable | Self::DeliveryRejected => Stage::Delivery,
            Self::ReadBackUnavailable | Self::NotDurable => Stage::ReadBack,
            Self::Unknown => Stage::Unknown,
        }
    }

    const fn accepts_source(self, source: Source) -> bool {
        match self {
            Self::VersionProbeFailed | Self::UnsupportedVersion => {
                matches!(source, Source::VersionProbe)
            }
            Self::ApiUnavailable | Self::IncompatibleApi => {
                matches!(source, Source::OpenApiDocument)
            }
            Self::SseConnectFailed | Self::SseDisconnected | Self::UnknownEvent => {
                matches!(source, Source::EventStream)
            }
            Self::StatusUnavailable | Self::MalformedStatus | Self::UnknownStatus => {
                matches!(source, Source::StatusSnapshot)
            }
            Self::PermissionUnavailable | Self::MalformedPermissions => {
                matches!(source, Source::PermissionSnapshot)
            }
            Self::QuestionUnavailable | Self::MalformedQuestions => {
                matches!(source, Source::QuestionSnapshot)
            }
            Self::MissingAskId => {
                matches!(source, Source::PermissionSnapshot | Source::QuestionSnapshot)
            }
            Self::DeliveryUnavailable | Self::DeliveryRejected => {
                matches!(source, Source::PromptTransport)
            }
            Self::ReadBackUnavailable | Self::NotDurable => {
                matches!(source, Source::MessageReadBack)
            }
            Self::Unknown => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Source {
    VersionProbe,
    OpenApiDocument,
    EventStream,
    StatusSnapshot,
    PermissionSnapshot,
    QuestionSnapshot,
    PromptTransport,
    MessageReadBack,
    #[serde(other)]
    Unknown,
}

impl Source {
    pub const ALL: [Self; 8] = [
        Self::VersionProbe,
        Self::OpenApiDocument,
        Self::EventStream,
        Self::StatusSnapshot,
        Self::PermissionSnapshot,
        Self::QuestionSnapshot,
        Self::PromptTransport,
        Self::MessageReadBack,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::VersionProbe => "versionProbe",
            Self::OpenApiDocument => "openApiDocument",
            Self::EventStream => "eventStream",
            Self::StatusSnapshot => "statusSnapshot",
            Self::PermissionSnapshot => "permissionSnapshot",
            Self::QuestionSnapshot => "questionSnapshot",
            Self::PromptTransport => "promptTransport",
            Self::MessageReadBack => "messageReadBack",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Support {
    Supported,
    Unsupported,
    Unknown,
    #[serde(other)]
    Unrecognized,
}

impl Support {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Supported => "supported",
            Self::Unsupported => "unsupported",
            Self::Unknown => "unknown",
            Self::Unrecognized => "unrecognized",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Record {
    schema: String,
    driver: Driver,
    stage: Stage,
    reason: Reason,
    source: Source,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    producer_version: Option<String>,
    support: Support,
    observed_at: u64,
    recovery: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    pub driver: Driver,
    pub stage: Stage,
    pub reason: Reason,
    pub source: Source,
    pub producer_version: Option<String>,
    pub support: Support,
    pub observed_at: u64,
    pub evidence_age_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidReason {
    MalformedRecord,
    UnsupportedSchema,
    UnknownVocabulary,
    FutureSkew,
}

impl InvalidReason {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MalformedRecord => "malformedRecord",
            Self::UnsupportedSchema => "unsupportedSchema",
            Self::UnknownVocabulary => "unknownVocabulary",
            Self::FutureSkew => "futureSkew",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observed {
    Absent,
    Failure(Failure),
    Indeterminate(InvalidReason),
}

impl Observed {
    pub const fn status(&self) -> &'static str {
        match self {
            Self::Absent => "absent",
            Self::Failure(_) => "failure",
            Self::Indeterminate(_) => "indeterminate",
        }
    }
}

/// Stable operator guidance shared by Doctor and any future renderer. The text is driver-agnostic;
/// stage and source carry the typed native-driver boundary.
pub fn repair_text(observed: &Observed) -> &'static str {
    match observed {
        Observed::Absent => {
            "no diagnostic evidence exists — wait for the native driver to publish a boundary result or restart the seat"
        }
        Observed::Indeterminate(InvalidReason::MalformedRecord) => {
            "replace the malformed driver-diagnostic record by restarting the seat"
        }
        Observed::Indeterminate(InvalidReason::UnsupportedSchema) => {
            "upgrade this st2 reader or restart the seat with a compatible driver-diagnostic writer"
        }
        Observed::Indeterminate(InvalidReason::UnknownVocabulary) => {
            "upgrade this st2 reader; unknown diagnostic vocabulary is not healthy evidence"
        }
        Observed::Indeterminate(InvalidReason::FutureSkew) => {
            "correct the writer clock or restart the seat after clock recovery"
        }
        Observed::Failure(failure) => match failure.stage {
            Stage::VersionGate => "install a supported producer version and restart the seat",
            Stage::ApiGate => "restore the producer API contract, then restart the seat",
            Stage::Sse => "restore the producer event stream; recovery clears this advisory",
            Stage::Seed => "restore readable producer state snapshots; recovery clears this advisory",
            Stage::Delivery => "restore the native prompt transport; the queued message remains retryable",
            Stage::ReadBack => "restore message read-back; st2 will reconcile without duplicating the prompt",
            Stage::Unknown => "upgrade this st2 reader; an unknown stage is not healthy evidence",
        },
    }
}

pub fn path(agent_dir: &Path) -> PathBuf {
    agent_dir.join("driver-diagnostic")
}

/// Whether this declaration has a native driver that currently publishes this record.
pub fn expected_for(spec: &crate::AgentSpec) -> bool {
    matches!(
        spec.driver.as_ref(),
        Some(crate::Driver::OpenCode(_))
    )
}
pub fn read(path: &Path) -> Observed {
    let raw = match fs::read(path) {
        Ok(raw) => raw,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Observed::Absent,
        Err(_) => return Observed::Indeterminate(InvalidReason::MalformedRecord),
    };
    read_at(&raw, now_ms())
}

fn read_at(raw: &[u8], now: u64) -> Observed {
    let record = match serde_json::from_slice::<Record>(raw) {
        Ok(record) => record,
        Err(_) => return Observed::Indeterminate(InvalidReason::MalformedRecord),
    };
    if record.schema != SCHEMA || record.recovery != RECOVERY {
        return Observed::Indeterminate(InvalidReason::UnsupportedSchema);
    }
    if record.stage == Stage::Unknown
        || record.reason == Reason::Unknown
        || record.source == Source::Unknown
        || record.driver == Driver::Unknown
        || record.reason.stage() != record.stage
        || !record.reason.accepts_source(record.source)
        || record.support == Support::Unrecognized
    {
        return Observed::Indeterminate(InvalidReason::UnknownVocabulary);
    }
    if record.observed_at > now.saturating_add(FUTURE_SKEW_MS) {
        return Observed::Indeterminate(InvalidReason::FutureSkew);
    }
    Observed::Failure(Failure {
        driver: record.driver,
        stage: record.stage,
        reason: record.reason,
        source: record.source,
        producer_version: record.producer_version,
        support: record.support,
        observed_at: record.observed_at,
        evidence_age_ms: now.saturating_sub(record.observed_at),
    })
}

/// In-process stage set for one native driver session. Persistence failures stay diagnostic-only:
/// they are logged but never change launch, observation, delivery, retry, or archive semantics.
pub struct Publisher {
    path: PathBuf,
    driver: Driver,
    producer_version: Option<String>,
    support: Support,
    failures: [Option<Record>; 6],
}

impl Publisher {
    pub fn new(
        agent_dir: &Path,
        driver: Driver,
        producer_version: Option<String>,
        support: Support,
    ) -> Self {
        let path = path(agent_dir);
        if matches!(
            read(&path),
            Observed::Indeterminate(InvalidReason::MalformedRecord)
        ) && let Err(error) = fs::remove_file(&path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                path = %path.display(),
                "st2 driver diagnostic malformed predecessor cleanup failed: {error}"
            );
        }
        Self {
            path,
            driver,
            producer_version,
            support,
            failures: array::from_fn(|_| None),
        }
    }

    pub fn publish(&mut self, stage: Stage, reason: Reason, source: Source) {
        let Some(index) = stage.index() else {
            return;
        };
        if reason.stage() != stage || !reason.accepts_source(source) {
            return;
        }
        if self.failures[index]
            .as_ref()
            .is_some_and(|failure| failure.reason == reason && failure.source == source)
        {
            return;
        }
        let record = Record {
            schema: SCHEMA.to_string(),
            driver: self.driver,
            stage,
            reason,
            source,
            producer_version: self.producer_version.clone(),
            support: self.support,
            observed_at: now_ms(),
            recovery: RECOVERY.to_string(),
        };
        self.failures[index] = Some(record);
        crate::metrics::record_driver_diagnostic(stage, reason, source, self.support, false);
        emit(stage, reason, source, self.support, "failure", self.producer_version.as_deref());
        self.persist();
    }

    pub fn clear(&mut self, stage: Stage) {
        let Some(index) = stage.index() else {
            return;
        };
        let cleared = self.failures[index].take().or_else(|| {
            let raw = fs::read(&self.path).ok()?;
            let record = serde_json::from_slice::<Record>(&raw).ok()?;
            (record.schema == SCHEMA
                && record.recovery == RECOVERY
                && record.driver == self.driver
                && record.stage == stage)
                .then_some(record)
        });
        let Some(cleared) = cleared else {
            return;
        };
        crate::metrics::record_driver_diagnostic(
            stage,
            cleared.reason,
            cleared.source,
            cleared.support,
            true,
        );
        emit(
            stage,
            cleared.reason,
            cleared.source,
            cleared.support,
            "recovery",
            self.producer_version.as_deref(),
        );
        self.persist();
    }

    fn persist(&self) {
        let result = match self.failures.iter().flatten().next() {
            Some(record) => atomic_json(&self.path, record),
            None => match fs::remove_file(&self.path) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error),
            },
        };
        if let Err(error) = result {
            tracing::warn!(
                path = %self.path.display(),
                "st2 driver diagnostic persistence failed: {error}"
            );
        }
    }
}

fn emit(
    stage: Stage,
    reason: Reason,
    source: Source,
    support: Support,
    outcome: &'static str,
    producer_version: Option<&str>,
) {
    let span = crate::telemetry::tracer_export_enabled().then(|| {
        tracing::info_span!(
            "st2.driver.diagnostic",
            "span.label" = stage.as_str(),
            "st2.driver.stage" = stage.as_str(),
            "st2.driver.reason" = reason.as_str(),
            "st2.driver.source" = source.as_str(),
            "st2.driver.support" = support.as_str(),
            "st2.outcome" = outcome,
            "st2.driver.producer_version" = producer_version,
        )
    });
    let _guard = span.as_ref().map(tracing::Span::enter);
    tracing::info!(
        stage = stage.as_str(),
        reason = reason.as_str(),
        source = source.as_str(),
        support = support.as_str(),
        outcome,
        producer_version,
        "st2 native driver diagnostic transition"
    );
}

fn atomic_json(path: &Path, value: &impl Serialize) -> std::io::Result<()> {
    let Some(parent) = path.parent() else {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "diagnostic path has no parent",
        ));
    };
    fs::create_dir_all(parent)?;
    let tmp = parent.join(format!(".driver-diagnostic.{}.tmp", std::process::id()));
    let mut file = fs::File::create(&tmp)?;
    serde_json::to_writer(&mut file, value).map_err(std::io::Error::other)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    if let Err(error) = fs::rename(&tmp, path) {
        let _ = fs::remove_file(&tmp);
        return Err(error);
    }
    Ok(())
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_stage_reason_and_source_has_bounded_wire_vocabulary() {
        for stage in Stage::ALL {
            let wire = serde_json::to_value(stage).unwrap();
            assert_eq!(wire, serde_json::Value::String(stage.as_str().to_string()));
            assert_ne!(stage.as_str(), "unknown");
        }
        for reason in Reason::ALL {
            let wire = serde_json::to_value(reason).unwrap();
            assert_eq!(wire, serde_json::Value::String(reason.as_str().to_string()));
            assert_ne!(reason.as_str(), "unknown");
            assert!(Stage::ALL.contains(&reason.stage()));
        }
        for driver in Driver::ALL {
            let wire = serde_json::to_value(driver).unwrap();
            assert_eq!(wire, serde_json::Value::String(driver.as_str().to_string()));
        }
        for source in Source::ALL {
            let wire = serde_json::to_value(source).unwrap();
            assert_eq!(wire, serde_json::Value::String(source.as_str().to_string()));
            assert_ne!(source.as_str(), "unknown");
        }
    }

    #[test]
    fn additive_fields_decode_but_malformed_foreign_and_unknown_records_are_indeterminate() {
        let valid = br#"{
          "schema":"st2.driver-diagnostic.v1","driver":"opencode","stage":"seed",
          "reason":"unknownStatus","source":"statusSnapshot","producerVersion":"1.18.19",
          "support":"supported","observedAt":100,"recovery":"clearsOnStageRecovery",
          "futureField":{"ignored":true}
        }"#;
        let Observed::Failure(failure) = read_at(valid, 125) else {
            panic!("valid additive record must remain readable")
        };
        assert_eq!(failure.evidence_age_ms, 25);
        assert_eq!(failure.stage, Stage::Seed);
        assert_eq!(read_at(b"not json", 0), Observed::Indeterminate(InvalidReason::MalformedRecord));
        assert_eq!(
            read_at(&valid.replace(b"st2.driver-diagnostic.v1", b"st2.driver-diagnostic.v9"), 0),
            Observed::Indeterminate(InvalidReason::UnsupportedSchema)
        );
        assert_eq!(
            read_at(&valid.replace(b"unknownStatus", b"futureReason"), 0),
            Observed::Indeterminate(InvalidReason::UnknownVocabulary)
        );
        assert_eq!(
            read_at(&valid.replace(b"opencode", b"futureDriver"), 0),
            Observed::Indeterminate(InvalidReason::UnknownVocabulary)
        );
        assert_eq!(
            read_at(&valid.replace(b"supported", b"futureSupport"), 0),
            Observed::Indeterminate(InvalidReason::UnknownVocabulary)
        );
        assert_eq!(
            read_at(&valid.replace(b"unknownStatus", b"notDurable"), 0),
            Observed::Indeterminate(InvalidReason::UnknownVocabulary),
            "a known reason on the wrong stage is not valid evidence"
        );
        assert_eq!(
            read_at(
                &valid.replace(b"\"observedAt\":100", b"\"observedAt\":70000"),
                0,
            ),
            Observed::Indeterminate(InvalidReason::FutureSkew)
        );
    }

    #[test]
    fn recovery_clears_only_its_stage_and_reveals_the_next_failure() {
        let tmp = tempfile::tempdir().unwrap();
        let mut publisher = Publisher::new(
            tmp.path(),
            Driver::OpenCode,
            Some("1.18.19".to_string()),
            Support::Supported,
        );
        publisher.publish(Stage::ReadBack, Reason::NotDurable, Source::MessageReadBack);
        publisher.publish(Stage::Sse, Reason::SseDisconnected, Source::EventStream);
        let Observed::Failure(failure) = read(&path(tmp.path())) else { panic!() };
        assert_eq!(failure.stage, Stage::Sse, "earliest boundary wins");

        publisher.clear(Stage::ReadBack);
        let Observed::Failure(failure) = read(&path(tmp.path())) else { panic!() };
        assert_eq!(failure.stage, Stage::Sse, "unrelated recovery cannot clear SSE");

        publisher.clear(Stage::Sse);
        assert_eq!(read(&path(tmp.path())), Observed::Absent);

        fs::write(path(tmp.path()), b"{bad").unwrap();
        assert_eq!(
            read(&path(tmp.path())),
            Observed::Indeterminate(InvalidReason::MalformedRecord)
        );
        let _successor = Publisher::new(
            tmp.path(),
            Driver::OpenCode,
            Some("1.18.19".to_string()),
            Support::Supported,
        );
        assert_eq!(
            read(&path(tmp.path())),
            Observed::Absent,
            "a replacement writer removes an unreadable predecessor snapshot"
        );
    }

    trait ReplaceBytes {
        fn replace(&self, from: &[u8], to: &[u8]) -> Vec<u8>;
    }

    impl ReplaceBytes for [u8] {
        fn replace(&self, from: &[u8], to: &[u8]) -> Vec<u8> {
            let at = self.windows(from.len()).position(|window| window == from).unwrap();
            let mut out = Vec::with_capacity(self.len() - from.len() + to.len());
            out.extend_from_slice(&self[..at]);
            out.extend_from_slice(to);
            out.extend_from_slice(&self[at + from.len()..]);
            out
        }
    }
}
