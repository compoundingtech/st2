//! Resource Profile runtime wire types, framing, and codecs.

use std::collections::BTreeSet;
use std::fmt;
use std::path::PathBuf;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use sha2::{Digest as _, Sha256};

pub const MAX_PROTOCOL_LINE_BYTES: usize = 2 * 1024 * 1024;
pub const MAX_SNAPSHOT_BYTES: usize = 1024 * 1024;
pub const MAX_SELECTOR_BYTES: usize = 16 * 1024;
pub const MAX_HEALTH_DETAIL_BYTES: usize = 16 * 1024;
const MAX_OPAQUE_ID_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpaqueIdError {
    kind: &'static str,
    reason: &'static str,
}

impl fmt::Display for OpaqueIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} {}", self.kind, self.reason)
    }
}

impl std::error::Error for OpaqueIdError {}

macro_rules! opaque_id {
    ($name:ident, $kind:literal) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, OpaqueIdError> {
                let value = value.into();
                if value.is_empty() {
                    return Err(OpaqueIdError {
                        kind: $kind,
                        reason: "must not be empty",
                    });
                }
                if value.len() > MAX_OPAQUE_ID_BYTES {
                    return Err(OpaqueIdError {
                        kind: $kind,
                        reason: "is too large",
                    });
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: Deserializer<'de>,
            {
                let value = String::deserialize(deserializer)?;
                Self::new(value).map_err(de::Error::custom)
            }
        }
    };
}

opaque_id!(RuntimeIncarnation, "runtime incarnation");
opaque_id!(OwnerClaim, "owner claim");
opaque_id!(BindingId, "binding id");
opaque_id!(RegistrationToken, "registration token");

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeOwner {
    incarnation: RuntimeIncarnation,
    claim: OwnerClaim,
}

impl RuntimeOwner {
    pub fn new(incarnation: RuntimeIncarnation, claim: OwnerClaim) -> Self {
        Self { incarnation, claim }
    }

    pub fn incarnation(&self) -> &RuntimeIncarnation {
        &self.incarnation
    }

    pub fn claim(&self) -> &OwnerClaim {
        &self.claim
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct SnapshotBytes(Vec<u8>);

impl fmt::Debug for SnapshotBytes {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SnapshotBytes")
            .field("len", &self.0.len())
            .finish()
    }
}

impl SnapshotBytes {
    pub fn new(bytes: Vec<u8>) -> Result<Self, SnapshotSizeError> {
        if bytes.len() > MAX_SNAPSHOT_BYTES {
            return Err(SnapshotSizeError {
                actual: bytes.len(),
            });
        }
        Ok(Self(bytes))
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotSizeError {
    pub actual: usize,
}

impl fmt::Display for SnapshotSizeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "decoded snapshot is {} bytes; maximum is {MAX_SNAPSHOT_BYTES}",
            self.actual
        )
    }
}

impl std::error::Error for SnapshotSizeError {}

impl Serialize for SnapshotBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&encode_base64(&self.0))
    }
}

impl<'de> Deserialize<'de> for SnapshotBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SnapshotBytesVisitor;

        impl Visitor<'_> for SnapshotBytesVisitor {
            type Value = SnapshotBytes;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an RFC 4648 padded base64 snapshot")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                let bytes = decode_base64(value).map_err(E::custom)?;
                SnapshotBytes::new(bytes).map_err(E::custom)
            }
        }

        deserializer.deserialize_str(SnapshotBytesVisitor)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Base64Error(&'static str);

impl fmt::Display for Base64Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

fn encode_base64(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut encoded = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let first = chunk[0];
        let second = chunk.get(1).copied().unwrap_or(0);
        let third = chunk.get(2).copied().unwrap_or(0);
        encoded.push(ALPHABET[(first >> 2) as usize] as char);
        encoded.push(ALPHABET[(((first & 0x03) << 4) | (second >> 4)) as usize] as char);
        if chunk.len() > 1 {
            encoded.push(ALPHABET[(((second & 0x0f) << 2) | (third >> 6)) as usize] as char);
        } else {
            encoded.push('=');
        }
        if chunk.len() > 2 {
            encoded.push(ALPHABET[(third & 0x3f) as usize] as char);
        } else {
            encoded.push('=');
        }
    }
    encoded
}

fn decode_base64(encoded: &str) -> Result<Vec<u8>, Base64Error> {
    if encoded.len() % 4 != 0 {
        return Err(Base64Error("base64 length is not a multiple of four"));
    }
    let maximum_encoded = MAX_SNAPSHOT_BYTES.div_ceil(3) * 4;
    if encoded.len() > maximum_encoded {
        return Err(Base64Error("decoded snapshot exceeds the size limit"));
    }
    if encoded.is_empty() {
        return Ok(Vec::new());
    }

    fn value(byte: u8) -> Result<u8, Base64Error> {
        match byte {
            b'A'..=b'Z' => Ok(byte - b'A'),
            b'a'..=b'z' => Ok(byte - b'a' + 26),
            b'0'..=b'9' => Ok(byte - b'0' + 52),
            b'+' => Ok(62),
            b'/' => Ok(63),
            _ => Err(Base64Error("base64 contains an invalid character")),
        }
    }

    let input = encoded.as_bytes();
    let padding =
        usize::from(input[input.len() - 1] == b'=') + usize::from(input[input.len() - 2] == b'=');
    let decoded_len = input.len() / 4 * 3 - padding;
    if decoded_len > MAX_SNAPSHOT_BYTES {
        return Err(Base64Error("decoded snapshot exceeds the size limit"));
    }
    let mut decoded = Vec::with_capacity(decoded_len);
    let chunks = input.chunks_exact(4);
    let chunk_count = chunks.len();
    for (index, chunk) in chunks.enumerate() {
        let last = index + 1 == chunk_count;
        let a = value(chunk[0])?;
        let b = value(chunk[1])?;
        decoded.push((a << 2) | (b >> 4));
        match (chunk[2], chunk[3]) {
            (b'=', b'=') if last => {
                if b & 0x0f != 0 {
                    return Err(Base64Error("base64 has non-canonical trailing bits"));
                }
            }
            (third, b'=') if last => {
                let c = value(third)?;
                if c & 0x03 != 0 {
                    return Err(Base64Error("base64 has non-canonical trailing bits"));
                }
                decoded.push((b << 4) | (c >> 2));
            }
            (b'=', _) => return Err(Base64Error("base64 padding is misplaced")),
            (third, fourth) => {
                let c = value(third)?;
                let d = value(fourth)?;
                decoded.push((b << 4) | (c >> 2));
                decoded.push((c << 6) | d);
            }
        }
    }
    Ok(decoded)
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapshotDigest([u8; 32]);

impl SnapshotDigest {
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for SnapshotDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for SnapshotDigest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl Serialize for SnapshotDigest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for SnapshotDigest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct DigestVisitor;

        impl Visitor<'_> for DigestVisitor {
            type Value = SnapshotDigest;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a lowercase 64-character SHA-256 digest")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value.len() != 64 || value.bytes().any(|byte| !byte.is_ascii_hexdigit()) {
                    return Err(E::custom("invalid SHA-256 digest"));
                }
                if value.bytes().any(|byte| byte.is_ascii_uppercase()) {
                    return Err(E::custom("SHA-256 digest must use lowercase hex"));
                }
                let mut digest = [0_u8; 32];
                for (index, pair) in value.as_bytes().chunks_exact(2).enumerate() {
                    let pair = std::str::from_utf8(pair).map_err(E::custom)?;
                    digest[index] = u8::from_str_radix(pair, 16).map_err(E::custom)?;
                }
                Ok(SnapshotDigest(digest))
            }
        }

        deserializer.deserialize_str(DigestVisitor)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum HostMessage {
    Register {
        owner: RuntimeOwner,
        binding_id: BindingId,
        registration: RegistrationToken,
        uri: String,
        selector: Value,
        carrier_path: PathBuf,
        #[serde(skip_serializing_if = "Option::is_none")]
        previous_digest: Option<SnapshotDigest>,
    },
    Unregister {
        owner: RuntimeOwner,
        binding_id: BindingId,
        registration: RegistrationToken,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RuntimeHealthState {
    Starting,
    Ready,
    Degraded,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase",
    deny_unknown_fields
)]
pub enum RuntimeMessage {
    Publish {
        owner: RuntimeOwner,
        binding_id: BindingId,
        registration: RegistrationToken,
        schema_id: String,
        media_type: String,
        bytes: SnapshotBytes,
        topics: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        observed_at: Option<String>,
    },
    Health {
        owner: RuntimeOwner,
        #[serde(skip_serializing_if = "Option::is_none")]
        binding_id: Option<BindingId>,
        #[serde(skip_serializing_if = "Option::is_none")]
        registration: Option<RegistrationToken>,
        state: RuntimeHealthState,
        #[serde(skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
}

#[derive(Debug)]
pub enum ProtocolError {
    MissingNewline,
    MultipleLines,
    EmptyLine,
    LineTooLarge { actual: usize },
    SelectorTooLarge { actual: usize },
    HealthDetailTooLarge { actual: usize },
    InvalidTopics(&'static str),
    InvalidHealthScope,
    Json(serde_json::Error),
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingNewline => formatter.write_str("protocol frame is missing its newline"),
            Self::MultipleLines => formatter.write_str("protocol frame contains multiple lines"),
            Self::EmptyLine => formatter.write_str("protocol frame is empty"),
            Self::LineTooLarge { actual } => write!(
                formatter,
                "protocol line is {actual} bytes; maximum is {MAX_PROTOCOL_LINE_BYTES}"
            ),
            Self::SelectorTooLarge { actual } => write!(
                formatter,
                "selector is {actual} bytes; maximum is {MAX_SELECTOR_BYTES}"
            ),
            Self::HealthDetailTooLarge { actual } => write!(
                formatter,
                "health detail is {actual} bytes; maximum is {MAX_HEALTH_DETAIL_BYTES}"
            ),
            Self::InvalidTopics(reason) => write!(formatter, "invalid topics: {reason}"),
            Self::InvalidHealthScope => formatter
                .write_str("binding-scoped health must carry both bindingId and registration"),
            Self::Json(error) => write!(formatter, "invalid protocol JSON: {error}"),
        }
    }
}

impl std::error::Error for ProtocolError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Json(error) => Some(error),
            _ => None,
        }
    }
}

pub fn decode_host_line(line: &[u8]) -> Result<HostMessage, ProtocolError> {
    let payload = protocol_payload(line)?;
    let message = serde_json::from_slice(payload).map_err(ProtocolError::Json)?;
    validate_host_message(&message)?;
    Ok(message)
}

pub fn decode_runtime_line(line: &[u8]) -> Result<RuntimeMessage, ProtocolError> {
    let payload = protocol_payload(line)?;
    let message = serde_json::from_slice(payload).map_err(ProtocolError::Json)?;
    validate_runtime_message(&message)?;
    Ok(message)
}

pub fn encode_host_line(message: &HostMessage) -> Result<Vec<u8>, ProtocolError> {
    validate_host_message(message)?;
    encode_protocol_line(message)
}

pub fn encode_runtime_line(message: &RuntimeMessage) -> Result<Vec<u8>, ProtocolError> {
    validate_runtime_message(message)?;
    encode_protocol_line(message)
}

fn protocol_payload(line: &[u8]) -> Result<&[u8], ProtocolError> {
    if line.len() > MAX_PROTOCOL_LINE_BYTES {
        return Err(ProtocolError::LineTooLarge { actual: line.len() });
    }
    let Some(payload) = line.strip_suffix(b"\n") else {
        return Err(ProtocolError::MissingNewline);
    };
    if payload.is_empty() {
        return Err(ProtocolError::EmptyLine);
    }
    if payload.contains(&b'\n') || payload.contains(&b'\r') {
        return Err(ProtocolError::MultipleLines);
    }
    Ok(payload)
}

fn encode_protocol_line(message: &impl Serialize) -> Result<Vec<u8>, ProtocolError> {
    let mut line = serde_json::to_vec(message).map_err(ProtocolError::Json)?;
    line.push(b'\n');
    if line.len() > MAX_PROTOCOL_LINE_BYTES {
        return Err(ProtocolError::LineTooLarge { actual: line.len() });
    }
    Ok(line)
}

fn validate_host_message(message: &HostMessage) -> Result<(), ProtocolError> {
    if let HostMessage::Register { selector, .. } = message {
        let actual = serde_json::to_vec(selector)
            .map_err(ProtocolError::Json)?
            .len();
        if actual > MAX_SELECTOR_BYTES {
            return Err(ProtocolError::SelectorTooLarge { actual });
        }
    }
    Ok(())
}

fn validate_runtime_message(message: &RuntimeMessage) -> Result<(), ProtocolError> {
    match message {
        RuntimeMessage::Publish { topics, .. } => validate_topics(topics),
        RuntimeMessage::Health {
            binding_id,
            registration,
            detail,
            ..
        } => {
            if binding_id.is_some() != registration.is_some() {
                return Err(ProtocolError::InvalidHealthScope);
            }
            if let Some(detail) = detail
                && detail.len() > MAX_HEALTH_DETAIL_BYTES
            {
                return Err(ProtocolError::HealthDetailTooLarge {
                    actual: detail.len(),
                });
            }
            Ok(())
        }
    }
}

fn validate_topics(topics: &[String]) -> Result<(), ProtocolError> {
    let mut unique = BTreeSet::new();
    for topic in topics {
        if topic.is_empty() {
            return Err(ProtocolError::InvalidTopics(
                "topic names must not be empty",
            ));
        }
        if !unique.insert(topic.as_str()) {
            return Err(ProtocolError::InvalidTopics("topic names must be unique"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn owner() -> RuntimeOwner {
        RuntimeOwner::new(
            RuntimeIncarnation::new("incarnation").unwrap(),
            OwnerClaim::new("claim").unwrap(),
        )
    }

    fn publish(bytes: &[u8]) -> RuntimeMessage {
        RuntimeMessage::Publish {
            owner: owner(),
            binding_id: BindingId::new("binding").unwrap(),
            registration: RegistrationToken::new("registration").unwrap(),
            schema_id: "schema.v1".to_owned(),
            media_type: "application/json".to_owned(),
            bytes: SnapshotBytes::new(bytes.to_vec()).unwrap(),
            topics: vec!["selected".to_owned()],
            observed_at: Some("2026-08-30T00:00:00Z".to_owned()),
        }
    }

    #[test]
    fn host_frames_have_exact_json_shape_and_newline() {
        let register = HostMessage::Register {
            owner: owner(),
            binding_id: BindingId::new("binding").unwrap(),
            registration: RegistrationToken::new("registration").unwrap(),
            uri: "resource://one".to_owned(),
            selector: json!({"kind": "one"}),
            carrier_path: PathBuf::from("resources/one.json"),
            previous_digest: Some(SnapshotDigest::of(b"previous")),
        };
        assert_eq!(
            encode_host_line(&register).unwrap(),
            b"{\"type\":\"register\",\"owner\":{\"incarnation\":\"incarnation\",\"claim\":\"claim\"},\"bindingId\":\"binding\",\"registration\":\"registration\",\"uri\":\"resource://one\",\"selector\":{\"kind\":\"one\"},\"carrierPath\":\"resources/one.json\",\"previousDigest\":\"6da0633528deaa0144e7b058315f0b753ec0b945163a72bf96a0d18180f9de0d\"}\n"
        );
        let unregister = HostMessage::Unregister {
            owner: owner(),
            binding_id: BindingId::new("binding").unwrap(),
            registration: RegistrationToken::new("registration").unwrap(),
        };
        assert_eq!(
            encode_host_line(&unregister).unwrap(),
            b"{\"type\":\"unregister\",\"owner\":{\"incarnation\":\"incarnation\",\"claim\":\"claim\"},\"bindingId\":\"binding\",\"registration\":\"registration\"}\n"
        );
    }

    #[test]
    fn runtime_frames_have_exact_json_shape_and_padded_base64() {
        assert_eq!(
            encode_runtime_line(&publish(b"one byte")).unwrap(),
            b"{\"type\":\"publish\",\"owner\":{\"incarnation\":\"incarnation\",\"claim\":\"claim\"},\"bindingId\":\"binding\",\"registration\":\"registration\",\"schemaId\":\"schema.v1\",\"mediaType\":\"application/json\",\"bytes\":\"b25lIGJ5dGU=\",\"topics\":[\"selected\"],\"observedAt\":\"2026-08-30T00:00:00Z\"}\n"
        );
        let health = RuntimeMessage::Health {
            owner: owner(),
            binding_id: None,
            registration: None,
            state: RuntimeHealthState::Ready,
            detail: None,
        };
        assert_eq!(
            encode_runtime_line(&health).unwrap(),
            b"{\"type\":\"health\",\"owner\":{\"incarnation\":\"incarnation\",\"claim\":\"claim\"},\"state\":\"ready\"}\n"
        );
        assert_eq!(
            decode_runtime_line(&encode_runtime_line(&publish(b"one byte")).unwrap()).unwrap(),
            publish(b"one byte")
        );
    }

    #[test]
    fn decoding_is_strict_about_fields_ids_and_digest_encoding() {
        let unknown_message_field = b"{\"type\":\"health\",\"owner\":{\"incarnation\":\"i\",\"claim\":\"c\"},\"state\":\"ready\",\"extra\":true}\n";
        assert!(matches!(
            decode_runtime_line(unknown_message_field),
            Err(ProtocolError::Json(_))
        ));
        let unknown_owner_field = b"{\"type\":\"health\",\"owner\":{\"incarnation\":\"i\",\"claim\":\"c\",\"extra\":true},\"state\":\"ready\"}\n";
        assert!(matches!(
            decode_runtime_line(unknown_owner_field),
            Err(ProtocolError::Json(_))
        ));
        let empty_id = b"{\"type\":\"unregister\",\"owner\":{\"incarnation\":\"\",\"claim\":\"c\"},\"bindingId\":\"b\",\"registration\":\"r\"}\n";
        assert!(matches!(
            decode_host_line(empty_id),
            Err(ProtocolError::Json(_))
        ));
        let uppercase_digest = b"{\"type\":\"register\",\"owner\":{\"incarnation\":\"i\",\"claim\":\"c\"},\"bindingId\":\"b\",\"registration\":\"r\",\"uri\":\"u\",\"selector\":{},\"carrierPath\":\"p\",\"previousDigest\":\"AFA459DEEC028BA69A538AB3DF3ED61A63F9C8383C2FDAFA95ED544547FB675D\"}\n";
        assert!(matches!(
            decode_host_line(uppercase_digest),
            Err(ProtocolError::Json(_))
        ));
    }

    #[test]
    fn decoding_rejects_malformed_frames() {
        assert!(matches!(
            decode_runtime_line(b"{}"),
            Err(ProtocolError::MissingNewline)
        ));
        assert!(matches!(
            decode_runtime_line(b"\n"),
            Err(ProtocolError::EmptyLine)
        ));
        assert!(matches!(
            decode_runtime_line(b"{}\n{}\n"),
            Err(ProtocolError::MultipleLines)
        ));
        assert!(matches!(
            decode_runtime_line(b"{}\r\n"),
            Err(ProtocolError::MultipleLines)
        ));
        assert!(matches!(
            decode_runtime_line(b"not json\n"),
            Err(ProtocolError::Json(_))
        ));
    }

    #[test]
    fn snapshot_base64_rejects_malformed_and_noncanonical_values() {
        for encoded in ["A", "!!!!", "A===", "AB==", "AAB=", "AA=A", "AA==AAAA"] {
            let line = format!(
                "{{\"type\":\"publish\",\"owner\":{{\"incarnation\":\"i\",\"claim\":\"c\"}},\"bindingId\":\"b\",\"registration\":\"r\",\"schemaId\":\"s\",\"mediaType\":\"m\",\"bytes\":\"{encoded}\",\"topics\":[]}}\n"
            );
            assert!(
                matches!(
                    decode_runtime_line(line.as_bytes()),
                    Err(ProtocolError::Json(_))
                ),
                "accepted {encoded}"
            );
        }
    }

    #[test]
    fn protocol_enforces_all_byte_limits() {
        assert!(matches!(
            decode_runtime_line(&vec![b'x'; MAX_PROTOCOL_LINE_BYTES + 1]),
            Err(ProtocolError::LineTooLarge { actual }) if actual == MAX_PROTOCOL_LINE_BYTES + 1
        ));
        assert_eq!(
            SnapshotBytes::new(vec![0; MAX_SNAPSHOT_BYTES + 1]).unwrap_err(),
            SnapshotSizeError {
                actual: MAX_SNAPSHOT_BYTES + 1
            }
        );
        let oversized_snapshot = encode_base64(&vec![0; MAX_SNAPSHOT_BYTES + 1]);
        let snapshot_line = format!(
            "{{\"type\":\"publish\",\"owner\":{{\"incarnation\":\"i\",\"claim\":\"c\"}},\"bindingId\":\"b\",\"registration\":\"r\",\"schemaId\":\"s\",\"mediaType\":\"m\",\"bytes\":\"{oversized_snapshot}\",\"topics\":[]}}\n"
        );
        assert!(matches!(
            decode_runtime_line(snapshot_line.as_bytes()),
            Err(ProtocolError::Json(_))
        ));

        let register = HostMessage::Register {
            owner: owner(),
            binding_id: BindingId::new("binding").unwrap(),
            registration: RegistrationToken::new("registration").unwrap(),
            uri: "u".to_owned(),
            selector: Value::String("x".repeat(MAX_SELECTOR_BYTES)),
            carrier_path: PathBuf::from("p"),
            previous_digest: None,
        };
        assert!(matches!(
            encode_host_line(&register),
            Err(ProtocolError::SelectorTooLarge { .. })
        ));
        let mut register_line = serde_json::to_vec(&register).unwrap();
        register_line.push(b'\n');
        assert!(matches!(
            decode_host_line(&register_line),
            Err(ProtocolError::SelectorTooLarge { .. })
        ));

        let health = RuntimeMessage::Health {
            owner: owner(),
            binding_id: None,
            registration: None,
            state: RuntimeHealthState::Degraded,
            detail: Some("x".repeat(MAX_HEALTH_DETAIL_BYTES + 1)),
        };
        assert!(matches!(
            encode_runtime_line(&health),
            Err(ProtocolError::HealthDetailTooLarge { .. })
        ));
        let mut health_line = serde_json::to_vec(&health).unwrap();
        health_line.push(b'\n');
        assert!(matches!(
            decode_runtime_line(&health_line),
            Err(ProtocolError::HealthDetailTooLarge { .. })
        ));
        assert!(RuntimeIncarnation::new("x".repeat(MAX_OPAQUE_ID_BYTES + 1)).is_err());
    }

    #[test]
    fn runtime_semantic_validation_applies_on_encode_and_decode() {
        let invalid_health = RuntimeMessage::Health {
            owner: owner(),
            binding_id: Some(BindingId::new("binding").unwrap()),
            registration: None,
            state: RuntimeHealthState::Failed,
            detail: None,
        };
        assert!(matches!(
            encode_runtime_line(&invalid_health),
            Err(ProtocolError::InvalidHealthScope)
        ));
        let mut duplicate_topics = publish(b"bytes");
        let RuntimeMessage::Publish { topics, .. } = &mut duplicate_topics else {
            unreachable!();
        };
        *topics = vec!["same".to_owned(), "same".to_owned()];
        assert!(matches!(
            encode_runtime_line(&duplicate_topics),
            Err(ProtocolError::InvalidTopics(_))
        ));
    }
}
