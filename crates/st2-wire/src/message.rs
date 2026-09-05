//! The rows `st2 message ls --json` and `st2 message read --json` emit.

use serde::{Deserialize, Serialize};

/// One message as st2's CLI reports it.
///
/// The same type serializes in st2 and deserializes in every reader, which is the point: a reader
/// cannot disagree with the producer about which fields exist or which of them may be absent.
///
/// # What is optional, and why it is not the same as empty
///
/// `from`, `subject`, `in_reply_to` and `priority` are `Option` because the message file's
/// frontmatter genuinely may not carry them — a message sent without `--subject` has no subject,
/// and st2 emits `null`. That is distinct from a message whose subject is the empty string, which
/// is a subject that happens to say nothing. The distinction is preserved here rather than
/// flattened, so a reader can choose how to present each; collapsing them into `""` would discard
/// the difference before the reader ever saw it.
///
/// `filename` and `ts` are not optional. Both are derived by st2 from the message's filename —
/// `ts` from its unix-ms prefix, falling back to `0` — so neither can be absent from a row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageRow {
    /// `<unix-ms>-<rand6>.md` — the message's identity within its box.
    pub filename: String,
    /// Send time in unix ms, from the filename prefix.
    pub ts: u64,
    /// The claimed sender, absent when the frontmatter carries none.
    pub from: Option<String>,
    pub subject: Option<String>,
    /// The filename of the message this replies to.
    #[serde(rename = "inReplyTo")]
    pub in_reply_to: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub priority: Option<String>,
    /// The caller's optional operation identity for exact retry semantics.
    #[serde(
        rename = "idempotencyKey",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub idempotency_key: Option<String>,
    /// The declared stream that produced this inbox event, when this is an event record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream: Option<String>,
    /// The producer-owned stable identity of this event.
    #[serde(rename = "eventId", default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    /// The optional stream-local supersession key.
    #[serde(rename = "eventKey", default, skip_serializing_if = "Option::is_none")]
    pub event_key: Option<String>,
    /// The markdown body.
    ///
    /// Absent — the key omitted entirely, not `null` — from a `ls --json` row unless
    /// `--include-body` was passed, because a list of bodies is a different and much larger
    /// request than a list of messages. `read --json` always carries it. A reader must therefore
    /// treat "no body" as "not asked for", never as "the message is empty".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
}

/// How much sender history st2 can prove is represented by [`SentMessages`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "_tag", rename_all = "kebab-case")]
pub enum SentCoverage {
    /// This sender has no index marker, so an empty row set says nothing about earlier sends.
    Unavailable,
    /// Every successful send at or after `since` is represented.
    Since { since: u64 },
    /// One or more durable send intents have not reached a terminal sender-owned state.
    Partial { since: u64, pending: usize },
}

/// Which kind of bus endpoint a durable message field names.
///
/// This is what stops a reader from mistaking a canonical service-principal or external route for
/// an agent ID. An `agent` endpoint carries an immutable agent ID; every other kind carries that
/// endpoint's own canonical address. Absent on a version-1 row, which means `agent`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EndpointKind {
    /// A declared Agent. The endpoint value is its catalog-global immutable agent ID.
    Agent,
    /// A declared service principal. The endpoint value is its canonical `<host>.<identity>` route.
    Principal,
    /// An eval-owned external requester mailbox. The endpoint value is its canonical route.
    External,
}

impl EndpointKind {
    /// The meaning of an absent `fromKind`/`toKind`: a version-1 row only ever carried agents.
    pub const fn or_version_1_default(kind: Option<Self>) -> Self {
        match kind {
            Some(kind) => kind,
            None => Self::Agent,
        }
    }
}

/// One sender-owned message row. The sender is the selected index owner; `to` is the directional
/// field that varies between rows.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SentMessageRow {
    pub filename: String,
    pub ts: u64,
    pub to: String,
    pub subject: Option<String>,
    #[serde(rename = "inReplyTo")]
    pub in_reply_to: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub priority: Option<String>,
    #[serde(
        rename = "idempotencyKey",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    pub idempotency_key: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<String>,
    /// The recipient's bus address at publication time.
    ///
    /// Display only, and only meaningful for an `agent` endpoint: an address is mutable and a
    /// released address is immediately reusable, so this is never a selector and never authority.
    #[serde(rename = "toAddress", default, skip_serializing_if = "Option::is_none")]
    pub to_address: Option<String>,
    /// Whether `to` is an agent ID or a canonical non-Agent address. Absent means `agent`.
    #[serde(rename = "toKind", default, skip_serializing_if = "Option::is_none")]
    pub to_kind: Option<EndpointKind>,
}

/// The stable `st2 message sent --json` envelope.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SentMessages {
    pub coverage: SentCoverage,
    pub messages: Vec<SentMessageRow>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row() -> MessageRow {
        MessageRow {
            filename: "1785000000000-abcdef.md".to_string(),
            ts: 1785000000000,
            from: Some("agent.example".to_string()),
            subject: Some("a subject".to_string()),
            in_reply_to: None,
            tags: Vec::new(),
            priority: None,
            idempotency_key: None,
            stream: None,
            event_id: None,
            event_key: None,
            body: None,
        }
    }

    /// The bug this crate exists to prevent: a reader that cannot accept what st2 emits. A message
    /// sent without `--subject` is ordinary, and `null` must round-trip to `None` rather than fail.
    #[test]
    fn a_null_subject_or_sender_round_trips_as_absent() {
        let absent = MessageRow {
            from: None,
            subject: None,
            ..row()
        };
        let json = serde_json::to_string(&absent).expect("serialize");
        assert!(json.contains(r#""subject":null"#), "{json}");
        assert!(json.contains(r#""from":null"#), "{json}");
        assert_eq!(
            serde_json::from_str::<MessageRow>(&json).expect("deserialize"),
            absent
        );
    }

    /// Absent and empty are different claims and must not collapse into each other.
    #[test]
    fn an_empty_subject_is_not_the_same_as_no_subject() {
        let empty = MessageRow {
            subject: Some(String::new()),
            ..row()
        };
        let none = MessageRow {
            subject: None,
            ..row()
        };
        assert_ne!(empty, none);
        for value in [empty, none] {
            let json = serde_json::to_string(&value).expect("serialize");
            assert_eq!(
                serde_json::from_str::<MessageRow>(&json).expect("deserialize"),
                value
            );
        }
    }

    /// `body` is omitted rather than nulled when it was not requested, and a reader must not read
    /// that absence as an empty message.
    #[test]
    fn an_unrequested_body_is_omitted_from_the_wire_entirely() {
        let json = serde_json::to_string(&row()).expect("serialize");
        assert!(!json.contains("body"), "{json}");
        assert_eq!(
            serde_json::from_str::<MessageRow>(&json)
                .expect("deserialize")
                .body,
            None
        );

        let with_body = MessageRow {
            body: Some(String::new()),
            ..row()
        };
        let json = serde_json::to_string(&with_body).expect("serialize");
        assert!(json.contains(r#""body":"""#), "{json}");
    }

    #[test]
    fn event_identity_round_trips_without_changing_ordinary_message_rows() {
        let ordinary = serde_json::to_value(row()).unwrap();
        assert!(ordinary.get("stream").is_none());
        assert!(ordinary.get("eventId").is_none());
        assert!(ordinary.get("eventKey").is_none());

        let event = MessageRow {
            stream: Some("gh-ci".to_string()),
            event_id: Some("run-812".to_string()),
            event_key: Some("main".to_string()),
            ..row()
        };
        let json = serde_json::to_value(&event).unwrap();
        assert_eq!(json["stream"], "gh-ci");
        assert_eq!(json["eventId"], "run-812");
        assert_eq!(json["eventKey"], "main");
        assert_eq!(serde_json::from_value::<MessageRow>(json).unwrap(), event);
    }

    /// An ABSENT optional key and an explicit `null` both mean "not carried", and both must parse.
    ///
    /// This is the distinction the defect turned on: `#[serde(default)]` covers only the absent
    /// case, so a hand-written reader that used it accepted a missing `subject` and rejected the
    /// `null` st2 actually emits. `Option` covers both, and this test is what stops anyone
    /// "simplifying" these fields back to a defaulted `String`.
    #[test]
    fn an_absent_optional_key_parses_the_same_as_an_explicit_null() {
        let absent = r#"{"filename":"1785000000000-abcdef.md","ts":1785000000000,"tags":[]}"#;
        let nulled = r#"{"filename":"1785000000000-abcdef.md","ts":1785000000000,"tags":[],
            "from":null,"subject":null,"inReplyTo":null,"priority":null}"#;
        let absent = serde_json::from_str::<MessageRow>(absent).expect("absent keys parse");
        let nulled = serde_json::from_str::<MessageRow>(nulled).expect("null keys parse");
        assert_eq!(absent, nulled);
        assert_eq!(absent.subject, None);
        assert_eq!(absent.from, None);
    }

    /// A reader pinned to this crate may be older than the `st2` binary it invokes, so a field it
    /// has never heard of must be ignored rather than fatal.
    #[test]
    fn a_field_this_reader_does_not_know_is_ignored_not_rejected() {
        let json = r#"{"filename":"1785000000000-abcdef.md","ts":1785000000000,"from":null,
            "subject":null,"inReplyTo":null,"tags":[],"priority":null,"somethingAddedLater":42}"#;
        let parsed = serde_json::from_str::<MessageRow>(json).expect("unknown fields are ignored");
        assert_eq!(parsed.filename, "1785000000000-abcdef.md");
    }

    #[test]
    fn sent_rows_carry_to_and_coverage_never_collapses_unavailable_into_empty() {
        let unavailable = SentMessages {
            coverage: SentCoverage::Unavailable,
            messages: Vec::new(),
        };
        assert_eq!(
            serde_json::to_value(&unavailable).unwrap(),
            serde_json::json!({
                "coverage": { "_tag": "unavailable" },
                "messages": [],
            })
        );

        let indexed = SentMessages {
            coverage: SentCoverage::Since {
                since: 1_785_000_000_000,
            },
            messages: vec![SentMessageRow {
                filename: "1785000000000-abcdef.md".to_string(),
                ts: 1_785_000_000_000,
                to: "h.recipient".to_string(),
                subject: None,
                in_reply_to: None,
                tags: Vec::new(),
                priority: None,
                idempotency_key: None,
                body: None,
                to_address: None,
                to_kind: None,
            }],
        };
        let json = serde_json::to_value(indexed).unwrap();
        assert_eq!(json["messages"][0]["to"], "h.recipient");
        assert!(json["messages"][0].get("from").is_none());
        assert!(json["messages"][0].get("body").is_none());
    }

    /// A row emitted before typed endpoints existed carries neither key, and must still be read as
    /// an agent endpoint — never as an unknown or non-Agent one.
    #[test]
    fn a_version_1_sent_row_without_to_kind_decodes_as_an_agent_endpoint() {
        let json = r#"{"filename":"1785000000000-abcdef.md","ts":1785000000000,
            "to":"h.recipient","subject":null,"inReplyTo":null,"tags":[],"priority":null}"#;
        let row = serde_json::from_str::<SentMessageRow>(json).expect("a version-1 row parses");
        assert_eq!(row.to_kind, None);
        assert_eq!(row.to_address, None);
        assert_eq!(
            EndpointKind::or_version_1_default(row.to_kind),
            EndpointKind::Agent
        );
        // Absent stays absent on re-emission: a reader must not invent a kind it was not told.
        let reemitted = serde_json::to_value(&row).unwrap();
        assert!(reemitted.get("toKind").is_none());
        assert!(reemitted.get("toAddress").is_none());
    }

    /// A typed non-Agent endpoint keeps its canonical address in `to` and says so, so no reader
    /// can mistake that address for an agent ID.
    #[test]
    fn a_typed_non_agent_sent_row_round_trips_its_kind_and_snapshot() {
        let row = SentMessageRow {
            filename: "1785000000000-abcdef.md".to_string(),
            ts: 1_785_000_000_000,
            to: "h.billing".to_string(),
            subject: None,
            in_reply_to: None,
            tags: Vec::new(),
            priority: None,
            idempotency_key: None,
            body: None,
            to_address: None,
            to_kind: Some(EndpointKind::Principal),
        };
        let json = serde_json::to_value(&row).unwrap();
        assert_eq!(json["toKind"], "principal");
        assert_eq!(serde_json::from_value::<SentMessageRow>(json).unwrap(), row);

        let agent = SentMessageRow {
            to: "0199b8f4-8d3a-7c21-9a44-6f85b7320ea1".to_string(),
            to_address: Some("dev4.fractal.chat".to_string()),
            to_kind: Some(EndpointKind::Agent),
            ..row
        };
        let json = serde_json::to_value(&agent).unwrap();
        assert_eq!(json["toKind"], "agent");
        assert_eq!(json["toAddress"], "dev4.fractal.chat");
        assert_eq!(
            serde_json::from_value::<SentMessageRow>(json).unwrap(),
            agent
        );
    }
}
