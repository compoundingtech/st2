//! Agent identity — the immutable catalog-global agent ID and the mutable host-local address.
//!
//! Decision 0015 splits one overloaded `identity` into four values with different mutability and
//! scope (root `spec.md`, "Immutable agent ID, mutable address, and presentation"):
//!
//! | value | form | mutability | scope | use |
//! |---|---|---|---|---|
//! | agent ID | `id "<agent-id>"` | immutable | catalog-global | subject, ownership, automation, graph edges |
//! | agent address | `address "<address>"`, else positional `identity` | mutable | unique per logical host | human routing |
//! | bus address | `<host>.<effective-address>` | derived | catalog | qualified human routing |
//! | presentation | `name` / `description` | mutable | non-unique | display only |
//!
//! Two rules make this module small. First, an ID is **opaque**: a frozen legacy ID literally *is*
//! the subject's former `<host>.<identity>` bus identity, so the type never asserts that an ID is a
//! UUID nor that its dots mean placement. Second, ID and address are **separate typed namespaces**
//! — equal bytes never collide, so nothing here compares one against the other.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::Read as _;
use std::time::{SystemTime, UNIX_EPOCH};

/// Maximum byte length of an agent ID. A frozen legacy ID is `<host>.<identity>`, so this bound has
/// to be at least as generous as the address bound it may have been derived from.
pub const AGENT_ID_MAX_BYTES: usize = 255;

/// Maximum character length of an explicit agent address (root `spec.md`, F20).
pub const AGENT_ADDRESS_MAX_CHARS: usize = 255;

/// Maximum character length of one dotted address segment.
pub const AGENT_ADDRESS_SEGMENT_MAX_CHARS: usize = 63;

/// A catalog-global immutable agent ID.
///
/// The value is opaque. New subjects receive [`AgentId::generate`] (UUIDv7); migrated legacy
/// subjects receive [`AgentId::frozen_legacy`], which preserves their existing runtime identifiers
/// byte for byte. A later host move changes placement and bus address, never the ID.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentId(String);

impl AgentId {
    /// Admit one declared or migrated agent ID.
    ///
    /// The grammar is deliberately wider than the address grammar because a frozen legacy ID
    /// carries whatever bytes that subject's `<host>.<identity>` already used. It is narrow enough
    /// that an ID stays usable as a PTY task ID and as a shell-free CLI argument: printable ASCII,
    /// no whitespace, no path separators, and no leading or trailing dot.
    pub fn parse(value: &str) -> Result<Self, IdentityError> {
        if value.is_empty() {
            return Err(IdentityError::EmptyId);
        }
        if value.len() > AGENT_ID_MAX_BYTES {
            return Err(IdentityError::IdTooLong {
                bytes: value.len(),
                max: AGENT_ID_MAX_BYTES,
            });
        }
        for byte in value.bytes() {
            if !byte.is_ascii_graphic() {
                return Err(IdentityError::IdByte { byte });
            }
            if byte == b'/' || byte == b'\\' {
                return Err(IdentityError::IdByte { byte });
            }
        }
        if value.starts_with('.') || value.ends_with('.') || value.contains("..") {
            return Err(IdentityError::IdDots);
        }
        Ok(Self(value.to_owned()))
    }

    /// Generate a fresh UUIDv7 agent ID for a brand-new subject.
    pub fn generate() -> Result<Self, IdentityError> {
        Ok(Self(uuid_v7()?))
    }

    /// Freeze one legacy subject's existing host-qualified bus identity as its explicit ID.
    ///
    /// This is the migration primitive: the bytes are exactly what runtime ownership, task IDs, and
    /// declaration-anchored state already use, so migration moves no state.
    pub fn frozen_legacy(host: &str, identity: &str) -> Result<Self, IdentityError> {
        Self::parse(&legacy_bus_identity(host, identity))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_string(self) -> String {
        self.0
    }
}

impl fmt::Display for AgentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl serde::Serialize for AgentId {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for AgentId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

/// A mutable host-local semantic address used for ordinary human routing.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AgentAddress(String);

impl AgentAddress {
    /// Admit one explicit address: at most 255 ASCII characters, a dotted sequence of 1..=63
    /// character segments of lowercase letters, digits, and hyphens, each starting and ending with a
    /// letter or digit (root `spec.md`, F20).
    pub fn parse(value: &str) -> Result<Self, IdentityError> {
        if value.is_empty() {
            return Err(IdentityError::EmptyAddress);
        }
        if !value.is_ascii() {
            return Err(IdentityError::AddressNotAscii);
        }
        if value.len() > AGENT_ADDRESS_MAX_CHARS {
            return Err(IdentityError::AddressTooLong {
                chars: value.len(),
                max: AGENT_ADDRESS_MAX_CHARS,
            });
        }
        for segment in value.split('.') {
            validate_address_segment(segment)?;
        }
        Ok(Self(value.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AgentAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl serde::Serialize for AgentAddress {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> serde::Deserialize<'de> for AgentAddress {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(&raw).map_err(serde::de::Error::custom)
    }
}

fn validate_address_segment(segment: &str) -> Result<(), IdentityError> {
    if segment.is_empty() {
        return Err(IdentityError::AddressEmptySegment);
    }
    if segment.len() > AGENT_ADDRESS_SEGMENT_MAX_CHARS {
        return Err(IdentityError::AddressSegmentTooLong {
            segment: segment.to_owned(),
            max: AGENT_ADDRESS_SEGMENT_MAX_CHARS,
        });
    }
    let bytes = segment.as_bytes();
    let boundary_ok = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    if !boundary_ok(bytes[0]) || !boundary_ok(bytes[bytes.len() - 1]) {
        return Err(IdentityError::AddressSegmentBoundary {
            segment: segment.to_owned(),
        });
    }
    for &byte in bytes {
        if !(byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-') {
            return Err(IdentityError::AddressSegmentByte {
                segment: segment.to_owned(),
                byte,
            });
        }
    }
    Ok(())
}

/// The positional declaration key `<host>.<identity>`.
///
/// This is both the legacy address fallback and the bytes migration freezes as a legacy ID. It is
/// *not* the agent ID of an already migrated subject.
pub fn legacy_bus_identity(host: &str, identity: &str) -> String {
    if host.is_empty() {
        identity.to_owned()
    } else {
        format!("{host}.{identity}")
    }
}

/// Join a host and an effective address into the qualified bus address.
pub fn bus_address(host: &str, effective_address: &str) -> String {
    legacy_bus_identity(host, effective_address)
}

/// Every way one identity value can be rejected at the shared parse/authoring boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentityError {
    EmptyId,
    IdTooLong { bytes: usize, max: usize },
    IdByte { byte: u8 },
    IdDots,
    EmptyAddress,
    AddressNotAscii,
    AddressTooLong { chars: usize, max: usize },
    AddressEmptySegment,
    AddressSegmentTooLong { segment: String, max: usize },
    AddressSegmentBoundary { segment: String },
    AddressSegmentByte { segment: String, byte: u8 },
    RandomnessUnavailable { detail: String },
}

impl fmt::Display for IdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyId => write!(f, "agent `id` must not be empty"),
            Self::IdTooLong { bytes, max } => {
                write!(f, "agent `id` is {bytes} bytes; the limit is {max}")
            }
            Self::IdByte { byte } => write!(
                f,
                "agent `id` contains byte 0x{byte:02x}; an id is printable ASCII without \
                 whitespace or path separators"
            ),
            Self::IdDots => write!(
                f,
                "agent `id` must not begin or end with `.` or contain an empty dotted segment"
            ),
            Self::EmptyAddress => write!(f, "agent `address` must not be empty"),
            Self::AddressNotAscii => write!(f, "agent `address` must be ASCII"),
            Self::AddressTooLong { chars, max } => {
                write!(f, "agent `address` is {chars} characters; the limit is {max}")
            }
            Self::AddressEmptySegment => {
                write!(f, "agent `address` must not contain an empty dotted segment")
            }
            Self::AddressSegmentTooLong { segment, max } => write!(
                f,
                "agent `address` segment '{segment}' is longer than {max} characters"
            ),
            Self::AddressSegmentBoundary { segment } => write!(
                f,
                "agent `address` segment '{segment}' must begin and end with a lowercase letter \
                 or digit"
            ),
            Self::AddressSegmentByte { segment, byte } => write!(
                f,
                "agent `address` segment '{segment}' contains byte 0x{byte:02x}; segments use \
                 lowercase letters, digits, and hyphens"
            ),
            Self::RandomnessUnavailable { detail } => {
                write!(f, "cannot generate an agent id without randomness: {detail}")
            }
        }
    }
}

impl std::error::Error for IdentityError {}

/// One admitted subject as the address book sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subject {
    /// Catalog-global immutable ID.
    pub id: AgentId,
    /// Resolved logical host.
    pub host: String,
    /// Effective address: the explicit `address`, else the positional `identity` fallback.
    pub effective_address: String,
    /// Whether ordinary address routing may reach this subject. Retired subjects are non-routable
    /// and release their address.
    pub routable: bool,
}

impl Subject {
    /// The qualified human route, or `None` for a proved non-routable subject.
    pub fn bus_address(&self) -> Option<String> {
        self.routable
            .then(|| bus_address(&self.host, &self.effective_address))
    }
}

/// How a caller named one subject. The two forms are disjoint by construction: an exact ID never
/// falls through to address lookup, and an ordinary reference never reaches the ID namespace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentSelector {
    /// An explicit typed ID. Catalog-global exact lookup only.
    Id(String),
    /// An ordinary human reference: a bare address or a host-qualified bus address.
    Address {
        reference: String,
        /// A caller-pinned host, when the command supplied one.
        host: Option<String>,
    },
}

impl AgentSelector {
    pub fn id(value: impl Into<String>) -> Self {
        Self::Id(value.into())
    }

    pub fn address(reference: impl Into<String>) -> Self {
        Self::Address {
            reference: reference.into(),
            host: None,
        }
    }

    pub fn address_on_host(reference: impl Into<String>, host: impl Into<String>) -> Self {
        Self::Address {
            reference: reference.into(),
            host: Some(host.into()),
        }
    }

    /// The caller's literal input, for diagnostics.
    pub fn as_input(&self) -> &str {
        match self {
            Self::Id(id) => id,
            Self::Address { reference, .. } => reference,
        }
    }
}

/// Why one selector did not name exactly one subject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    /// No subject carries this ID.
    UnknownId { id: String },
    /// More than one subject claims this ID. A catalog-global ID is unique by contract, so this is
    /// a broken catalog rather than a selection to disambiguate.
    AmbiguousId { id: String, count: usize },
    /// No routable subject answers this ordinary reference.
    UnknownAddress {
        reference: String,
        host: Option<String>,
    },
    /// The reference is decidable as more than one distinct subject.
    AmbiguousAddress {
        reference: String,
        candidates: Vec<String>,
    },
}

impl fmt::Display for ResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownId { id } => {
                write!(f, "no agent with id '{id}' exists in the selected catalog")
            }
            Self::AmbiguousId { id, count } => write!(
                f,
                "agent id '{id}' is claimed by {count} declarations; a catalog-global id must be \
                 unique, so the catalog is broken rather than the selector ambiguous"
            ),
            Self::UnknownAddress {
                reference,
                host: Some(host),
            } => write!(
                f,
                "no routable agent answers address '{reference}' on host '{host}'"
            ),
            Self::UnknownAddress {
                reference,
                host: None,
            } => write!(f, "no routable agent answers address '{reference}'"),
            Self::AmbiguousAddress {
                reference,
                candidates,
            } => write!(
                f,
                "address '{reference}' is ambiguous across {} subjects: {}",
                candidates.len(),
                candidates.join(", ")
            ),
        }
    }
}

impl std::error::Error for ResolveError {}

/// One admitted uniqueness violation in a complete prospective catalog.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UniquenessConflict {
    /// Two subjects declare the same catalog-global ID.
    DuplicateId { id: String, count: usize },
    /// Two routable subjects on one logical host share an effective address. This includes a
    /// collision between an explicit address and another declaration's identity fallback.
    DuplicateAddress {
        host: String,
        address: String,
        ids: Vec<String>,
    },
}

impl fmt::Display for UniquenessConflict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateId { id, count } => {
                write!(f, "agent id '{id}' is declared by {count} subjects")
            }
            Self::DuplicateAddress { host, address, ids } => write!(
                f,
                "effective address '{address}' on host '{host}' is claimed by {}",
                ids.join(", ")
            ),
        }
    }
}

/// The complete catalog projection ordinary references resolve against.
///
/// The book is built from one immutable discovery snapshot so a lookup and the uniqueness proof it
/// depends on describe the same catalog generation.
#[derive(Debug, Clone, Default)]
pub struct AddressBook {
    subjects: Vec<Subject>,
    hosts: BTreeSet<String>,
}

impl AddressBook {
    /// Build the book from every admitted subject. Admitted logical hosts are exactly the hosts
    /// those subjects resolve to, which is what makes a dotted split decidable without guessing.
    pub fn new(subjects: Vec<Subject>) -> Self {
        let hosts = subjects
            .iter()
            .map(|subject| subject.host.clone())
            .filter(|host| !host.is_empty())
            .collect();
        Self { subjects, hosts }
    }

    pub fn subjects(&self) -> &[Subject] {
        &self.subjects
    }

    pub fn hosts(&self) -> &BTreeSet<String> {
        &self.hosts
    }

    /// Exact catalog-global ID lookup. Never falls through to address lookup.
    ///
    /// An ID is catalog-global and unique by contract, so two subjects claiming one ID is a broken
    /// catalog, not a choice to make. Returning the first match would let a duplicate-ID catalog
    /// silently bind ownership, authority, or a message to whichever declaration happened to be
    /// discovered first, so this refuses instead (R19: unknown and ambiguous targets refuse before
    /// writes, listing, or actions).
    pub fn resolve_id(&self, id: &str) -> Result<&Subject, ResolveError> {
        let mut matches = self
            .subjects
            .iter()
            .filter(|subject| subject.id.as_str() == id);
        let first = matches
            .next()
            .ok_or_else(|| ResolveError::UnknownId { id: id.to_owned() })?;
        match matches.next() {
            None => Ok(first),
            Some(_) => {
                let count = self
                    .subjects
                    .iter()
                    .filter(|subject| subject.id.as_str() == id)
                    .count();
                Err(ResolveError::AmbiguousId {
                    id: id.to_owned(),
                    count,
                })
            }
        }
    }

    /// The fail-closed bare-or-qualified ordinary reference algorithm (root `spec.md`):
    ///
    /// 1. a host-pinned reference tries the complete input as an address in that host plus the
    ///    qualified split whose prefix equals the pinned host;
    /// 2. an unpinned reference tries the complete input as a bare address across the catalog plus
    ///    every dotted split whose prefix is an admitted logical host and whose suffix is an
    ///    effective address in that host;
    /// 3. candidates deduplicate by agent ID and exactly one distinct subject must remain.
    pub fn resolve_address(
        &self,
        reference: &str,
        pinned_host: Option<&str>,
    ) -> Result<&Subject, ResolveError> {
        let mut candidates: Vec<&Subject> = Vec::new();

        match pinned_host {
            Some(host) => {
                self.collect_routable(host, reference, &mut candidates);
                if let Some(rest) = reference
                    .strip_prefix(host)
                    .and_then(|rest| rest.strip_prefix('.'))
                {
                    self.collect_routable(host, rest, &mut candidates);
                }
            }
            None => {
                for subject in &self.subjects {
                    if subject.routable && subject.effective_address == reference {
                        push_unique(&mut candidates, subject);
                    }
                }
                for (index, byte) in reference.bytes().enumerate() {
                    if byte != b'.' {
                        continue;
                    }
                    let (host, rest) = (&reference[..index], &reference[index + 1..]);
                    if !self.hosts.contains(host) {
                        continue;
                    }
                    self.collect_routable(host, rest, &mut candidates);
                }
            }
        }

        match candidates.as_slice() {
            [] => Err(ResolveError::UnknownAddress {
                reference: reference.to_owned(),
                host: pinned_host.map(str::to_owned),
            }),
            [only] => Ok(only),
            many => {
                let mut candidates = many
                    .iter()
                    .map(|subject| {
                        format!(
                            "{} ({})",
                            subject.id,
                            bus_address(&subject.host, &subject.effective_address)
                        )
                    })
                    .collect::<Vec<_>>();
                candidates.sort();
                Err(ResolveError::AmbiguousAddress {
                    reference: reference.to_owned(),
                    candidates,
                })
            }
        }
    }

    /// Resolve one typed selector.
    pub fn resolve(&self, selector: &AgentSelector) -> Result<&Subject, ResolveError> {
        match selector {
            AgentSelector::Id(id) => self.resolve_id(id),
            AgentSelector::Address { reference, host } => {
                self.resolve_address(reference, host.as_deref())
            }
        }
    }

    /// Append every routable subject on `host` whose effective address is exactly `address`.
    fn collect_routable<'a>(
        &'a self,
        host: &str,
        address: &str,
        candidates: &mut Vec<&'a Subject>,
    ) {
        for subject in &self.subjects {
            if subject.routable && subject.host == host && subject.effective_address == address {
                push_unique(candidates, subject);
            }
        }
    }

    /// Every uniqueness violation in this complete prospective catalog: catalog-global ID
    /// duplicates and host-local effective-address duplicates among routable subjects.
    pub fn conflicts(&self) -> Vec<UniquenessConflict> {
        let mut conflicts = Vec::new();

        let mut by_id: BTreeMap<&str, usize> = BTreeMap::new();
        for subject in &self.subjects {
            *by_id.entry(subject.id.as_str()).or_default() += 1;
        }
        for (id, count) in by_id {
            if count > 1 {
                conflicts.push(UniquenessConflict::DuplicateId {
                    id: id.to_owned(),
                    count,
                });
            }
        }

        let mut by_address: BTreeMap<(&str, &str), Vec<&str>> = BTreeMap::new();
        for subject in self.subjects.iter().filter(|subject| subject.routable) {
            by_address
                .entry((subject.host.as_str(), subject.effective_address.as_str()))
                .or_default()
                .push(subject.id.as_str());
        }
        for ((host, address), ids) in by_address {
            if ids.len() > 1 {
                let mut ids = ids.iter().map(|id| (*id).to_owned()).collect::<Vec<_>>();
                ids.sort();
                conflicts.push(UniquenessConflict::DuplicateAddress {
                    host: host.to_owned(),
                    address: address.to_owned(),
                    ids,
                });
            }
        }

        conflicts
    }
}

/// Candidate accumulation deduplicates by agent ID: the same subject reached as a bare address and
/// again as a host-qualified split is one subject, not an ambiguity. The candidate set is bounded
/// by the dot count of one reference, so a linear scan is the right structure.
fn push_unique<'a>(candidates: &mut Vec<&'a Subject>, subject: &'a Subject) {
    if !candidates
        .iter()
        .any(|candidate| candidate.id == subject.id)
    {
        candidates.push(subject);
    }
}

/// Format a UUIDv7 as its canonical lowercase hyphenated string.
fn uuid_v7() -> Result<String, IdentityError> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|since| since.as_millis())
        .unwrap_or_default() as u64
        & 0x0000_FFFF_FFFF_FFFF;
    let mut bytes = [0u8; 16];
    bytes[..6].copy_from_slice(&millis.to_be_bytes()[2..]);
    fill_random(&mut bytes[6..])?;
    bytes[6] = (bytes[6] & 0x0F) | 0x70;
    bytes[8] = (bytes[8] & 0x3F) | 0x80;

    let hex = |slice: &[u8]| {
        slice
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    Ok(format!(
        "{}-{}-{}-{}-{}",
        hex(&bytes[0..4]),
        hex(&bytes[4..6]),
        hex(&bytes[6..8]),
        hex(&bytes[8..10]),
        hex(&bytes[10..16])
    ))
}

/// Fill `buffer` from the operating system CSPRNG. A failure refuses rather than degrading to a
/// predictable id: a colliding "unique" subject ID is unrecoverable.
fn fill_random(buffer: &mut [u8]) -> Result<(), IdentityError> {
    let mut source = std::fs::File::open("/dev/urandom").map_err(|error| {
        IdentityError::RandomnessUnavailable {
            detail: format!("open /dev/urandom: {error}"),
        }
    })?;
    source
        .read_exact(buffer)
        .map_err(|error| IdentityError::RandomnessUnavailable {
            detail: format!("read /dev/urandom: {error}"),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn subject(id: &str, host: &str, address: &str) -> Subject {
        Subject {
            id: AgentId::parse(id).unwrap(),
            host: host.to_owned(),
            effective_address: address.to_owned(),
            routable: true,
        }
    }

    #[test]
    fn generated_ids_are_distinct_uuid_v7_values() {
        let first = AgentId::generate().unwrap();
        let second = AgentId::generate().unwrap();
        assert_ne!(first, second);
        for id in [&first, &second] {
            let text = id.as_str();
            assert_eq!(text.len(), 36, "{text}");
            let fields = text.split('-').collect::<Vec<_>>();
            assert_eq!(
                fields.iter().map(|f| f.len()).collect::<Vec<_>>(),
                vec![8, 4, 4, 4, 12]
            );
            assert!(text.chars().all(|c| c == '-' || c.is_ascii_hexdigit()));
            assert_eq!(&fields[2][..1], "7", "version nibble: {text}");
            let variant = u8::from_str_radix(&fields[3][..1], 16).unwrap();
            assert_eq!(variant & 0xC, 0x8, "variant nibble: {text}");
        }
    }

    #[test]
    fn frozen_legacy_id_is_the_existing_bus_identity() {
        let id = AgentId::frozen_legacy("dev3", "dotfiles.fractal.verifier").unwrap();
        assert_eq!(id.as_str(), "dev3.dotfiles.fractal.verifier");
    }

    #[test]
    fn address_grammar_admits_dotted_lowercase_segments_and_refuses_everything_else() {
        assert!(AgentAddress::parse("dotfiles.fractal.keymap.verifier").is_ok());
        assert!(AgentAddress::parse("a").is_ok());
        assert!(AgentAddress::parse("a-b").is_ok());
        assert!(AgentAddress::parse("").is_err());
        assert!(AgentAddress::parse("Upper").is_err());
        assert!(AgentAddress::parse("has_underscore").is_err());
        assert!(AgentAddress::parse("-leading").is_err());
        assert!(AgentAddress::parse("trailing-").is_err());
        assert!(AgentAddress::parse("a..b").is_err());
        assert!(AgentAddress::parse(".a").is_err());
        assert!(AgentAddress::parse("a.").is_err());
        assert!(AgentAddress::parse(&"a".repeat(64)).is_err());
        assert!(AgentAddress::parse(&format!("{}.{}", "a".repeat(63), "b".repeat(63))).is_ok());
        let too_long = std::iter::repeat_n("abcd".to_string(), 64)
            .collect::<Vec<_>>()
            .join(".");
        assert!(too_long.len() > AGENT_ADDRESS_MAX_CHARS);
        assert!(AgentAddress::parse(&too_long).is_err());
    }

    #[test]
    fn id_grammar_is_opaque_but_refuses_unusable_bytes() {
        assert!(AgentId::parse("dev3.omp.zf8bz8y7").is_ok());
        assert!(AgentId::parse("0199b8f4-8d3a-7c21-9a44-6f85b7320ea1").is_ok());
        assert!(AgentId::parse("Mixed.Case_Legacy").is_ok());
        assert!(AgentId::parse("").is_err());
        assert!(AgentId::parse("has space").is_err());
        assert!(AgentId::parse("has/slash").is_err());
        assert!(AgentId::parse("has\\backslash").is_err());
        assert!(AgentId::parse(".leading").is_err());
        assert!(AgentId::parse("trailing.").is_err());
        assert!(AgentId::parse("double..dot").is_err());
        assert!(AgentId::parse(&"a".repeat(256)).is_err());
    }

    #[test]
    fn id_and_address_are_separate_namespaces() {
        let book = AddressBook::new(vec![
            subject("dev3.alpha", "dev3", "alpha"),
            subject("uuid-beta", "dev3", "dev3.alpha"),
        ]);
        // Equal bytes across the two namespaces are not a conflict.
        assert!(book.conflicts().is_empty());
        assert_eq!(book.resolve_id("dev3.alpha").unwrap().id.as_str(), "dev3.alpha");
        // The ordinary reference `dev3.alpha` is decidable: the bare-address candidate is the
        // second subject and the qualified split is the first, so it is ambiguous, not a silent win.
        assert!(matches!(
            book.resolve_address("dev3.alpha", None),
            Err(ResolveError::AmbiguousAddress { .. })
        ));
    }

    #[test]
    fn unpinned_reference_tries_bare_then_admitted_host_splits() {
        let book = AddressBook::new(vec![
            subject("id-a", "dev3", "dotfiles.fractal.verifier"),
            subject("id-b", "dev4", "other"),
        ]);
        assert_eq!(
            book.resolve_address("dotfiles.fractal.verifier", None)
                .unwrap()
                .id
                .as_str(),
            "id-a"
        );
        assert_eq!(
            book.resolve_address("dev3.dotfiles.fractal.verifier", None)
                .unwrap()
                .id
                .as_str(),
            "id-a"
        );
        // `mbp` is not an admitted host, so no split is attempted and the lookup fails closed.
        assert!(matches!(
            book.resolve_address("mbp.dotfiles.fractal.verifier", None),
            Err(ResolveError::UnknownAddress { .. })
        ));
    }

    #[test]
    fn host_pinned_reference_accepts_bare_and_self_qualified_input_only() {
        let book = AddressBook::new(vec![
            subject("id-a", "dev3", "worker"),
            subject("id-b", "dev4", "worker"),
        ]);
        assert_eq!(
            book.resolve_address("worker", Some("dev3")).unwrap().id.as_str(),
            "id-a"
        );
        assert_eq!(
            book.resolve_address("dev3.worker", Some("dev3"))
                .unwrap()
                .id
                .as_str(),
            "id-a"
        );
        // A pinned host never reaches another host's split.
        assert!(matches!(
            book.resolve_address("dev4.worker", Some("dev3")),
            Err(ResolveError::UnknownAddress { .. })
        ));
        // The same address on two hosts is legal; only an unpinned bare reference is ambiguous.
        assert!(matches!(
            book.resolve_address("worker", None),
            Err(ResolveError::AmbiguousAddress { .. })
        ));
    }

    #[test]
    fn retired_subjects_are_non_routable_and_release_their_address() {
        let mut retired = subject("id-a", "dev3", "worker");
        retired.routable = false;
        let book = AddressBook::new(vec![retired, subject("id-b", "dev3", "worker")]);
        // The retired subject does not occupy the namespace, so no conflict and no ambiguity.
        assert!(book.conflicts().is_empty());
        assert_eq!(
            book.resolve_address("worker", Some("dev3")).unwrap().id.as_str(),
            "id-b"
        );
        // It remains reachable by exact ID.
        assert_eq!(book.resolve_id("id-a").unwrap().bus_address(), None);
    }

    #[test]
    fn duplicate_ids_and_host_local_addresses_are_conflicts() {
        let book = AddressBook::new(vec![
            subject("dup", "dev3", "a"),
            subject("dup", "dev4", "b"),
            subject("id-c", "dev3", "shared"),
            subject("id-d", "dev3", "shared"),
        ]);
        let conflicts = book.conflicts();
        assert!(conflicts.iter().any(|conflict| matches!(
            conflict,
            UniquenessConflict::DuplicateId { id, count: 2 } if id == "dup"
        )));
        assert!(conflicts.iter().any(|conflict| matches!(
            conflict,
            UniquenessConflict::DuplicateAddress { host, address, .. }
                if host == "dev3" && address == "shared"
        )));
    }

    #[test]
    fn a_duplicate_agent_id_refuses_instead_of_binding_the_first_declaration() {
        // A catalog-global id is unique by contract. Returning the first match would let a broken
        // catalog silently bind ownership or authority to whichever subject was discovered first.
        let book = AddressBook::new(vec![
            subject("dup", "dev3", "first"),
            subject("dup", "dev4", "second"),
        ]);
        assert!(matches!(
            book.resolve_id("dup"),
            Err(ResolveError::AmbiguousId { count: 2, .. })
        ));
        assert!(matches!(
            book.resolve(&AgentSelector::id("dup")),
            Err(ResolveError::AmbiguousId { .. })
        ));
        // The conflict is also reported as a catalog-level uniqueness violation.
        assert!(book.conflicts().iter().any(|conflict| matches!(
            conflict,
            UniquenessConflict::DuplicateId { count: 2, .. }
        )));
    }

    #[test]
    fn exact_id_selection_never_falls_through_to_address_lookup() {
        let book = AddressBook::new(vec![subject("id-a", "dev3", "worker")]);
        assert!(matches!(
            book.resolve(&AgentSelector::id("worker")),
            Err(ResolveError::UnknownId { .. })
        ));
        assert!(matches!(
            book.resolve(&AgentSelector::id("dev3.worker")),
            Err(ResolveError::UnknownId { .. })
        ));
        assert_eq!(
            book.resolve(&AgentSelector::id("id-a")).unwrap().id.as_str(),
            "id-a"
        );
    }
}
