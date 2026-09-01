use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs as _};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use st2_resource_wasip2::{CapabilityModule, InvocationStore, ObservationRequest};
use wasmtime::component::{HasSelf, Linker};

mod bindings {
    wasmtime::component::bindgen!({
        path: "../../wit/github-issue",
        world: "github-issue-observer",
    });
}

use bindings::compoundingtech::st2_github_issue::github_issue::{
    Host, IssueError, IssueRequest, IssueResponse,
};

const IMPORT_NAME: &str = "compoundingtech:st2-github-issue/github-issue@0.1.0";
const API_HOST: &str = "api.github.com";
const API_PORT: u16 = 443;
const MAX_HEADERS_BYTES: usize = 16 * 1024;
const MAX_BODY_BYTES: usize = 256 * 1024;
const MAX_ETAG_BYTES: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubIssueConfig {
    pub owner: String,
    pub repo: String,
    pub number: u64,
    pub connect_timeout: Duration,
    pub total_timeout: Duration,
}

impl GitHubIssueConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !valid_slug(&self.owner) || !valid_slug(&self.repo) || self.number == 0 {
            return Err("GitHub issue scope is invalid");
        }
        if self.connect_timeout.is_zero()
            || self.total_timeout.is_zero()
            || self.connect_timeout > self.total_timeout
            || self.total_timeout > Duration::from_secs(60)
        {
            return Err("GitHub issue deadlines are invalid");
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct GitHubIssueModule {
    config: GitHubIssueConfig,
    cache: Arc<Mutex<BTreeMap<IssueKey, CachedIssue>>>,
    pending: Arc<Mutex<BTreeMap<u64, Arc<GitHubCancellationControl>>>>,
}

impl GitHubIssueModule {
    pub fn new(config: GitHubIssueConfig) -> Result<Self, &'static str> {
        config.validate()?;
        Ok(Self {
            config,
            cache: Arc::new(Mutex::new(BTreeMap::new())),
            pending: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    pub fn prepare(&self, invocation_id: u64) -> GitHubIssueCancellation {
        let control = Arc::new(GitHubCancellationControl::new());
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(invocation_id, Arc::clone(&control));
        GitHubIssueCancellation { control }
    }

    pub fn discard_prepared(&self, invocation_id: u64) -> bool {
        self.pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&invocation_id)
            .is_some()
    }
}

#[derive(Clone)]
pub struct GitHubIssueCancellation {
    control: Arc<GitHubCancellationControl>,
}

impl GitHubIssueCancellation {
    pub fn cancel(&self) -> bool {
        self.control.cancel()
    }
}

struct GitHubCancellationControl {
    cancelled: AtomicBool,
    notify: tokio::sync::Notify,
}

impl GitHubCancellationControl {
    fn new() -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        }
    }

    fn cancel(&self) -> bool {
        let changed = !self.cancelled.swap(true, Ordering::AcqRel);
        if changed {
            self.notify.notify_waiters();
        }
        changed
    }

    async fn cancelled(&self) {
        loop {
            let notified = self.notify.notified();
            if self.cancelled.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct IssueKey {
    owner: String,
    repo: String,
    number: u64,
}

#[derive(Debug, Clone)]
struct CachedIssue {
    etag: Option<String>,
    body: Vec<u8>,
}

pub struct GitHubIssueInvocation {
    config: GitHubIssueConfig,
    cache: Arc<Mutex<BTreeMap<IssueKey, CachedIssue>>>,
    has_authoritative_prior: bool,
    cancellation: Arc<GitHubCancellationControl>,
}

impl CapabilityModule for GitHubIssueModule {
    type Invocation = GitHubIssueInvocation;

    fn import_names(&self) -> &'static [&'static str] {
        &[IMPORT_NAME]
    }

    fn add_to_linker(
        &self,
        linker: &mut Linker<InvocationStore<Self::Invocation>>,
    ) -> Result<(), wasmtime::Error> {
        bindings::GithubIssueObserver::add_to_linker::<_, HasSelf<_>>(linker, |state| state)
    }

    fn begin(&self, request: &ObservationRequest) -> Self::Invocation {
        GitHubIssueInvocation {
            config: self.config.clone(),
            cache: Arc::clone(&self.cache),
            has_authoritative_prior: request.previous_digest.is_some(),
            cancellation: self
                .pending
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&request.invocation_id)
                .unwrap_or_else(|| Arc::new(GitHubCancellationControl::new())),
        }
    }
}

impl Host for InvocationStore<GitHubIssueInvocation> {
    fn get(&mut self, request: IssueRequest) -> Result<IssueResponse, IssueError> {
        self.capability_mut().get(request)
    }
}

impl GitHubIssueInvocation {
    fn get(&mut self, request: IssueRequest) -> Result<IssueResponse, IssueError> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| IssueError::Unavailable)?;
        runtime.block_on(self.get_async(request))
    }

    async fn get_async(&mut self, request: IssueRequest) -> Result<IssueResponse, IssueError> {
        if request.owner != self.config.owner
            || request.repo != self.config.repo
            || request.number != self.config.number
            || request.etag.as_ref().is_some_and(|etag| !valid_etag(etag))
        {
            return Err(IssueError::Denied);
        }
        let key = IssueKey {
            owner: request.owner,
            repo: request.repo,
            number: request.number,
        };
        let cached = self
            .cache
            .lock()
            .map_err(|_| IssueError::Unavailable)?
            .get(&key)
            .cloned();
        let requested_etag = request.etag;
        let reused_cached_entry = self.has_authoritative_prior
            && requested_etag.is_none()
            && cached.as_ref().is_some_and(|entry| entry.etag.is_some());
        let etag = conditional_etag(
            self.has_authoritative_prior,
            requested_etag,
            cached.as_ref().and_then(|entry| entry.etag.clone()),
        );
        let endpoint = format!(
            "https://{API_HOST}/repos/{}/{}/issues/{}",
            key.owner, key.repo, key.number
        );
        let address = resolve_public_api_address()?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(self.config.connect_timeout)
            .timeout(self.config.total_timeout)
            .gzip(true)
            .resolve(API_HOST, address)
            .build()
            .map_err(|_| IssueError::Unavailable)?;
        let mut builder = client
            .get(endpoint)
            .header("accept", "application/vnd.github+json")
            .header("x-github-api-version", "2022-11-28")
            .header("user-agent", "st2-resource-provider");
        if let Some(etag) = etag.as_deref() {
            builder = builder.header("if-none-match", etag);
        }
        let mut response = tokio::select! {
            biased;
            () = self.cancellation.cancelled() => return Err(IssueError::Unavailable),
            response = builder.send() => response.map_err(map_transport_error)?,
        };
        let status = response.status();
        if status.is_redirection() && status.as_u16() != 304 {
            return Err(IssueError::Denied);
        }
        let header_bytes = response.headers().iter().try_fold(0_usize, |total, (name, value)| {
            total
                .checked_add(name.as_str().len())
                .and_then(|total| total.checked_add(value.as_bytes().len()))
                .ok_or(IssueError::ResourceExhausted)
        })?;
        if header_bytes > MAX_HEADERS_BYTES {
            return Err(IssueError::ResourceExhausted);
        }
        let response_etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .filter(|value| valid_etag(value))
            .map(str::to_owned);
        match status.as_u16() {
            304 => Ok(not_modified_response(
                reused_cached_entry,
                response_etag,
                etag,
                cached,
            )),
            200 => {
                let mut body = Vec::new();
                loop {
                    let chunk = tokio::select! {
                        biased;
                        () = self.cancellation.cancelled() => return Err(IssueError::Unavailable),
                        chunk = response.chunk() => chunk.map_err(map_transport_error)?,
                    };
                    let Some(chunk) = chunk else {
                        break;
                    };
                    if body.len().saturating_add(chunk.len()) > MAX_BODY_BYTES {
                        return Err(IssueError::ResourceExhausted);
                    }
                    body.extend_from_slice(&chunk);
                }
                self.cache
                    .lock()
                    .map_err(|_| IssueError::Unavailable)?
                    .insert(
                        key,
                        CachedIssue {
                            etag: response_etag.clone(),
                            body: body.clone(),
                        },
                    );
                Ok(IssueResponse::Ok((response_etag, body)))
            }
            401 | 403 | 404 => Err(IssueError::Denied),
            _ => Err(IssueError::Unavailable),
        }
    }
}

fn conditional_etag(
    has_authoritative_prior: bool,
    requested: Option<String>,
    cached: Option<String>,
) -> Option<String> {
    has_authoritative_prior
        .then(|| requested.or(cached))
        .flatten()
}

fn not_modified_response(
    reused_cached_entry: bool,
    response_etag: Option<String>,
    conditional_etag: Option<String>,
    cached: Option<CachedIssue>,
) -> IssueResponse {
    let effective_etag = response_etag.or(conditional_etag);
    if reused_cached_entry
        && let Some(cached) = cached
        && cached.etag == effective_etag
    {
        return IssueResponse::Ok((effective_etag, cached.body));
    }
    IssueResponse::NotModified(effective_etag)
}

fn valid_slug(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 100
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_etag(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ETAG_BYTES
        && !value.bytes().any(|byte| byte == b'\r' || byte == b'\n' || byte == 0)
}

fn map_transport_error(error: reqwest::Error) -> IssueError {
    if error.is_timeout() {
        IssueError::DeadlineExceeded
    } else {
        IssueError::Unavailable
    }
}

fn resolve_public_api_address() -> Result<SocketAddr, IssueError> {
    let addresses = (API_HOST, API_PORT)
        .to_socket_addrs()
        .map_err(|_| IssueError::Unavailable)?
        .collect::<Vec<_>>();
    if addresses.is_empty() || addresses.iter().any(|address| !is_public(address.ip())) {
        return Err(IssueError::Denied);
    }
    addresses.into_iter().next().ok_or(IssueError::Unavailable)
}

fn is_public(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            let octets = address.octets();
            !(address.is_private()
                || address.is_loopback()
                || address.is_link_local()
                || address.is_broadcast()
                || address.is_documentation()
                || address.is_unspecified()
                || address.is_multicast()
                || octets[0] == 0
                || (octets[0] == 100 && (octets[1] & 0xc0) == 64)
                || (octets[0] == 192 && octets[1] == 0 && octets[2] == 0)
                || (octets[0] == 198 && matches!(octets[1], 18 | 19))
                || octets[0] >= 240)
        }
        IpAddr::V6(address) => {
            let segments = address.segments();
            let first = segments[0];
            let mapped_private = address
                .to_ipv4_mapped()
                .is_some_and(|mapped| !is_public(IpAddr::V4(mapped)));
            !(address.is_loopback()
                || address.is_unspecified()
                || address.is_multicast()
                || mapped_private
                || (first & 0xfe00) == 0xfc00
                || (first & 0xffc0) == 0xfe80
                || (segments[0] == 0x2001 && segments[1] == 0x0db8))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discarded_preparation_does_not_leak_pending_invocation_state() {
        let module = GitHubIssueModule::new(live_config()).unwrap();
        let _cancellation = module.prepare(42);
        assert_eq!(module.pending.lock().unwrap().len(), 1);
        assert!(module.discard_prepared(42));
        assert!(module.pending.lock().unwrap().is_empty());
    }

    fn live_config() -> GitHubIssueConfig {
        GitHubIssueConfig {
            owner: "rust-lang".into(),
            repo: "rust".into(),
            number: 1,
            connect_timeout: Duration::from_secs(3),
            total_timeout: Duration::from_secs(10),
        }
    }

    #[test]
    fn exact_scope_and_header_policy_deny_before_transport() {
        let module = GitHubIssueModule::new(live_config()).unwrap();
        for request in [
            IssueRequest {
                owner: "other".into(),
                repo: "rust".into(),
                number: 1,
                etag: None,
            },
            IssueRequest {
                owner: "rust-lang".into(),
                repo: "other".into(),
                number: 1,
                etag: None,
            },
            IssueRequest {
                owner: "rust-lang".into(),
                repo: "rust".into(),
                number: 2,
                etag: None,
            },
            IssueRequest {
                owner: "rust-lang".into(),
                repo: "rust".into(),
                number: 1,
                etag: Some("\"ok\"\r\nx-injected: true".into()),
            },
        ] {
            let mut invocation = GitHubIssueInvocation {
                config: module.config.clone(),
                cache: Arc::clone(&module.cache),
                has_authoritative_prior: false,
                cancellation: Arc::new(GitHubCancellationControl::new()),
            };
            assert!(matches!(invocation.get(request), Err(IssueError::Denied)));
        }
    }

    #[test]
    fn private_special_and_documentation_addresses_are_never_admitted() {
        for address in [
            "0.0.0.0",
            "10.0.0.1",
            "127.0.0.1",
            "169.254.1.1",
            "192.0.2.1",
            "224.0.0.1",
            "::",
            "::1",
            "fc00::1",
            "fe80::1",
            "2001:db8::1",
            "ff02::1",
        ] {
            assert!(!is_public(address.parse().unwrap()), "{address}");
        }
        assert!(is_public("8.8.8.8".parse().unwrap()));
        assert!(is_public("2606:4700:4700::1111".parse().unwrap()));
    }

    #[test]
    fn deadlines_are_bounded_and_ordered() {
        let mut config = live_config();
        config.connect_timeout = Duration::from_secs(11);
        assert!(config.validate().is_err());
        config.connect_timeout = Duration::from_secs(1);
        config.total_timeout = Duration::from_secs(61);
        assert!(config.validate().is_err());
    }

    #[test]
    fn shared_runtime_does_not_reuse_an_etag_for_a_binding_without_prior_state() {
        let key = IssueKey {
            owner: "rust-lang".into(),
            repo: "rust".into(),
            number: 1,
        };
        let module = GitHubIssueModule::new(live_config()).unwrap();
        module.cache.lock().unwrap().insert(
            key,
            CachedIssue {
                etag: Some("\"cached\"".into()),
                body: br#"{"title":"new"}"#.to_vec(),
            },
        );
        let without_prior = module.begin(&ObservationRequest {
            invocation_id: 1,
            uri: "github-issue:rust-lang/rust#1".into(),
            selector: serde_json::json!({}),
            previous_digest: None,
        });
        let with_prior = module.begin(&ObservationRequest {
            uri: "github-issue:rust-lang/rust#1".into(),
            invocation_id: 2,
            selector: serde_json::json!({}),
            previous_digest: Some(st2_resource_protocol::SnapshotDigest::of(b"prior")),
        });

        assert!(!without_prior.has_authoritative_prior);
        assert_eq!(
            conditional_etag(
                without_prior.has_authoritative_prior,
                None,
                Some("\"cached\"".into()),
            ),
            None
        );
        assert_eq!(
            conditional_etag(
                with_prior.has_authoritative_prior,
                None,
                Some("\"cached\"".into()),
            ),
            Some("\"cached\"".into())
        );
        assert!(with_prior.has_authoritative_prior);
    }

    #[test]
    fn shared_runtime_304_replays_cached_body_to_an_older_binding() {
        let newest_body = br#"{"title":"new"}"#.to_vec();
        let cached = CachedIssue {
            etag: Some("\"new\"".into()),
            body: newest_body.clone(),
        };
        let response = not_modified_response(
            true,
            None,
            Some("\"new\"".into()),
            Some(cached),
        );
        let IssueResponse::Ok((etag, body)) = response else {
            panic!("shared cached revalidation must return the exact cached body");
        };
        assert_eq!(etag.as_deref(), Some("\"new\""));
        assert_eq!(body, newest_body);
        assert_ne!(
            st2_resource_protocol::SnapshotDigest::of(b"{\"title\":\"old\"}"),
            st2_resource_protocol::SnapshotDigest::of(&body),
            "the guest can compare and publish the newer body for the skewed binding"
        );
    }

    #[test]
    #[ignore = "explicit public read-only GitHub smoke: cargo test -p st2-resource-providers github_live_200_then_etag_304 -- --ignored"]
    fn github_live_200_then_etag_304() {
        let module = GitHubIssueModule::new(live_config()).unwrap();
        let request = || IssueRequest {
            owner: "rust-lang".into(),
            repo: "rust".into(),
            number: 1,
            etag: None,
        };
        let mut first = GitHubIssueInvocation {
            config: module.config.clone(),
            cache: Arc::clone(&module.cache),
            has_authoritative_prior: false,
            cancellation: Arc::new(GitHubCancellationControl::new()),
        };
        assert!(matches!(first.get(request()).unwrap(), IssueResponse::Ok(_)));
        let mut replay = GitHubIssueInvocation {
            config: module.config.clone(),
            cache: Arc::clone(&module.cache),
            has_authoritative_prior: true,
            cancellation: Arc::new(GitHubCancellationControl::new()),
        };
        assert!(matches!(
            replay.get(request()).unwrap(),
            IssueResponse::Ok(_)
        ));
    }
}
