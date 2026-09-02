use std::collections::BTreeMap;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use chrono::{SecondsFormat, Utc};
use serde::Deserialize;
use st2_resource_wasip2::{
    CapabilityContext, CapabilityModule, CapabilityPhase, InterruptionReason, InvocationControl,
    InvocationStore,
};
use wasmtime::component::{HasSelf, Linker};

mod bindings {
    wasmtime::component::bindgen!({
        path: "../../wit/github-pr",
        world: "github-pr-provider",
    });
}

use bindings::compoundingtech::st2_github_pr::github_pr::{
    Host, PullRequestError, PullRequestRequest, PullRequestResponse, SourceObject,
    SourceObservation, SourceSnapshot,
};

const IMPORT_NAME: &str = "compoundingtech:st2-github-pr/github-pr@0.1.0";
const API_HOST: &str = "api.github.com";
const API_PORT: u16 = 443;
const MAX_HEADERS_BYTES: usize = 16 * 1024;
const MAX_SOURCE_BYTES: usize = 1024 * 1024;
const MAX_ETAG_BYTES: usize = 1024;
const SNAPSHOT_DIGEST_BYTES: usize = 32;
const MAX_CACHED_SNAPSHOTS: usize = 16;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitHubPrConfig {
    pub owner: String,
    pub repo: String,
    pub number: u64,
    pub connect_timeout: Duration,
    pub total_timeout: Duration,
}

impl GitHubPrConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !valid_component(&self.owner, 39)
            || !valid_component(&self.repo, 100)
            || self.number == 0
        {
            return Err("GitHub pull request scope is invalid");
        }
        if self.connect_timeout.is_zero()
            || self.total_timeout.is_zero()
            || self.connect_timeout > self.total_timeout
            || self.total_timeout > Duration::from_secs(60)
        {
            return Err("GitHub pull request deadlines are invalid");
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct GitHubPrModule {
    config: GitHubPrConfig,
    cache: Arc<Mutex<SnapshotCache>>,
}

impl GitHubPrModule {
    pub fn new(config: GitHubPrConfig) -> Result<Self, &'static str> {
        config.validate()?;
        Ok(Self {
            config,
            cache: Arc::new(Mutex::new(SnapshotCache::default())),
        })
    }
}

#[derive(Debug, Clone)]
struct PullRequestKey {
    owner: String,
    repo: String,
    number: u64,
}

#[derive(Debug, Clone)]
struct CachedObject {
    etag: Option<String>,
    body: Vec<u8>,
}

#[derive(Debug, Clone)]
struct CachedSource {
    pull_request: CachedObject,
    check_runs: CachedObject,
    combined_status: CachedObject,
    observed_at: String,
}

#[derive(Default)]
struct SnapshotCache {
    sources: BTreeMap<[u8; SNAPSHOT_DIGEST_BYTES], CachedSource>,
}

impl SnapshotCache {
    fn get(&self, digest: &[u8; SNAPSHOT_DIGEST_BYTES]) -> Option<&CachedSource> {
        self.sources.get(digest)
    }

    fn insert(&mut self, digest: [u8; SNAPSHOT_DIGEST_BYTES], source: CachedSource) {
        if !self.sources.contains_key(&digest) && self.sources.len() >= MAX_CACHED_SNAPSHOTS {
            if let Some(evicted) = self.sources.keys().next().cloned() {
                self.sources.remove(&evicted);
            }
        }
        self.sources.insert(digest, source);
    }
}

pub struct GitHubPrInvocation {
    config: GitHubPrConfig,
    cache: Arc<Mutex<SnapshotCache>>,
    prior_digest: Option<[u8; SNAPSHOT_DIGEST_BYTES]>,
    current_source: Option<CachedSource>,
    control: InvocationControl,
}

impl CapabilityModule for GitHubPrModule {
    type Invocation = GitHubPrInvocation;

    fn import_names(&self) -> &'static [&'static str] {
        &[IMPORT_NAME]
    }

    fn add_to_linker(
        &self,
        linker: &mut Linker<InvocationStore<Self::Invocation>>,
    ) -> Result<(), wasmtime::Error> {
        bindings::GithubPrProvider::add_to_linker::<_, HasSelf<_>>(linker, |state| state)
    }

    fn begin(&self, context: CapabilityContext<'_>) -> Self::Invocation {
        let prior_digest = match context.phase() {
            CapabilityPhase::Describe => None,
            CapabilityPhase::Observe(request) => {
                request.prior_digest.as_ref().map(|digest| *digest.as_bytes())
            }
        };
        GitHubPrInvocation {
            config: self.config.clone(),
            cache: Arc::clone(&self.cache),
            prior_digest,
            current_source: None,
            control: context.control().clone(),
        }
    }
}

impl Host for InvocationStore<GitHubPrInvocation> {
    fn get(
        &mut self,
        request: PullRequestRequest,
    ) -> Result<PullRequestResponse, PullRequestError> {
        self.capability_mut().get(request)
    }

    fn bind_snapshot(&mut self, digest: Vec<u8>) -> Result<(), PullRequestError> {
        self.capability_mut().bind_snapshot(digest)
    }
}

impl GitHubPrInvocation {
    fn get(
        &mut self,
        request: PullRequestRequest,
    ) -> Result<PullRequestResponse, PullRequestError> {
        run_on_runtime(self.get_async(request))
    }

    async fn get_async(
        &mut self,
        request: PullRequestRequest,
    ) -> Result<PullRequestResponse, PullRequestError> {
        if !request_matches_scope(&self.config, &request) {
            return Err(PullRequestError::Denied);
        }
        if let Some(reason) = self.control.interruption_reason() {
            return Err(interruption_error(reason));
        }
        let key = PullRequestKey {
            owner: request.owner,
            repo: request.repo,
            number: request.number,
        };
        let prior = match self.prior_digest.as_ref() {
            Some(digest) => self
                .cache
                .lock()
                .map_err(|_| PullRequestError::Unavailable)?
                .get(digest)
                .cloned(),
            None => None,
        };
        let deadline = Instant::now()
            .checked_add(self.config.total_timeout)
            .ok_or(PullRequestError::DeadlineExceeded)?;
        let address = resolve_public_api_address(&self.control, deadline).await?;
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(self.config.connect_timeout)
            .gzip(true)
            .resolve(API_HOST, address)
            .build()
            .map_err(|_| PullRequestError::Unavailable)?;

        let pull_endpoint = format!(
            "https://{API_HOST}/repos/{}/{}/pulls/{}",
            key.owner, key.repo, key.number
        );
        let pull_request = self
            .fetch_object(
                &client,
                pull_endpoint,
                prior.as_ref().map(|source| &source.pull_request),
                deadline,
                MAX_SOURCE_BYTES,
            )
            .await?;
        let pull: PullRequestHead = serde_json::from_slice(&pull_request.object.body)
            .map_err(|_| PullRequestError::Unavailable)?;
        if pull.number != key.number || !valid_head_sha(&pull.head.sha) {
            return Err(PullRequestError::Denied);
        }

        let mut remaining = MAX_SOURCE_BYTES
            .checked_sub(pull_request.object.body.len())
            .ok_or(PullRequestError::ResourceExhausted)?;
        let checks_endpoint = format!(
            "https://{API_HOST}/repos/{}/{}/commits/{}/check-runs?per_page=100",
            key.owner, key.repo, pull.head.sha
        );
        let check_runs = require_complete(
            self.fetch_object(
                &client,
                checks_endpoint,
                prior.as_ref().map(|source| &source.check_runs),
                deadline,
                remaining,
            )
            .await?,
        )?;
        remaining = remaining
            .checked_sub(check_runs.object.body.len())
            .ok_or(PullRequestError::ResourceExhausted)?;
        let status_endpoint = format!(
            "https://{API_HOST}/repos/{}/{}/commits/{}/status?per_page=100",
            key.owner, key.repo, pull.head.sha
        );
        let combined_status = require_complete(
            self.fetch_object(
                &client,
                status_endpoint,
                prior.as_ref().map(|source| &source.combined_status),
                deadline,
                remaining,
            )
            .await?,
        )?;

        if !pull_request.modified && !check_runs.modified && !combined_status.modified {
            return Ok(PullRequestResponse::NotModified);
        }
        let observed_at = prior
            .as_ref()
            .filter(|prior| {
                prior.pull_request.body == pull_request.object.body
                    && prior.check_runs.body == check_runs.object.body
                    && prior.combined_status.body == combined_status.object.body
            })
            .map_or_else(
                || Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true),
                |prior| prior.observed_at.clone(),
            );
        let source = CachedSource {
            pull_request: pull_request.object,
            check_runs: check_runs.object,
            combined_status: combined_status.object,
            observed_at,
        };
        let current = source_to_wit(&source);
        self.current_source = Some(source);
        Ok(PullRequestResponse::Ok(SourceObservation {
            current,
            previous: prior.as_ref().map(source_to_wit),
        }))
    }

    fn bind_snapshot(&mut self, digest: Vec<u8>) -> Result<(), PullRequestError> {
        let digest: [u8; SNAPSHOT_DIGEST_BYTES] = digest
            .try_into()
            .map_err(|_| PullRequestError::Denied)?;
        let source = self
            .current_source
            .take()
            .ok_or(PullRequestError::Unavailable)?;
        self.cache
            .lock()
            .map_err(|_| PullRequestError::Unavailable)?
            .insert(digest, source);
        Ok(())
    }

    async fn fetch_object(
        &self,
        client: &reqwest::Client,
        endpoint: String,
        cached: Option<&CachedObject>,
        deadline: Instant,
        max_body_bytes: usize,
    ) -> Result<FetchedObject, PullRequestError> {
        if max_body_bytes == 0 {
            return Err(PullRequestError::ResourceExhausted);
        }
        if let Some(reason) = self.control.interruption_reason() {
            return Err(interruption_error(reason));
        }
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .filter(|remaining| !remaining.is_zero())
            .ok_or(PullRequestError::DeadlineExceeded)?;
        let builder = request_builder(client, endpoint, remaining, cached);
        let mut response = tokio::select! {
            biased;
            reason = wait_for_interruption(&self.control) => {
                return Err(interruption_error(reason));
            }
            response = builder.send() => response.map_err(map_transport_error)?,
        };
        let status = response.status();
        if status.is_redirection() && status.as_u16() != 304 {
            return Err(PullRequestError::Denied);
        }
        let header_bytes = response.headers().iter().try_fold(
            0_usize,
            |total, (name, value)| {
                total
                    .checked_add(name.as_str().len())
                    .and_then(|total| total.checked_add(value.as_bytes().len()))
                    .ok_or(PullRequestError::ResourceExhausted)
            },
        )?;
        if header_bytes > MAX_HEADERS_BYTES {
            return Err(PullRequestError::ResourceExhausted);
        }
        let response_etag = response
            .headers()
            .get(reqwest::header::ETAG)
            .and_then(|value| value.to_str().ok())
            .filter(|value| valid_etag(value))
            .map(str::to_owned);
        let has_next_page = response_has_next_page(response.headers());
        match status.as_u16() {
            304 => replay_not_modified(cached, response_etag, has_next_page),
            200 => {
                let mut body = Vec::new();
                loop {
                    let chunk = tokio::select! {
                        biased;
                        reason = wait_for_interruption(&self.control) => {
                            return Err(interruption_error(reason));
                        }
                        chunk = response.chunk() => chunk.map_err(map_transport_error)?,
                    };
                    let Some(chunk) = chunk else {
                        break;
                    };
                    if body.len().saturating_add(chunk.len()) > max_body_bytes {
                        return Err(PullRequestError::ResourceExhausted);
                    }
                    body.extend_from_slice(&chunk);
                }
                Ok(FetchedObject {
                    object: CachedObject {
                        etag: response_etag,
                        body,
                    },
                    has_next_page,
                    modified: true,
                })
            }
            401 | 403 | 404 => Err(PullRequestError::Denied),
            _ => Err(PullRequestError::Unavailable),
        }
    }
}

#[derive(Deserialize)]
struct PullRequestHead {
    number: u64,
    head: PullRequestHeadSha,
}

#[derive(Deserialize)]
struct PullRequestHeadSha {
    sha: String,
}

struct FetchedObject {
    object: CachedObject,
    modified: bool,
    has_next_page: bool,
}

fn require_complete(object: FetchedObject) -> Result<FetchedObject, PullRequestError> {
    if object.has_next_page {
        Err(PullRequestError::ResourceExhausted)
    } else {
        Ok(object)
    }
}

fn source_to_wit(source: &CachedSource) -> SourceSnapshot {
    SourceSnapshot {
        pull_request: object_to_wit(&source.pull_request),

        check_runs: object_to_wit(&source.check_runs),
        combined_status: object_to_wit(&source.combined_status),
        observed_at: source.observed_at.clone(),
    }
}

fn run_on_runtime<T>(
    future: impl Future<Output = Result<T, PullRequestError>>,
) -> Result<T, PullRequestError> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| PullRequestError::Unavailable)?;
    runtime.block_on(future)
}

fn request_builder(
    client: &reqwest::Client,
    endpoint: String,
    timeout: Duration,
    cached: Option<&CachedObject>,
) -> reqwest::RequestBuilder {
    let mut builder = client
        .get(endpoint)
        .timeout(timeout)
        .header("accept", "application/vnd.github+json")
        .header("x-github-api-version", "2022-11-28")
        .header("user-agent", "st2-github-pr-resource-profile/1");
    if let Some(etag) = cached.and_then(|object| object.etag.as_deref()) {
        builder = builder.header(reqwest::header::IF_NONE_MATCH, etag);
    }
    builder
}

fn response_has_next_page(headers: &reqwest::header::HeaderMap) -> bool {
    headers
        .get_all(reqwest::header::LINK)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .flat_map(|link| link.split(';').skip(1))
        .any(|parameter| parameter.trim().eq_ignore_ascii_case(r#"rel="next""#))
}

fn replay_not_modified(
    cached: Option<&CachedObject>,
    response_etag: Option<String>,
    has_next_page: bool,
) -> Result<FetchedObject, PullRequestError> {
    let cached = cached.ok_or(PullRequestError::Unavailable)?;
    let effective_etag = response_etag.or_else(|| cached.etag.clone());
    if cached.etag != effective_etag {
        return Err(PullRequestError::Unavailable);
    }
    Ok(FetchedObject {
        object: cached.clone(),
        modified: false,
        has_next_page,
    })
}

fn object_to_wit(object: &CachedObject) -> SourceObject {
    SourceObject {
        etag: object.etag.clone(),
        body: object.body.clone(),
    }
}

async fn wait_for_interruption(control: &InvocationControl) -> InterruptionReason {
    loop {
        if let Some(reason) = control.interruption_reason() {
            return reason;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn interruption_error(reason: InterruptionReason) -> PullRequestError {
    match reason {
        InterruptionReason::Cancelled => PullRequestError::Unavailable,
        InterruptionReason::TimedOut => PullRequestError::DeadlineExceeded,
    }
}

fn request_matches_scope(config: &GitHubPrConfig, request: &PullRequestRequest) -> bool {
    request.owner == config.owner
        && request.repo == config.repo
        && request.number == config.number
}

fn valid_component(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && !matches!(value, "." | "..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_head_sha(value: &str) -> bool {
    value.len() == 40 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_etag(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_ETAG_BYTES
        && !value
            .bytes()
            .any(|byte| byte == b'\r' || byte == b'\n' || byte == 0)
}

fn map_transport_error(error: reqwest::Error) -> PullRequestError {
    if error.is_timeout() {
        PullRequestError::DeadlineExceeded
    } else {
        PullRequestError::Unavailable
    }
}

async fn resolve_public_api_address(
    control: &InvocationControl,
    deadline: Instant,
) -> Result<SocketAddr, PullRequestError> {
    let remaining = deadline
        .checked_duration_since(Instant::now())
        .filter(|remaining| !remaining.is_zero())
        .ok_or(PullRequestError::DeadlineExceeded)?;
    let resolver = hickory_resolver::Resolver::builder_tokio()
        .and_then(hickory_resolver::ResolverBuilder::build)
        .map_err(|_| PullRequestError::Unavailable)?;
    let lookup = tokio::select! {
        biased;
        reason = wait_for_interruption(control) => return Err(interruption_error(reason)),
        result = await_dns_lookup(remaining, resolver.lookup_ip(API_HOST)) => result?,
    };
    let mut addresses = lookup.iter();
    let first = addresses.next().ok_or(PullRequestError::Unavailable)?;
    if !is_public(first) || addresses.any(|address| !is_public(address)) {
        return Err(PullRequestError::Denied);
    }
    Ok(SocketAddr::new(first, API_PORT))
}

async fn await_dns_lookup<T, E>(
    remaining: Duration,
    lookup: impl Future<Output = Result<T, E>>,
) -> Result<T, PullRequestError> {
    tokio::time::timeout(remaining, lookup)
        .await
        .map_err(|_| PullRequestError::DeadlineExceeded)?
        .map_err(|_| PullRequestError::Unavailable)
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

    fn live_config() -> GitHubPrConfig {
        GitHubPrConfig {
            owner: "example".into(),
            repo: "demo".into(),
            number: 389,
            connect_timeout: Duration::from_secs(3),
            total_timeout: Duration::from_secs(10),
        }
    }

    #[test]
    fn exact_scope_denies_before_transport() {
        let config = live_config();
        for request in [
            PullRequestRequest {
                owner: "other".into(),
                repo: "demo".into(),
                number: 389,
            },
            PullRequestRequest {
                owner: "example".into(),
                repo: "other".into(),
                number: 389,
            },
            PullRequestRequest {
                owner: "example".into(),
                repo: "demo".into(),
                number: 390,
            },
        ] {
            assert!(!request_matches_scope(&config, &request));
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
    fn interruption_reasons_preserve_cancel_and_deadline_semantics() {
        assert!(matches!(
            interruption_error(InterruptionReason::Cancelled),
            PullRequestError::Unavailable
        ));
        assert!(matches!(
            interruption_error(InterruptionReason::TimedOut),
            PullRequestError::DeadlineExceeded
        ));
    }

    #[test]
    fn conditional_cache_is_selected_by_the_exact_prior_digest() {
        let object_v1 = CachedObject {
            etag: Some("\"v1\"".into()),
            body: b"v1".to_vec(),
        };
        let object_v2 = CachedObject {
            etag: Some("\"v2\"".into()),
            body: b"v2".to_vec(),
        };
        let source = |object: CachedObject| CachedSource {
            pull_request: object.clone(),
            check_runs: object.clone(),
            combined_status: object,
            observed_at: "2026-09-02T10:00:00Z".into(),
        };
        let digest_v1 = [1; SNAPSHOT_DIGEST_BYTES];
        let digest_v2 = [2; SNAPSHOT_DIGEST_BYTES];
        let mut cache = SnapshotCache::default();
        cache.insert(digest_v1, source(object_v1));
        cache.insert(digest_v2, source(object_v2));
        let prior = cache.get(&digest_v1).unwrap();
        assert_eq!(prior.pull_request.body, b"v1");

        let client = reqwest::Client::new();
        let unbound = request_builder(
            &client,
            "https://api.github.com/example".into(),
            Duration::from_secs(1),
            None,
        )
        .build()
        .unwrap();
        assert!(unbound.headers().get(reqwest::header::IF_NONE_MATCH).is_none());
        let bound = request_builder(
            &client,
            "https://api.github.com/example".into(),
            Duration::from_secs(1),
            Some(&prior.pull_request),
        )
        .build()
        .unwrap();
        assert_eq!(
            bound.headers()[reqwest::header::IF_NONE_MATCH],
            "\"v1\""
        );
    }

    #[test]
    fn paginated_ci_responses_are_rejected_instead_of_published_incomplete() {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::LINK,
            r#"<https://api.github.com/resource?page=2>; rel="next", <https://api.github.com/resource?page=4>; rel="last""#
                .parse()
                .unwrap(),
        );
        assert!(response_has_next_page(&headers));
        let incomplete = FetchedObject {
            object: CachedObject {
                etag: None,
                body: Vec::new(),
            },
            modified: true,
            has_next_page: true,
        };
        assert!(matches!(
            require_complete(incomplete),
            Err(PullRequestError::ResourceExhausted)
        ));

        headers.insert(
            reqwest::header::LINK,
            r#"<https://api.github.com/resource?page=1>; rel="prev""#
                .parse()
                .unwrap(),
        );
        assert!(!response_has_next_page(&headers));
    }

    #[test]
    fn async_dns_deadline_drops_the_pending_lookup() {
        struct PendingLookup(Arc<std::sync::atomic::AtomicBool>);

        impl Future for PendingLookup {
            type Output = Result<(), ()>;

            fn poll(
                self: std::pin::Pin<&mut Self>,
                _context: &mut std::task::Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                std::task::Poll::Pending
            }
        }

        impl Drop for PendingLookup {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }

        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let result = run_on_runtime(await_dns_lookup(
            Duration::ZERO,
            PendingLookup(Arc::clone(&dropped)),
        ));
        assert!(matches!(result, Err(PullRequestError::DeadlineExceeded)));
        assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn etags_and_head_shas_reject_header_and_path_injection() {
        assert!(valid_etag("\"safe\""));
        assert!(!valid_etag("\"safe\"\r\nx-injected: true"));
        assert!(valid_head_sha("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        assert!(!valid_head_sha("../heads/main"));
    }
}
