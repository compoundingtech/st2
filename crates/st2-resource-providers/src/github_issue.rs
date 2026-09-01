use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs as _};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use st2_resource_wasip2::{
    CapabilityContext, CapabilityModule, CapabilityPhase, InterruptionReason, InvocationControl,
    InvocationStore,
};
use wasmtime::component::{HasSelf, Linker};

mod bindings {
    wasmtime::component::bindgen!({
        path: "../../wit/github-issue",
        world: "github-issue-provider",
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
}

impl GitHubIssueModule {
    pub fn new(config: GitHubIssueConfig) -> Result<Self, &'static str> {
        config.validate()?;
        Ok(Self {
            config,
            cache: Arc::new(Mutex::new(BTreeMap::new())),
        })
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
    control: InvocationControl,
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
        bindings::GithubIssueProvider::add_to_linker::<_, HasSelf<_>>(linker, |state| state)
    }

    fn begin(&self, context: CapabilityContext<'_>) -> Self::Invocation {
        let has_authoritative_prior = match context.phase() {
            CapabilityPhase::Describe => false,
            CapabilityPhase::Observe(request) => request.prior_digest.is_some(),
        };
        GitHubIssueInvocation {
            config: self.config.clone(),
            cache: Arc::clone(&self.cache),
            has_authoritative_prior,
            control: context.control().clone(),
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
        if !request_matches_scope(&self.config, &request) {
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
            reason = wait_for_interruption(&self.control) => {
                return Err(interruption_error(reason));
            }
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
                        reason = wait_for_interruption(&self.control) => {
                            return Err(interruption_error(reason));
                        }
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
async fn wait_for_interruption(control: &InvocationControl) -> InterruptionReason {
    loop {
        if let Some(reason) = control.interruption_reason() {
            return reason;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn interruption_error(reason: InterruptionReason) -> IssueError {
    match reason {
        InterruptionReason::Cancelled => IssueError::Unavailable,
        InterruptionReason::TimedOut => IssueError::DeadlineExceeded,
    }
}

fn request_matches_scope(config: &GitHubIssueConfig, request: &IssueRequest) -> bool {
    request.owner == config.owner
        && request.repo == config.repo
        && request.number == config.number
        && request.etag.as_ref().is_none_or(|etag| valid_etag(etag))
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
            assert!(!request_matches_scope(&module.config, &request));
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
    fn shared_runtime_only_reuses_an_etag_for_a_binding_with_prior_state() {
        assert_eq!(conditional_etag(false, None, Some("\"cached\"".into())), None);
        assert_eq!(
            conditional_etag(true, None, Some("\"cached\"".into())),
            Some("\"cached\"".into())
        );
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

}
