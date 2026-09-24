use std::collections::{BTreeMap, VecDeque};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use st3_client::{
    Agent, Attention, Client, ClientError, Device, Envelope, ErrorCode, EventPage, EventType,
    Fence, Launch, Machine, Message, Mission, Page, Resource, Runtime, Session, Snapshot,
    TimelineBody, TimelineEntry, Work,
};

/// Each collection is deliberately capped. The UI shows a truncation marker when a cap is hit.
const PAGE_SIZE: usize = 50;
const MAX_PAGES: usize = 4;

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct Collection {
    pub items: Vec<Resource>,
    pub snapshot: Option<Snapshot>,
    pub truncated: bool,
}

impl Collection {
    pub fn find(&self, id: &str) -> Option<&Resource> {
        self.items.iter().find(|item| item.header().id == id)
    }

    pub fn fence(&self, id: &str) -> Option<Fence> {
        let item = self.find(id)?;
        let mut fence = Fence {
            snapshot_id: self.snapshot.as_ref()?.id.clone(),
            subject_revisions: BTreeMap::from([(id.to_owned(), item.header().revision.clone())]),
            ..Fence::default()
        };
        if let Resource::Runtime(runtime) = item {
            fence.runtime_incarnation = runtime.incarnation_id.clone();
            fence.terminal_sequence = runtime.terminal_sequence;
        }
        Some(fence)
    }
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct Model {
    pub now: Collection,
    pub messages: Collection,
    pub launches: Collection,
    pub missions: Collection,
    pub work: Collection,
    pub agents: Collection,
    pub sessions: Collection,
    pub runtimes: Collection,
    pub machines: Collection,
    pub devices: Collection,
    pub timeline: Vec<TimelineEntry>,
    pub timeline_truncated: bool,
    pub event_cursor: String,
    pub actor: String,
    pub status: String,
    recent_events: VecDeque<String>,
}

impl Model {
    pub async fn bootstrap(client: &Client) -> Result<Self> {
        let capabilities = client
            .capabilities()
            .await
            .context("client capabilities")?
            .value;
        let mut model = Self {
            event_cursor: capabilities.event_cursor,
            actor: capabilities.session_actor,
            ..Self::default()
        };
        let (now, agents, sessions, messages) = tokio::try_join!(
            read_pages(client, Kind::Now),
            read_pages(client, Kind::Agents),
            read_pages(client, Kind::Sessions),
            read_pages(client, Kind::Messages),
        )?;
        model.now = now;
        model.agents = agents;
        model.sessions = sessions;
        model.messages = messages;
        model.status = "Connected · loading details…".into();
        Ok(model)
    }

    pub async fn reload(&mut self, client: &Client) -> Result<()> {
        let (
            now,
            messages,
            launches,
            missions,
            work,
            agents,
            sessions,
            runtimes,
            machines,
            devices,
        ) = tokio::try_join!(
            read_pages(client, Kind::Now),
            read_pages(client, Kind::Messages),
            read_pages(client, Kind::Launches),
            read_pages(client, Kind::Missions),
            read_pages(client, Kind::Work),
            read_pages(client, Kind::Agents),
            read_pages(client, Kind::Sessions),
            read_pages(client, Kind::Runtimes),
            read_pages(client, Kind::Machines),
            read_pages(client, Kind::Devices),
        )?;
        (
            self.now,
            self.messages,
            self.launches,
            self.missions,
            self.work,
            self.agents,
            self.sessions,
            self.runtimes,
            self.machines,
            self.devices,
        ) = (
            now, messages, launches, missions, work, agents, sessions, runtimes, machines, devices,
        );
        self.status = "Connected".into();
        Ok(())
    }

    pub async fn sync(&mut self, client: &Client) -> Result<(bool, Vec<String>)> {
        let response = client
            .events(Some(&self.event_cursor), Some(PAGE_SIZE), Some(15_000))
            .await;
        let events = match response {
            Ok(envelope) => envelope.value,
            Err(ClientError::Api(ErrorCode::CursorGap, _, _)) => {
                // A cursor gap invalidates every cached projection and timeline.
                let caps = client.capabilities().await?.value;
                self.event_cursor = caps.event_cursor;
                self.timeline.clear();
                self.recent_events.clear();
                self.reload(client).await?;
                self.status = "Resynchronized after cursor gap".into();
                return Ok((true, Vec::new()));
            }
            Err(error) => return Err(error.into()),
        };
        let (changed, invalidated_sessions) = self.consume_events(events);
        if changed {
            self.reload(client).await?;
        }
        Ok((changed, invalidated_sessions))
    }

    /// Native harnesses may start outside st3, so no graph event announces them.
    pub async fn refresh_sessions(&mut self, client: &Client) -> Result<bool> {
        let next = read_pages(client, Kind::Sessions).await?;
        let changed =
            self.sessions.items != next.items || self.sessions.truncated != next.truncated;
        self.sessions = next;
        Ok(changed)
    }

    fn consume_events(&mut self, events: EventPage) -> (bool, Vec<String>) {
        if events.items.is_empty() {
            self.event_cursor = events.resume_cursor;
            return (false, Vec::new());
        }
        let mut changed = false;
        let mut invalidated_sessions = Vec::new();
        for event in events.items {
            if self.recent_events.contains(&event.id) {
                self.event_cursor = event.next_cursor;
                continue;
            }
            self.recent_events.push_back(event.id);
            if self.recent_events.len() > 256 {
                self.recent_events.pop_front();
            }
            let projection_changed = event.resource_ids.is_empty()
                || event
                    .resource_ids
                    .iter()
                    .any(|id| !id.starts_with("session/"));
            changed |= projection_changed
                && matches!(
                    event.event_type,
                    EventType::Upsert
                        | EventType::Delete
                        | EventType::TimelineDelta
                        | EventType::CapabilitiesChanged
                        | EventType::TerminalAvailable
                );
            if event.body.get("reason").and_then(serde_json::Value::as_str)
                == Some("session-timeline-invalidated")
            {
                invalidated_sessions.extend(
                    event
                        .resource_ids
                        .iter()
                        .filter(|id| id.starts_with("session/"))
                        .cloned(),
                );
            }
            self.event_cursor = event.next_cursor;
        }
        invalidated_sessions.sort();
        invalidated_sessions.dedup();
        (changed, invalidated_sessions)
    }

    pub async fn load_timeline(&mut self, client: &Client, session_id: &str) -> Result<()> {
        for attempt in 0..3 {
            match self.load_timeline_once(client, session_id).await {
                Ok(()) => return Ok(()),
                Err(error)
                    if attempt < 2
                        && error.downcast_ref::<ClientError>().is_some_and(|error| {
                            matches!(error, ClientError::Api(ErrorCode::PageCursorExpired, _, _))
                        }) =>
                {
                    tokio::time::sleep(std::time::Duration::from_millis(100 * (attempt + 1))).await;
                }
                Err(error) => return Err(error),
            }
        }
        unreachable!("bounded timeline retry returns from every attempt")
    }

    async fn load_timeline_once(&mut self, client: &Client, session_id: &str) -> Result<()> {
        // The first page is the newest window. Fetch it alone for immediate conversation
        // context; older history can be a deliberate follow-up without racing a busy cursor.
        let page = client
            .timeline(session_id, None, Some(PAGE_SIZE))
            .await?
            .value;
        self.timeline = page.items;
        self.timeline_truncated = page.page.has_more;
        self.timeline.sort_by_key(|entry| entry.sequence);
        Ok(())
    }

    pub fn attention(&self) -> impl Iterator<Item = &Attention> {
        self.now.items.iter().filter_map(|item| match item {
            Resource::Attention(v) if v.state != "resolved" && v.person_id == self.actor => Some(v),
            _ => None,
        })
    }
    pub fn agents(&self) -> impl Iterator<Item = &Agent> {
        self.agents.items.iter().filter_map(|item| match item {
            Resource::Agent(v) => Some(v),
            _ => None,
        })
    }
    pub fn undeclared_sessions(&self) -> impl Iterator<Item = &Session> {
        self.sessions.items.iter().filter_map(|item| match item {
            Resource::Session(v)
                if v.state == "running"
                    && v.extra.get("managed").and_then(serde_json::Value::as_bool)
                        == Some(false) =>
            {
                Some(v)
            }
            _ => None,
        })
    }
    pub fn messages(&self, session: Option<&str>, peer: &str) -> impl Iterator<Item = &Message> {
        self.messages
            .items
            .iter()
            .filter_map(move |item| match item {
                Resource::Message(v)
                    if v.session_id.as_deref() == session
                        && ((v.from == peer && v.to == self.actor)
                            || (v.to == peer && v.from == self.actor)) =>
                {
                    Some(v)
                }
                _ => None,
            })
    }
    pub fn launches(&self) -> impl Iterator<Item = &Launch> {
        self.launches.items.iter().filter_map(|item| match item {
            Resource::Launch(v) => Some(v),
            _ => None,
        })
    }
    pub fn missions(&self) -> impl Iterator<Item = &Mission> {
        self.missions.items.iter().filter_map(|item| match item {
            Resource::Mission(v) => Some(v),
            _ => None,
        })
    }
    pub fn work(&self) -> impl Iterator<Item = &Work> {
        self.work.items.iter().filter_map(|item| match item {
            Resource::Work(v) => Some(v),
            _ => None,
        })
    }
    pub fn runtimes(&self) -> impl Iterator<Item = &Runtime> {
        self.runtimes.items.iter().filter_map(|item| match item {
            Resource::Runtime(v) => Some(v),
            _ => None,
        })
    }
    pub fn machines(&self) -> impl Iterator<Item = &Machine> {
        self.machines.items.iter().filter_map(|item| match item {
            Resource::Machine(v) => Some(v),
            _ => None,
        })
    }
    pub fn devices(&self) -> impl Iterator<Item = &Device> {
        self.devices.items.iter().filter_map(|item| match item {
            Resource::Device(v) => Some(v),
            _ => None,
        })
    }
}

#[derive(Copy, Clone)]
enum Kind {
    Now,
    Messages,
    Launches,
    Missions,
    Work,
    Agents,
    Sessions,
    Runtimes,
    Machines,
    Devices,
}

async fn read_pages(client: &Client, kind: Kind) -> Result<Collection> {
    for attempt in 0..3 {
        match read_pages_once(client, kind).await {
            Ok(collection) => return Ok(collection),
            Err(error)
                if attempt < 2
                    && error.downcast_ref::<ClientError>().is_some_and(|error| {
                        matches!(error, ClientError::Api(ErrorCode::PageCursorExpired, _, _))
                    }) =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(100 * (attempt + 1))).await;
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("bounded page retry returns from every attempt")
}

async fn read_pages_once(client: &Client, kind: Kind) -> Result<Collection> {
    let mut result = Collection::default();
    let mut cursor = None;
    for page_index in 0..MAX_PAGES {
        let Envelope {
            snapshot, value, ..
        }: Envelope<Page> = match kind {
            Kind::Now => {
                client
                    .attention_list(cursor.as_deref(), Some(PAGE_SIZE), false)
                    .await?
            }
            Kind::Messages => {
                client
                    .messages_list(cursor.as_deref(), Some(PAGE_SIZE), false)
                    .await?
            }
            Kind::Launches => {
                client
                    .launches_list(cursor.as_deref(), Some(PAGE_SIZE), false)
                    .await?
            }
            Kind::Missions => {
                client
                    .missions_list(cursor.as_deref(), Some(PAGE_SIZE), false)
                    .await?
            }
            Kind::Work => {
                client
                    .work_list(cursor.as_deref(), Some(PAGE_SIZE), false)
                    .await?
            }
            Kind::Agents => {
                client
                    .agents_list(cursor.as_deref(), Some(PAGE_SIZE), false)
                    .await?
            }
            Kind::Sessions => {
                client
                    .sessions_list(cursor.as_deref(), Some(PAGE_SIZE), false)
                    .await?
            }
            Kind::Runtimes => {
                client
                    .runtimes_list(cursor.as_deref(), Some(PAGE_SIZE), false)
                    .await?
            }
            Kind::Machines => {
                client
                    .machines_list(cursor.as_deref(), Some(PAGE_SIZE), false)
                    .await?
            }
            Kind::Devices => {
                client
                    .devices_list(cursor.as_deref(), Some(PAGE_SIZE), false)
                    .await?
            }
        };
        result.snapshot = Some(snapshot);
        result.items.extend(value.items);
        if !value.page.has_more {
            return Ok(result);
        }
        if page_index + 1 == MAX_PAGES {
            result.truncated = true;
            return Ok(result);
        }
        cursor = value.page.next_cursor;
        if cursor.is_none() {
            anyhow::bail!("resource page omitted continuation cursor");
        }
    }
    Ok(result)
}

pub fn timeline_line(entry: &TimelineEntry) -> String {
    let role = format!("{:?}", entry.role).to_lowercase();
    let body = match &entry.body {
        TimelineBody::Content(v) => v
            .text
            .clone()
            .unwrap_or_else(|| format!("[{} attachment]", v.media_type)),
        TimelineBody::ToolCall(v) => format!("called {}", v.name),
        TimelineBody::ToolResult(v) => format!("tool result: {:?}", v.status),
        TimelineBody::Status(v) => format!("status: {:?}", v.status),
        TimelineBody::Error(v) => format!("error: {}", v.message),
        TimelineBody::Redaction(_) => "[redacted]".into(),
        TimelineBody::Truncation(_) => "[truncated]".into(),
        TimelineBody::Usage(_) => "[usage]".into(),
        TimelineBody::Message(_) => "[message]".into(),
        TimelineBody::Unknown { entry_type, .. } => format!("[{entry_type}]"),
    };
    format!("{role}: {}", body.replace('\n', " ⏎ "))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires a live local st3 daemon"]
    async fn live_full_snapshot_latency() {
        let path =
            st3_client::discover_unix_endpoint(std::env::var_os("ST3_ENDPOINT").map(Into::into))
                .unwrap();
        let actor =
            std::env::var("ST3_PERSON").expect("ST3_PERSON selects the person for this live test");
        let client = Client::unix_as(path, actor);
        let started = std::time::Instant::now();
        let mut model = Model::bootstrap(&client).await.unwrap();
        model.reload(&client).await.unwrap();
        eprintln!(
            "full snapshot: {:?}, agents: {}, sessions: {}",
            started.elapsed(),
            model.agents().count(),
            model.sessions.items.len()
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(10));
    }

    #[tokio::test]
    #[ignore = "requires a live local st3 daemon"]
    async fn live_bootstrap_latency() {
        let path =
            st3_client::discover_unix_endpoint(std::env::var_os("ST3_ENDPOINT").map(Into::into))
                .unwrap();
        let actor =
            std::env::var("ST3_PERSON").expect("ST3_PERSON selects the person for this live test");
        let client = Client::unix_as(path, actor);
        let started = std::time::Instant::now();
        let model = Model::bootstrap(&client).await.unwrap();
        eprintln!(
            "bootstrap: {:?}, agents: {}",
            started.elapsed(),
            model.agents().count()
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(3));
    }

    #[test]
    fn fixture_only_routes_person_attention_to_now() {
        let resources: Vec<Resource> = serde_json::from_str(include_str!(
            "../../../docs/st3/client-v0/fixtures/resources.json"
        ))
        .unwrap();
        let model = Model {
            actor: "person/nathan".into(),
            now: Collection {
                items: resources,
                ..Collection::default()
            },
            ..Model::default()
        };
        let attention: Vec<_> = model.attention().collect();
        assert_eq!(attention.len(), 1);
        assert_eq!(attention[0].header.id, "attention/release-review");
    }

    #[test]
    fn action_fence_uses_exact_resource_revision_and_snapshot() {
        let resource: Resource = serde_json::from_str(
            r#"{"id":"attention/one","kind":"attention","revision":"rev-7","updated_at":"2026-09-20T11:00:00Z","attention_kind":"review","source_id":"launch/one","person_id":"person/nathan","title":"Review","detail":"Choose","priority":"high","state":"open","requested_at":"2026-09-20T11:00:00Z"}"#
        ).unwrap();
        let collection = Collection {
            items: vec![resource],
            snapshot: Some(Snapshot {
                id: "snapshot/one".into(),
                host_id: "host/one".into(),
                store_index: 3,
                projection_version: "client-projection.v0".into(),
                created_at: "2026-09-20T11:00:00Z".into(),
            }),
            truncated: false,
        };
        let fence = collection.fence("attention/one").unwrap();
        assert_eq!(fence.snapshot_id, "snapshot/one");
        assert_eq!(fence.subject_revisions["attention/one"], "rev-7");
        assert!(collection.fence("attention/missing").is_none());
    }

    #[test]
    fn fixture_event_replay_does_not_trigger_another_reload() {
        let envelope: Envelope<EventPage> = serde_json::from_str(include_str!(
            "../../../docs/st3/client-v0/fixtures/events.json"
        ))
        .unwrap();
        let mut model = Model::default();
        assert!(model.consume_events(envelope.value.clone()).0);
        let cursor = model.event_cursor.clone();
        assert!(!model.consume_events(envelope.value).0);
        assert_eq!(model.event_cursor, cursor);
    }

    #[test]
    fn only_a_changed_session_invalidates_its_visible_timeline() {
        let events: EventPage = serde_json::from_value(serde_json::json!({
            "kind":"event-page",
            "oldest_cursor":"event-cursor/node/0",
            "resume_cursor":"event-cursor/node/2",
            "items":[{
                "id":"event/one", "epoch":"node", "sequence":1,
                "previous_cursor":"event-cursor/node/0", "next_cursor":"event-cursor/node/1",
                "timestamp":"2026-09-24T15:00:00Z", "type":"upsert",
                "resource_ids":["session/current"], "snapshot_id":"snapshot/one",
                "body":{"reason":"session-timeline-invalidated"}
            }, {
                "id":"event/two", "epoch":"node", "sequence":2,
                "previous_cursor":"event-cursor/node/1", "next_cursor":"event-cursor/node/2",
                "timestamp":"2026-09-24T15:00:01Z", "type":"upsert",
                "resource_ids":["agent/unrelated"], "snapshot_id":"snapshot/two",
                "body":{"reason":"client-projection-invalidated"}
            }],
            "has_more":false
        }))
        .unwrap();
        let mut model = Model::default();
        let (changed, sessions) = model.consume_events(events);
        assert!(changed);
        assert_eq!(sessions, vec!["session/current"]);
    }

    #[test]
    fn session_only_timeline_event_does_not_reload_fleet_projections() {
        let events: EventPage = serde_json::from_value(serde_json::json!({
            "kind":"event-page", "oldest_cursor":"event-cursor/node/0",
            "resume_cursor":"event-cursor/node/1", "has_more":false,
            "items":[{"id":"event/one", "epoch":"node", "sequence":1,
                "previous_cursor":"event-cursor/node/0", "next_cursor":"event-cursor/node/1",
                "timestamp":"2026-09-24T15:00:00Z", "type":"upsert",
                "resource_ids":["session/current"], "snapshot_id":"snapshot/one",
                "body":{"reason":"session-timeline-invalidated"}}]
        }))
        .unwrap();
        let mut model = Model::default();
        let (changed, sessions) = model.consume_events(events);
        assert!(!changed);
        assert_eq!(sessions, vec!["session/current"]);
    }

    #[test]
    fn running_undeclared_sessions_are_separate_from_managed_and_history() {
        let resources: Vec<Resource> = serde_json::from_str(
            r#"[
                {"kind":"session","id":"session/external","revision":"one","updated_at":"2026-09-24T09:00:00Z","owner_id":"external-session/codex/one","state":"running","started_at":"2026-09-24T08:00:00Z","ended_at":null,"timeline_cursor":"cursor/one","managed":false,"driver":"codex","native_session_id":"one"},
                {"kind":"session","id":"session/unresolved","revision":"two","updated_at":"2026-09-24T09:00:00Z","owner_id":"external-process/claude/42","state":"running","started_at":"2026-09-24T08:00:00Z","ended_at":null,"timeline_cursor":"cursor/two","managed":false,"driver":"claude","native_session_id":null},
                {"kind":"session","id":"session/managed","revision":"three","updated_at":"2026-09-24T09:00:00Z","owner_id":"agent/one","state":"running","started_at":"2026-09-24T08:00:00Z","ended_at":null,"timeline_cursor":"cursor/three"},
                {"kind":"session","id":"session/old","revision":"four","updated_at":"2026-09-24T09:00:00Z","owner_id":"external-session/codex/old","state":"completed","started_at":"2026-09-23T08:00:00Z","ended_at":"2026-09-23T09:00:00Z","timeline_cursor":"cursor/four","managed":false}
            ]"#,
        )
        .unwrap();
        let model = Model {
            sessions: Collection {
                items: resources,
                ..Collection::default()
            },
            ..Model::default()
        };
        let ids = model
            .undeclared_sessions()
            .map(|session| session.header.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, ["session/external", "session/unresolved"]);
    }
}
