use std::collections::{BTreeMap, VecDeque};

use anyhow::{Context, Result};
use st3_client::{
    Agent, Attention, Client, ClientError, Device, Envelope, ErrorCode, EventPage, EventType,
    Fence, Launch, Machine, Message, Mission, Page, Resource, Runtime, Snapshot, TimelineBody,
    TimelineEntry, Work,
};

/// Each collection is deliberately capped. The UI shows a truncation marker when a cap is hit.
const PAGE_SIZE: usize = 50;
const MAX_PAGES: usize = 4;

#[derive(Clone, Default)]
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

#[derive(Clone, Default)]
pub struct Model {
    pub now: Collection,
    pub messages: Collection,
    pub launches: Collection,
    pub missions: Collection,
    pub work: Collection,
    pub agents: Collection,
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
    pub async fn load(client: &Client) -> Result<Self> {
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
        model.reload(client).await?;
        Ok(model)
    }

    pub async fn reload(&mut self, client: &Client) -> Result<()> {
        let now = read_pages(client, Kind::Now).await?;
        let messages = read_pages(client, Kind::Messages).await?;
        let launches = read_pages(client, Kind::Launches).await?;
        let missions = read_pages(client, Kind::Missions).await?;
        let work = read_pages(client, Kind::Work).await?;
        let agents = read_pages(client, Kind::Agents).await?;
        let runtimes = read_pages(client, Kind::Runtimes).await?;
        let machines = read_pages(client, Kind::Machines).await?;
        let devices = read_pages(client, Kind::Devices).await?;
        (
            self.now,
            self.messages,
            self.launches,
            self.missions,
            self.work,
            self.agents,
            self.runtimes,
            self.machines,
            self.devices,
        ) = (
            now, messages, launches, missions, work, agents, runtimes, machines, devices,
        );
        self.status = "Connected".into();
        Ok(())
    }

    pub async fn sync(&mut self, client: &Client) -> Result<bool> {
        let response = client
            .events(Some(&self.event_cursor), Some(PAGE_SIZE), Some(0))
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
                return Ok(true);
            }
            Err(error) => return Err(error.into()),
        };
        let changed = self.consume_events(events);
        if changed {
            self.reload(client).await?;
        }
        Ok(changed)
    }

    fn consume_events(&mut self, events: EventPage) -> bool {
        if events.items.is_empty() {
            self.event_cursor = events.resume_cursor;
            return false;
        }
        let mut changed = false;
        for event in events.items {
            if self.recent_events.contains(&event.id) {
                self.event_cursor = event.next_cursor;
                continue;
            }
            self.recent_events.push_back(event.id);
            if self.recent_events.len() > 256 {
                self.recent_events.pop_front();
            }
            changed |= matches!(
                event.event_type,
                EventType::Upsert
                    | EventType::Delete
                    | EventType::TimelineDelta
                    | EventType::CapabilitiesChanged
                    | EventType::TerminalAvailable
            );
            self.event_cursor = event.next_cursor;
        }
        changed
    }

    pub async fn load_timeline(&mut self, client: &Client, session_id: &str) -> Result<()> {
        self.timeline.clear();
        self.timeline_truncated = false;
        let mut cursor = None;
        for page_index in 0..MAX_PAGES {
            let page = client
                .timeline(session_id, cursor.as_deref(), Some(PAGE_SIZE))
                .await?
                .value;
            self.timeline.extend(page.items);
            if !page.page.has_more {
                return Ok(());
            }
            if page_index + 1 == MAX_PAGES {
                self.timeline_truncated = true;
                return Ok(());
            }
            cursor = page.page.next_cursor;
            if cursor.is_none() {
                anyhow::bail!("timeline page omitted continuation cursor");
            }
        }
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
    Runtimes,
    Machines,
    Devices,
}

async fn read_pages(client: &Client, kind: Kind) -> Result<Collection> {
    let mut result = Collection::default();
    let mut cursor = None;
    for page_index in 0..MAX_PAGES {
        let Envelope {
            snapshot, value, ..
        }: Envelope<Page> = match kind {
            Kind::Now => {
                client
                    .now_list(cursor.as_deref(), Some(PAGE_SIZE), false)
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
        assert!(model.consume_events(envelope.value.clone()));
        let cursor = model.event_cursor.clone();
        assert!(!model.consume_events(envelope.value));
        assert_eq!(model.event_cursor, cursor);
    }
}
