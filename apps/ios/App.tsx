import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ActivityIndicator, Alert, AppState, Linking, Pressable, SafeAreaView, ScrollView, StyleSheet, Text, TextInput, View } from 'react-native';
import AsyncStorage from '@react-native-async-storage/async-storage';
import * as SecureStore from 'expo-secure-store';
import * as Crypto from 'expo-crypto';
import { API_VERSION, ClientError, St3Client, type Attention, type Capabilities, type Device, type Launch, type LaunchVariant, type Message, type Mission, type Page, type Resource, type Runtime, type Snapshot, type TerminalScreen, type TimelineEntry, type Work } from '../../clients/typescript/st3-client';
import { isSnapshotChurn, isUnmanaged, isUnresolved, listSessionPages, recentTimeline, sessionDetail, sessionLabel, timelineText, type SessionView } from './sessionView';
import { emptyData, encodeProjectionCache, hydrateProjectionForPairedDevice, offlinePresentation, PROJECTION_CACHE_KEY, type Data, type MachineView } from './projectionCache';
import { listCollectionPages, type CollectionResult } from './collectionPages';

const tabs = ['Now', 'Chat', 'Control', 'Fleet'] as const;
type Tab = typeof tabs[number];
const URL_KEY = 'st3.gateway.url', ORDER_KEY = 'st3.tabs.order', CREDENTIAL_KEY = 'st3.device.credential';
function items<K extends Resource['kind']>(page: Page, kind: K): Extract<Resource, { kind: K }>[] {
  return page.items.filter((item): item is Extract<Resource, { kind: K }> => item.kind === kind);
}
function collectionItems<K extends Resource['kind']>(collection: CollectionResult, kind: K): Extract<Resource, { kind: K }>[] {
  return collection.pages.flatMap(page => items(page.value, kind));
}
function errorText(error: unknown) { return error instanceof ClientError ? `${error.response.code}: ${error.message}` : error instanceof Error ? error.message : String(error); }
function missionDetail(mission: Mission, work: Work[]): string {
  const planned = mission.visualization?.nodes.filter(node => node.kind === 'step').length;
  const visible = work.filter(step => mission.runs.includes(step.mission_run_id));
  const current = visible.find(step => step.state === 'blocked') ?? visible.find(step => step.state === 'claimed' || step.state === 'ready') ?? visible[0];
  return `${mission.runs.length} runs${planned === undefined ? '' : ` · ${planned} planned steps`}${current ? ` · ${current.path} (${current.state})` : ''}`;
}
const missionGroups = ['Blocked', 'Waiting', 'Running', 'Drafts', 'Archive'] as const;
type MissionGroup = typeof missionGroups[number];
function missionGroup(mission: Mission, work: Work[]): MissionGroup {
  const states = work.filter(step => mission.runs.includes(step.mission_run_id)).map(step => step.state);
  if (states.includes('blocked')) return 'Blocked';
  if (states.includes('waiting')) return 'Waiting';
  if (mission.state === 'running' || mission.state === 'standing') return 'Running';
  if (mission.state === 'ready' || mission.state === 'draft') return 'Drafts';
  return 'Archive';
}
function missionLabel(mission: Mission): string {
  return (mission.title.split('/').pop() ?? mission.title).split('-').map(word => {
    const known: Record<string, string> = { ios: 'iOS', tui: 'TUI', st3: 'ST3', api: 'API', pty: 'PTY' };
    return known[word.toLowerCase()] ?? word.charAt(0).toUpperCase() + word.slice(1);
  }).join(' ');
}
function Button({ label, onPress, disabled = false }: { label: string; onPress: () => void; disabled?: boolean }) {
  return <Pressable accessibilityRole="button" disabled={disabled} onPress={onPress} style={[styles.button, disabled && styles.disabled]}><Text style={styles.buttonText}>{label}</Text></Pressable>;
}
function Card({ title, detail, children }: { title: string; detail?: string; children?: React.ReactNode }) {
  return <View style={styles.card}><Text style={styles.cardTitle}>{title}</Text>{detail ? <Text style={styles.muted}>{detail}</Text> : null}{children}</View>;
}
export default function App() {
  const [order, setOrder] = useState<Tab[]>([...tabs]);
  const [active, setActive] = useState<Tab>(() => {
    const testTab = process.env.EXPO_PUBLIC_ST3_TEST_TAB;
    return __DEV__ && tabs.includes(testTab as Tab) ? testTab as Tab : 'Now';
  });
  const [url, setUrl] = useState(''), [urlDraft, setUrlDraft] = useState('');
  const [credential, setCredential] = useState<string | null>(null);
  const [pairingId, setPairingId] = useState(''), [pairingCode, setPairingCode] = useState('');
  const [data, setData] = useState<Data>(emptyData);
  const [truncated, setTruncated] = useState<Partial<Record<keyof Data, boolean>>>({});
  const [cachedHostId, setCachedHostId] = useState('');
  const [caps, setCaps] = useState<Capabilities | null>(null), [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [status, setStatus] = useState<'setup' | 'connecting' | 'online' | 'offline'>('setup');
  const [hasSynced, setHasSynced] = useState(false);
  const [error, setError] = useState(''), [busy, setBusy] = useState(false);
  const [sessionId, setSessionId] = useState(''), [timeline, setTimeline] = useState<TimelineEntry[]>([]), [composer, setComposer] = useState('');
  const [chatDetailOpen, setChatDetailOpen] = useState(false);
  const [showHistory, setShowHistory] = useState(false), [historicalSessions, setHistoricalSessions] = useState<SessionView[]>([]), [historyBusy, setHistoryBusy] = useState(false);
  const [terminalId, setTerminalId] = useState(''), [screen, setScreen] = useState<TerminalScreen | null>(null), [terminalIssue, setTerminalIssue] = useState('');
  const [reviewLaunch, setReviewLaunch] = useState(''), [variants, setVariants] = useState<LaunchVariant[]>([]);
  const [selectedMissionId, setSelectedMissionId] = useState(''), [showSystemMissions, setShowSystemMissions] = useState(false), [showPlanner, setShowPlanner] = useState(false);
  const [title, setTitle] = useState(''), [request, setRequest] = useState(''), [workspace, setWorkspace] = useState('');
  const [provider, setProvider] = useState<'codex' | 'claude' | 'pi' | 'omp' | 'opencode'>('codex');
  const [model, setModel] = useState(''), [effort, setEffort] = useState(''), [feedback, setFeedback] = useState('');
  const refreshing = useRef(false), snapshotRetry = useRef(0);
  const cachedActor = useRef(''), cachedIndex = useRef(-1), cacheSavedAt = useRef(0);
  const cacheGeneration = useRef(0);
  const client = useMemo(() => url ? new St3Client({ baseUrl: url, credential: () => credential ?? undefined }) : null, [url, credential]);

  useEffect(() => { Promise.allSettled([AsyncStorage.getItem(URL_KEY), AsyncStorage.getItem(ORDER_KEY), SecureStore.getItemAsync(CREDENTIAL_KEY), AsyncStorage.getItem(PROJECTION_CACHE_KEY)]).then(([u, o, c, p]) => {
    if (u.status === 'fulfilled' && u.value) { setUrl(u.value); setUrlDraft(u.value); }
    if (o.status === 'fulfilled' && o.value) { try { const parsed: unknown = JSON.parse(o.value); if (Array.isArray(parsed) && parsed.length === 4 && tabs.every(t => parsed.includes(t))) setOrder(parsed as Tab[]); } catch { /* use default */ } }
    if (c.status === 'fulfilled' && c.value) {
      if (u.status === 'fulfilled' && u.value && p.status === 'fulfilled') {
        const cache = hydrateProjectionForPairedDevice(p.value, u.value, true);
        if (cache) { setData(cache.data); setTruncated(Object.fromEntries(cache.truncated.map(key => [key, true]))); setHasSynced(true); setStatus('connecting'); cachedActor.current = cache.actor; cachedIndex.current = cache.storeIndex; cacheSavedAt.current = cache.savedAt; setCachedHostId(cache.hostId); }
      }
      setCredential(c.value);
    }
    if (c.status === 'rejected') setError('Secure credential storage is unavailable on this build.');
  }); }, []);
  useEffect(() => {
    if (!__DEV__) return;
    let handled = false;
    async function handleDevPairLink(link: string | null) {
      if (!link || handled) return;
      const parsed = new URL(link);
      if (parsed.hostname !== 'pair') return;
      const gateway = parsed.searchParams.get('gateway')?.replace(/\/+$/, '');
      const id = parsed.searchParams.get('id');
      const code = parsed.searchParams.get('code');
      if (!gateway?.startsWith('https://') || !id || !code) return;
      handled = true;
      setBusy(true);
      try {
        const publicKey = Array.from(Crypto.getRandomBytes(32), b => b.toString(16).padStart(2, '0')).join('');
        const result = await new St3Client({ baseUrl: gateway }).completePairing(id, { api_version: API_VERSION, code, device_public_key: publicKey });
        await clearCachedProjection();
        await SecureStore.setItemAsync(CREDENTIAL_KEY, result.value.credential, { keychainAccessible: SecureStore.WHEN_UNLOCKED_THIS_DEVICE_ONLY });
        await AsyncStorage.setItem(URL_KEY, gateway);
        setUrl(gateway); setUrlDraft(gateway); setCredential(result.value.credential); setError('');
      } catch (e) { setError(errorText(e)); }
      finally { setBusy(false); }
    }
    void Linking.getInitialURL().then(handleDevPairLink);
    void handleDevPairLink(process.env.EXPO_PUBLIC_ST3_TEST_PAIR_LINK ?? null);
    const subscription = Linking.addEventListener('url', event => { void handleDevPairLink(event.url); });
    return () => subscription.remove();
  }, []);

  const refresh = useCallback(async () => {
    if (!client || !credential) { setStatus('setup'); return; }
    if (refreshing.current) return;
    refreshing.current = true;
    const generation = cacheGeneration.current;
    setStatus(s => s === 'online' ? s : 'connecting');
    try {
      const capability = await client.capabilities(), limit = Math.min(capability.value.limits.max_page_items, 30);
      if (cachedActor.current && cachedActor.current !== capability.value.session_actor) {
        cachedActor.current = ''; cachedIndex.current = -1; cacheSavedAt.current = 0;
        setData(emptyData); setTruncated({}); setHasSynced(false); setCachedHostId(''); setSnapshot(null); setStatus('connecting');
        void AsyncStorage.removeItem(PROJECTION_CACHE_KEY).catch(() => {});
      }
      const [collections, sessions] = await Promise.all([Promise.all([
        listCollectionPages(options => client.attentionList(options), limit, 10),
        listCollectionPages(options => client.messagesList(options), limit),
        listCollectionPages(options => client.missionsList(options), limit),
        listCollectionPages(options => client.launchesList(options), limit),
        listCollectionPages(options => client.machinesList(options), limit),
        listCollectionPages(options => client.devicesList(options), limit),
        listCollectionPages(options => client.runtimesList(options), limit),
        listCollectionPages(options => client.workList(options), limit),
      ]), listSessionPages(options => client.sessionsList(options), limit)]);
      if (generation !== cacheGeneration.current) return;
      const [attention, messages, missions, launches, machines, devices, runtimes, work] = collections;
      const firstSnapshot = attention.pages[0].snapshot;
      setCaps(capability.value); setSnapshot(firstSnapshot);
      const fresh: Data = { attention: collectionItems(attention, 'attention').filter(a => a.state === 'open' && a.actions.length > 0 && a.attention_kind !== 'unread-message'), messages: collectionItems(messages, 'message'), missions: collectionItems(missions, 'mission'), launches: collectionItems(launches, 'launch'), machines: machines.pages.flatMap(page => page.value.items.filter(i => (i as unknown as { kind: string }).kind === 'machine')) as unknown as MachineView[], devices: collectionItems(devices, 'device'), sessions, runtimes: collectionItems(runtimes, 'runtime'), work: collectionItems(work, 'work') };
      const truncatedKeys: Array<keyof Data> = (['attention', 'messages', 'missions', 'launches', 'machines', 'devices', 'runtimes', 'work'] as const).filter((_, index) => collections[index].truncated);
      setData(fresh); setTruncated(Object.fromEntries(truncatedKeys.map(key => [key, true]))); setCachedHostId(firstSnapshot.host_id);
      const now = Date.now(), index = firstSnapshot.store_index, actor = capability.value.session_actor;
      if (cachedActor.current !== actor || cachedIndex.current !== index || now - cacheSavedAt.current > 5 * 60 * 1000) {
        const encoded = encodeProjectionCache(url, actor, firstSnapshot.host_id, index, fresh, now, truncatedKeys);
        if (encoded) { cachedActor.current = actor; cachedIndex.current = index; cacheSavedAt.current = now; void AsyncStorage.setItem(PROJECTION_CACHE_KEY, encoded).catch(() => { cacheSavedAt.current = 0; }); }
      }
      snapshotRetry.current = 0; setHasSynced(true); setStatus('online'); setError('');
    } catch (e) {
      if (generation !== cacheGeneration.current) return;
      if (isSnapshotChurn(e)) {
        const delay = Math.min(2000, 200 * 2 ** Math.min(snapshotRetry.current++, 4));
        setStatus(s => s === 'online' ? s : 'connecting');
        if (AppState.currentState === 'active') setTimeout(() => { void refresh(); }, delay);
      } else {
        setStatus('offline');
        setError(e instanceof ClientError && e.status >= 400 && e.status < 500 ? errorText(e) : '');
      }
    } finally { refreshing.current = false; }
  }, [client, credential, url]);
  useEffect(() => { void refresh(); }, [refresh]);
  useEffect(() => { if (status === 'setup') return; const timer = setInterval(() => { if (AppState.currentState === 'active') void refresh(); }, 15000); return () => clearInterval(timer); }, [refresh, status]);
  useEffect(() => { if (!sessionId && data.sessions.some(s => s.state === 'running')) setSessionId(data.sessions.find(s => s.state === 'running')!.id); }, [data.sessions, sessionId]);
  const timelineSession = data.sessions.find(s => s.id === sessionId) ?? historicalSessions.find(s => s.id === sessionId);
  const timelineUnresolved = timelineSession ? isUnresolved(timelineSession) : false;
  useEffect(() => {
    if (!client || !sessionId || timelineUnresolved) { setTimeline([]); return; }
    if (status !== 'online') return;
    let live = true;
    void (async () => {
      for (let attempt = 0; attempt < 4 && live; attempt++) {
        try {
          let cursor: string | undefined;
          let entries: TimelineEntry[] = [];
          for (let page = 0; page < 5; page++) {
            const result = await client.timelineList(sessionId, { limit: Math.min(caps?.limits.max_page_items ?? 30, 30), cursor });
            entries = entries.concat(result.value.items);
            if (!result.value.page.has_more || !result.value.page.next_cursor) break;
            cursor = result.value.page.next_cursor;
          }
          if (live) setTimeline(recentTimeline(entries));
          return;
        } catch (error) {
          if (!isSnapshotChurn(error)) {
            if (live && error instanceof ClientError && error.status >= 400 && error.status < 500) setError(errorText(error));
            return;
          }
          if (attempt < 3) await new Promise(resolve => setTimeout(resolve, 150 * (attempt + 1)));
        }
      }
    })();
    return () => { live = false; };
  }, [client, sessionId, status, caps, timelineUnresolved]);
  useEffect(() => {
    if (!client || !terminalId || !chatDetailOpen || active !== 'Chat') return;
    let live = true, polling = false, unavailable = false;
    async function poll() {
      if (!live || polling || unavailable || status !== 'online' || AppState.currentState !== 'active') return;
      polling = true;
      try {
        const result = await client!.terminalScreen(terminalId);
        if (live) { setScreen(result.value); setTerminalIssue(''); }
      } catch (error) {
        if (live && error instanceof ClientError && error.status >= 400 && error.status < 500) { unavailable = true; setTerminalIssue(errorText(error)); }
      } finally { polling = false; }
    }
    void poll();
    const timer = setInterval(() => { void poll(); }, 1500);
    return () => { live = false; clearInterval(timer); };
  }, [client, terminalId, chatDetailOpen, active, status]);

  async function review(id: string) { if (!client || status !== 'online') return; try { const result = await client.launchVariantsList(id, { limit: Math.min(caps?.limits.max_page_items ?? 30, 30) }); setReviewLaunch(id); setVariants(items(result.value, 'launch-variant')); setError(''); } catch (e) { setError(errorText(e)); } }
  async function preview(launch: Launch, variant: LaunchVariant) { if (!client) return; await runAction(() => { const id = actionId(); return client.launchPreview({ id, idempotency_key: id, fence: fence({ [launch.id]: launch.revision, [variant.id]: variant.revision }), parameters: { launch_id: launch.id, variant_id: variant.id } }); }); void review(launch.id); }
  async function approve(launch: Launch, variant: LaunchVariant) { if (!client || !variant.preview_token) return; await runAction(() => { const id = actionId(); return client.launchApprove({ id, idempotency_key: id, fence: { ...fence({ [launch.id]: launch.revision, [variant.id]: variant.revision }), preview_token: variant.preview_token! }, parameters: { launch_id: launch.id, variant_id: variant.id } }); }); setVariants([]); }
  function showTerminal(id: string) { if (!client || status !== 'online') return; setScreen(null); setTerminalIssue(''); setTerminalId(id); setError(''); }
  async function clearCachedProjection() { cacheGeneration.current++; cachedActor.current = ''; cachedIndex.current = -1; cacheSavedAt.current = 0; setData(emptyData); setTruncated({}); setHasSynced(false); setCachedHostId(''); setSnapshot(null); setTimeline([]); await AsyncStorage.removeItem(PROJECTION_CACHE_KEY).catch(() => {}); }
  async function saveUrl() { const normalized = urlDraft.trim().replace(/\/+$/, ''); if (!/^https:\/\//.test(normalized)) { setError('Enter the paired gateway HTTPS URL.'); return; } if (normalized !== url) await clearCachedProjection(); await AsyncStorage.setItem(URL_KEY, normalized); setUrl(normalized); setError(''); }
  async function pair() { if (!client || !pairingId.trim() || !pairingCode.trim()) return; setBusy(true); try {
    const publicKey = Array.from(Crypto.getRandomBytes(32), b => b.toString(16).padStart(2, '0')).join('');
    const result = await client.completePairing(pairingId.trim(), { api_version: API_VERSION, code: pairingCode.trim(), device_public_key: publicKey });
    await clearCachedProjection();
    await SecureStore.setItemAsync(CREDENTIAL_KEY, result.value.credential, { keychainAccessible: SecureStore.WHEN_UNLOCKED_THIS_DEVICE_ONLY });
    setCredential(result.value.credential); setPairingCode(''); setPairingId(''); setError('');
  } catch (e) { setError(errorText(e)); } finally { setBusy(false); } }
  async function runAction(action: () => Promise<unknown>) { if (status !== 'online') { setError('Reconnect before sending an action.'); return; } setBusy(true); try { await action(); setError(''); await refresh(); } catch (e) { setError(errorText(e)); } finally { setBusy(false); } }
  function fence(revisions: Record<string, string> = {}) { if (!snapshot) throw new Error('Refresh before acting.'); return { snapshot_id: snapshot.id, subject_revisions: revisions }; }
  function actionId() { return `action/ios-${Crypto.randomUUID()}`; }
  async function send() { const session = data.sessions.find(s => s.id === sessionId); if (!client || !session || isUnmanaged(session) || session.state !== 'running' || !composer.trim()) return; const content = composer.trim(); await runAction(async () => { const id = actionId(); await client.messageSend({ id, idempotency_key: id, fence: fence(), parameters: { content, session_id: session.id, to: session.owner_id } }); setComposer(''); }); }
  async function createLaunch() { if (!client || !title.trim() || !request.trim() || !workspace.trim()) return; await runAction(async () => { const id = actionId(); await client.launchCreate({ id, idempotency_key: id, fence: fence(), parameters: { title: title.trim(), request: request.trim(), target: { type: 'new-mission', mission_id: `mission/ios-${Crypto.randomUUID()}`, workspace: workspace.trim() }, provider, ...(model.trim() ? { model: model.trim() } : {}), ...(effort.trim() ? { effort: effort.trim() } : {}) } }); setTitle(''); setRequest(''); }); }
  async function reviseLaunch(launch: Launch) { if (!client || !feedback.trim()) return; await runAction(async () => { const id = actionId(); await client.launchRevise({ id, idempotency_key: id, fence: fence({ [launch.id]: launch.revision }), parameters: { launch_id: launch.id, feedback: feedback.trim() } }); setFeedback(''); }); }
  async function resolve(item: Attention) { if (!client) return; await runAction(() => { const id = actionId(); return client.attentionResolve({ id, idempotency_key: id, fence: fence({ [item.id]: item.revision }), parameters: { attention_id: item.id, outcome: 'resolved' } }); }); }
  async function forget() { await SecureStore.deleteItemAsync(CREDENTIAL_KEY); await clearCachedProjection(); setCredential(null); setCaps(null); setHistoricalSessions([]); setShowHistory(false); setSessionId(''); setTerminalId(''); setScreen(null); setTerminalIssue(''); setStatus('setup'); }
  async function openHistory() { if (!client || !caps) return; setHistoryBusy(true); try { setHistoricalSessions((await listSessionPages(options => client.sessionsList(options), Math.min(caps.limits.max_page_items, 30), true)).filter(s => ['completed', 'failed', 'cancelled'].includes(s.state))); setShowHistory(true); setError(''); } catch (e) { if (!isSnapshotChurn(e)) setError(errorText(e)); } finally { setHistoryBusy(false); } }
  function move(tab: Tab, direction: -1 | 1) { const index = order.indexOf(tab), next = index + direction; if (next < 0 || next >= order.length) return; const updated = [...order]; [updated[index], updated[next]] = [updated[next], updated[index]]; setOrder(updated); void AsyncStorage.setItem(ORDER_KEY, JSON.stringify(updated)); }
  const selectedSession = [...data.sessions, ...historicalSessions].find(s => s.id === sessionId);
  const currentSessions = data.sessions.filter(s => s.state === 'running').sort((a, b) => Number(isUnmanaged(b)) - Number(isUnmanaged(a)));
  const knownHostId = snapshot?.host_id ?? cachedHostId;
  const gatewayMachineId = knownHostId ? `machine/${knownHostId.replace(/^host\//, '')}` : '';
  const sourceHost = data.machines.find(m => m.id === gatewayMachineId)?.name ?? knownHostId?.replace(/^host\//, '') ?? 'connected gateway host';
  const undeclaredSessions = currentSessions.filter(isUnmanaged);
  const managedSessions = currentSessions.filter(s => !isUnmanaged(s));
  const sessionMessages = data.messages.filter(m => m.session_id === sessionId).sort((a, b) => a.sent_at.localeCompare(b.sent_at));
  const conversationEntries = timeline.filter(e => e.type === 'content' && timelineText(e.body) !== null);
  const sessionChoice = (s: SessionView) => <Pressable key={s.id} onPress={() => { setSessionId(s.id); setTimeline([]); setTerminalId(''); setScreen(null); setTerminalIssue(''); setChatDetailOpen(true); }} style={[styles.choice, sessionId === s.id && styles.selected]}><Text style={styles.cardTitle}>{sessionLabel(s, sourceHost)}</Text><Text style={styles.small}>{sessionDetail(s)}</Text></Pressable>;
  const visibleMissions = data.missions.filter(m => showSystemMissions || !m.id.startsWith('mission/__st3/'));
  const selectedMission = visibleMissions.find(m => m.id === selectedMissionId);

  return <SafeAreaView style={styles.page}>
    <View style={styles.header}><Text style={styles.brand}>Smalltalk</Text><Text style={[styles.status, status === 'online' && styles.good]}>{status === 'online' ? 'Connected' : status === 'connecting' ? hasSynced ? 'Updating · showing last data' : 'Connecting…' : status === 'offline' ? offlinePresentation(hasSynced).title : 'Pair this device'}</Text></View>
    {error ? <Pressable onPress={() => setError('')} style={styles.error}><Text style={styles.errorText}>{error}</Text></Pressable> : null}{busy ? <ActivityIndicator color="#67d6c5" /> : null}
    <ScrollView style={styles.content} contentContainerStyle={styles.scroll} keyboardShouldPersistTaps="handled">
      {!url || !credential ? <><Text style={styles.title}>Connect to Smalltalk</Text><Text style={styles.muted}>Use the paired-only Tailscale HTTPS gateway. Begin pairing on a trusted st3 machine, then enter its short-lived ID and code.</Text>
        <TextInput style={styles.input} autoCapitalize="none" autoCorrect={false} keyboardType="url" placeholder="https://your-tailnet-host" placeholderTextColor="#8195a2" value={urlDraft} onChangeText={setUrlDraft} /><Button label="Save gateway" onPress={() => void saveUrl()} />
        {url ? <><TextInput style={styles.input} autoCapitalize="none" placeholder="Pairing ID" placeholderTextColor="#8195a2" value={pairingId} onChangeText={setPairingId} /><TextInput style={styles.input} autoCapitalize="none" placeholder="Pairing code" placeholderTextColor="#8195a2" value={pairingCode} onChangeText={setPairingCode} /><Button label="Pair device" disabled={busy} onPress={() => void pair()} /></> : null}</> : !hasSynced && status !== 'online' ? <>
        <Text style={styles.title}>{status === 'offline' ? 'Offline' : 'Connecting…'}</Text>
        <Text style={styles.muted}>{status === 'offline' ? offlinePresentation(false).detail : 'Loading your workspace for the first time.'}</Text>
        {status === 'offline' ? <Button label="Reconnect" onPress={() => void refresh()} /> : null}
      </> : <>
        {status === 'offline' ? <Button label="Reconnect" onPress={() => void refresh()} /> : null}
        {active === 'Now' ? <><Text style={styles.title}>Needs your attention</Text><Text style={styles.muted}>{data.attention.length ? `${data.attention.length} actionable items` : 'Nothing needs your attention.'}</Text>{truncated.attention ? <Text style={styles.warning}>More attention items exist beyond this view. Open the full inbox in the CLI to see them all.</Text> : null}{data.attention.map(a => <Card key={a.id} title={a.title} detail={`${a.priority} · ${a.detail}`}><Text style={styles.small}>{a.attention_kind} · {a.source_id}</Text>{a.actions.includes('attention.resolve') ? <Button label="Resolve" disabled={busy} onPress={() => Alert.alert('Resolve attention?', a.title, [{ text: 'Cancel' }, { text: 'Resolve', onPress: () => void resolve(a) }])} /> : null}</Card>)}<Button label="Refresh" onPress={() => void refresh()} /></> : null}
        {active === 'Chat' ? chatDetailOpen && selectedSession ? <>
          <Button label="← Agents" onPress={() => { setChatDetailOpen(false); setTerminalId(''); setScreen(null); setTerminalIssue(''); }} />
          <Text style={styles.section}>{sessionLabel(selectedSession, sourceHost)}</Text>
          <Text style={styles.muted}>{sessionDetail(selectedSession)}</Text>
          {terminalId ? <>
            <Button label="← Conversation" onPress={() => { setScreen(null); setTerminalId(''); setTerminalIssue(''); }} />
            <Card title="Terminal · read-only" detail={screen ? screen.lines.map(line => line.text).join('\n') : terminalIssue || (status === 'online' ? 'Loading terminal screen…' : 'Offline; no terminal screen is cached.')} />
            {screen && status !== 'online' ? <Text style={styles.muted}>Offline · showing the last terminal frame.</Text> : null}
          </> : <>
            {!isUnmanaged(selectedSession) && selectedSession.state === 'running' ? data.runtimes.filter(r => r.terminal_id && r.owner_id === selectedSession.owner_id).map(r => <Button key={r.id} label="View terminal" disabled={status !== 'online'} onPress={() => void showTerminal(r.terminal_id!)} />) : null}
            <Text style={styles.section}>Conversation</Text>
            {truncated.messages ? <Text style={styles.warning}>The message fallback is partial; open this session in the CLI for complete history.</Text> : null}
            {isUnresolved(selectedSession) ? <Text style={styles.muted}>This process has no exact native session history.</Text> : null}
            {conversationEntries.map(e => <Card key={e.id} title={e.role === 'assistant' ? 'Agent' : e.role === 'user' ? 'You' : e.role} detail={timelineText(e.body) ?? ''} />)}
            {!conversationEntries.length ? sessionMessages.map(m => <Card key={m.id} title={m.from} detail={m.content} />) : null}
            {!isUnmanaged(selectedSession) && selectedSession.state === 'running' ? <><TextInput style={[styles.input, styles.composer]} multiline placeholder="Message this session" placeholderTextColor="#8195a2" value={composer} onChangeText={setComposer} /><Button label="Send" disabled={busy || status !== 'online' || !composer.trim()} onPress={() => void send()} /></> : null}
          </>}
        </> : <>
          <Text style={styles.title}>Chat</Text><Text style={styles.muted}>Running sessions from this gateway.</Text>
          <Text style={styles.section}>Undeclared on {sourceHost}</Text>{undeclaredSessions.map(sessionChoice)}{!undeclaredSessions.length ? <Text style={styles.muted}>None discovered on this machine.</Text> : null}
          <Text style={styles.section}>Declared agents</Text>{managedSessions.map(sessionChoice)}{!managedSessions.length ? <Text style={styles.muted}>No running declared agents.</Text> : null}
          <Text style={styles.section}>Past sessions</Text><Button label={showHistory ? 'Refresh past sessions' : 'Show past sessions'} disabled={historyBusy || status !== 'online'} onPress={() => void openHistory()} />{showHistory ? historicalSessions.map(sessionChoice) : null}
        </> : null}
        {active === 'Control' ? <>
          <Text style={styles.title}>Control</Text>
          <Text style={styles.muted}>Missions grouped by what needs action. Select one to see its work tree and blockers.</Text>
          {truncated.missions || truncated.work ? <Text style={styles.warning}>This view is partial. Use the CLI for complete mission and work lists.</Text> : null}
          {missionGroups.map(group => {
            const missions = visibleMissions.filter(m => missionGroup(m, data.work) === group);
            return missions.length ? <View key={group}><Text style={styles.section}>{group} · {missions.length}</Text>{missions.map(m => <Pressable key={m.id} onPress={() => setSelectedMissionId(m.id)} style={[styles.choice, selectedMissionId === m.id && styles.selected]}><Text style={styles.cardTitle}>{missionLabel(m)}</Text><Text style={styles.small}>{missionDetail(m, data.work)}</Text></Pressable>)}</View> : null;
          })}
          <Button label={showSystemMissions ? 'Hide system missions' : 'Show system missions'} onPress={() => setShowSystemMissions(!showSystemMissions)} />
          {selectedMission ? <Card title={missionLabel(selectedMission)} detail={`${missionGroup(selectedMission, data.work)} · ${selectedMission.id}`}>
            <Text style={styles.section}>Work tree</Text>
            {data.work.filter(w => selectedMission.runs.includes(w.mission_run_id)).sort((a, b) => a.path.localeCompare(b.path)).map(w => <View key={w.id} style={{ marginLeft: Math.min(3, w.path.split('/').length - 1) * 14, marginTop: 10 }}><Text style={styles.cardTitle}>↳ {w.path.split('/').pop()} · {w.state}</Text>{w.blocked_reason ? <Text style={styles.warning}>Blocked: {w.blocked_reason}</Text> : null}{w.goals[0] ? <Text style={styles.muted}>Goal: {w.goals[0]}</Text> : null}{w.claimant ? <Text style={styles.small}>Agent: {w.claimant}</Text> : null}</View>)}
            {selectedMission.visualization?.groups.filter(g => g.kind === 'nested-mission').map(g => <Text key={g.id} style={styles.muted}>↳ Nested mission: {g.members.join(', ')}</Text>)}
          </Card> : null}
          <Text style={styles.section}>Plan a mission</Text><Button label={showPlanner ? 'Hide planner' : 'New mission'} onPress={() => setShowPlanner(!showPlanner)} />
          {showPlanner ? <>
          <Text style={styles.muted}>Start a configurable planner. Review and approval stay in st3.</Text>
          <TextInput style={styles.input} placeholder="Title" placeholderTextColor="#8195a2" value={title} onChangeText={setTitle} />
          <TextInput style={[styles.input, styles.composer]} multiline placeholder="What should be done?" placeholderTextColor="#8195a2" value={request} onChangeText={setRequest} />
          <TextInput style={styles.input} autoCapitalize="none" placeholder="Workspace on target machine" placeholderTextColor="#8195a2" value={workspace} onChangeText={setWorkspace} />
          <Text style={styles.small}>Planner</Text>
          <View style={styles.row}>{(['codex', 'claude', 'pi', 'omp', 'opencode'] as const).map(p => <Pressable key={p} onPress={() => setProvider(p)} style={[styles.chip, provider === p && styles.selected]}><Text style={styles.small}>{p}</Text></Pressable>)}</View>
          <TextInput style={styles.input} placeholder="Model (optional)" placeholderTextColor="#8195a2" value={model} onChangeText={setModel} />
          <TextInput style={styles.input} placeholder="Effort (optional)" placeholderTextColor="#8195a2" value={effort} onChangeText={setEffort} />
          <Button label="Create launch" disabled={busy || !title.trim() || !request.trim() || !workspace.trim()} onPress={() => void createLaunch()} />
          </> : null}
          <Text style={styles.section}>Launches</Text>{truncated.launches ? <Text style={styles.warning}>More launches exist beyond this view. Use the CLI for the complete list.</Text> : null}
          {data.launches.map(l => <Card key={l.id} title={l.title} detail={`${l.phase} · ${l.planner_config.provider} · ${l.id}`}><Text style={styles.muted}>{l.request}</Text><Button label="Review variants" onPress={() => void review(l.id)} />{reviewLaunch === l.id ? variants.map(v => <Card key={v.id} title={`Variant ${v.ordinal} · ${v.status}`} detail={v.diagnostics.map(d => `${d.severity}: ${d.message}`).join(' · ') || 'No diagnostics'}><Button label="Preview" disabled={busy} onPress={() => void preview(l, v)} />{v.preview_token ? <Button label="Approve" disabled={busy} onPress={() => Alert.alert('Approve launch variant?', l.title, [{ text: 'Cancel' }, { text: 'Approve', onPress: () => void approve(l, v) }])} /> : null}</Card>) : null}{l.phase === 'authoring' || l.phase === 'review' ? <><TextInput style={styles.input} placeholder="Planner feedback" placeholderTextColor="#8195a2" value={feedback} onChangeText={setFeedback} /><Button label="Revise" disabled={busy || !feedback.trim()} onPress={() => void reviseLaunch(l)} /></> : null}</Card>)}
        </> : null}
        {active === 'Fleet' ? <>
          <Text style={styles.title}>Fleet</Text>{truncated.machines || truncated.runtimes || truncated.devices ? <Text style={styles.warning}>This fleet view is partial. Use the CLI for the complete machine, runtime, and device lists.</Text> : null}
          {data.machines.map(m => <Card key={m.id} title={m.name} detail={`${m.state} · ${m.occupancy.running_runtimes} runtimes · ${m.capacity.state}`}>
            <Text style={styles.small}>{m.transports.map(t => `${t.protocol}: ${t.status}`).join(' · ')}</Text>
            {m.id === gatewayMachineId ? <><Text style={styles.section}>Undeclared sessions</Text>{undeclaredSessions.map(s => <Text key={s.id} style={styles.muted}>{isUnresolved(s) ? 'Unresolved running process' : 'Exact native session'} · {s.driver ?? 'native harness'}{s.process ? ` · PID ${s.process.pid}` : ''}{s.title ? ` · ${s.title}` : ''}</Text>)}{!undeclaredSessions.length ? <Text style={styles.muted}>None discovered on this machine.</Text> : null}</> : null}
          </Card>)}
          {!data.machines.some(m => m.id === gatewayMachineId) ? <Card title={sourceHost} detail="Connected gateway machine"><Text style={styles.section}>Undeclared sessions</Text>{undeclaredSessions.map(s => <Text key={s.id} style={styles.muted}>{isUnresolved(s) ? 'Unresolved running process' : 'Exact native session'} · {s.driver ?? 'native harness'}{s.process ? ` · PID ${s.process.pid}` : ''}</Text>)}{!undeclaredSessions.length ? <Text style={styles.muted}>None discovered on this machine.</Text> : null}</Card> : null}
          <Text style={styles.muted}>Discovery covers the connected gateway machine only.</Text>
          <Text style={styles.section}>You & devices</Text>
          <Card title="This connection" detail={caps ? `${caps.session_actor} · ${caps.transport}` : 'Reconnecting'}><Text style={styles.small}>{url}</Text><Button label="Forget local credential" onPress={() => Alert.alert('Forget this device?', 'You will need to pair again.', [{ text: 'Cancel' }, { text: 'Forget', onPress: () => void forget() }])} /></Card>
          {data.devices.map(d => <Card key={d.id} title={d.id} detail={`${d.state} · ${d.person_id}`}><Text style={styles.small}>{d.scopes.join(', ')}</Text></Card>)}
          <Text style={styles.section}>Tab order</Text>
          {order.map(t => <View key={t} style={styles.orderRow}><Text style={styles.cardTitle}>{t}</Text><Button label="↑" onPress={() => move(t, -1)} /><Button label="↓" onPress={() => move(t, 1)} /></View>)}
        </> : null}
      </>}
    </ScrollView><View accessibilityRole="tablist" style={styles.tabs}>{order.map(t => <Pressable key={t} accessibilityRole="tab" accessibilityState={{ selected: active === t }} onPress={() => setActive(t)} style={[styles.tab, active === t && styles.activeTab]}><Text style={[styles.tabText, active === t && styles.activeTabText]}>{t}</Text></Pressable>)}</View>
  </SafeAreaView>;
}
const styles = StyleSheet.create({ page: { flex: 1, backgroundColor: '#101923' }, header: { paddingHorizontal: 22, paddingTop: 16, paddingBottom: 14, borderBottomColor: '#344651', borderBottomWidth: 1 }, brand: { color: '#f3f7fa', fontSize: 24, fontWeight: '700' }, status: { color: '#f0ad69', marginTop: 4 }, good: { color: '#67d6c5' }, content: { flex: 1 }, scroll: { padding: 20, paddingBottom: 48 }, title: { color: '#f3f7fa', fontSize: 27, fontWeight: '700', marginBottom: 10 }, section: { color: '#67d6c5', fontSize: 20, fontWeight: '700', marginTop: 25, marginBottom: 10 }, muted: { color: '#b8c7d0', fontSize: 14, lineHeight: 21, marginTop: 4 }, warning: { color: '#f0c77c', fontSize: 13, lineHeight: 19, marginTop: 8, marginBottom: 6 }, small: { color: '#a9bac5', fontSize: 12, lineHeight: 18 }, card: { backgroundColor: '#1b2b36', borderRadius: 12, padding: 15, marginTop: 10 }, cardTitle: { color: '#f3f7fa', fontSize: 16, fontWeight: '600' }, input: { borderWidth: 1, borderColor: '#49606b', borderRadius: 10, color: '#f3f7fa', padding: 12, marginTop: 11, fontSize: 15 }, composer: { minHeight: 86, textAlignVertical: 'top' }, button: { backgroundColor: '#176d69', borderRadius: 9, paddingVertical: 10, paddingHorizontal: 13, alignSelf: 'flex-start', marginTop: 10 }, buttonText: { color: '#fff', fontWeight: '700', fontSize: 14 }, disabled: { opacity: 0.45 }, choice: { borderColor: '#49606b', borderWidth: 1, borderRadius: 10, padding: 10, marginTop: 8 }, selected: { borderColor: '#67d6c5', backgroundColor: '#214144' }, row: { flexDirection: 'row', flexWrap: 'wrap', gap: 5 }, chip: { borderRadius: 8, borderWidth: 1, borderColor: '#49606b', padding: 7, marginTop: 7 }, error: { backgroundColor: '#633b3b', padding: 10 }, errorText: { color: '#fff3ed' }, orderRow: { flexDirection: 'row', alignItems: 'center', justifyContent: 'space-between', paddingVertical: 5 }, tabs: { flexDirection: 'row', borderTopWidth: 1, borderTopColor: '#344651' }, tab: { flex: 1, alignItems: 'center', paddingVertical: 16 }, activeTab: { borderTopWidth: 3, borderTopColor: '#67d6c5' }, tabText: { color: '#a9bac5', fontSize: 13, fontWeight: '600' }, activeTabText: { color: '#f3f7fa' } });
