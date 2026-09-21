// Generated-model companion for st3.client.v0. Contract enum/path data is in Contract.generated.swift.
import Foundation

public let st3ClientAPIVersion = "st3.client.v0"
public let st3ClientTerminalSubprotocol = "st3.client.terminal.v0"

public struct Envelope<Value: Codable & Sendable>: Codable, Sendable {
    public let apiVersion: String
    public let requestID: String
    public let snapshot: Snapshot
    public let value: Value
    enum CodingKeys: String, CodingKey { case apiVersion = "api_version", requestID = "request_id", snapshot, value }
}

public struct Snapshot: Codable, Sendable, Equatable {
    public let id: String
    public let hostID: String
    public let storeIndex: UInt64
    public let projectionVersion: String
    public let createdAt: String
    enum CodingKeys: String, CodingKey { case id, hostID = "host_id", storeIndex = "store_index", projectionVersion = "projection_version", createdAt = "created_at" }
}

public struct ErrorEnvelope: Codable, Error, Sendable {
    public let apiVersion: String
    public let errorVersion: String
    public let requestID: String
    public let code: String
    public let message: String
    public let retryable: Bool
    public let retryAfterMS: UInt64?
    public let details: [String: JSONValue]
    enum CodingKeys: String, CodingKey { case apiVersion = "api_version", errorVersion = "error_version", requestID = "request_id", code, message, retryable, retryAfterMS = "retry_after_ms", details }
}

public enum JSONValue: Codable, Sendable, Equatable {
    case null, bool(Bool), number(Double), string(String), array([JSONValue]), object([String: JSONValue])
    public init(from decoder: Decoder) throws {
        let box = try decoder.singleValueContainer()
        if box.decodeNil() { self = .null }
        else if let value = try? box.decode(Bool.self) { self = .bool(value) }
        else if let value = try? box.decode(Double.self) { self = .number(value) }
        else if let value = try? box.decode(String.self) { self = .string(value) }
        else if let value = try? box.decode([JSONValue].self) { self = .array(value) }
        else { self = .object(try box.decode([String: JSONValue].self)) }
    }
    public func encode(to encoder: Encoder) throws {
        var box = encoder.singleValueContainer()
        switch self { case .null: try box.encodeNil(); case .bool(let v): try box.encode(v); case .number(let v): try box.encode(v); case .string(let v): try box.encode(v); case .array(let v): try box.encode(v); case .object(let v): try box.encode(v) }
    }
}

public struct Capability: Codable, Sendable { public let id: String; public let version: UInt; public let state: String }
public struct Limits: Codable, Sendable {
    public let maxPageItems: Int; public let maxEventItems: Int; public let maxResponseBytes: Int; public let maxWaitMS: UInt64
    enum CodingKeys: String, CodingKey { case maxPageItems = "max_page_items", maxEventItems = "max_event_items", maxResponseBytes = "max_response_bytes", maxWaitMS = "max_wait_ms" }
}
public struct Capabilities: Codable, Sendable {
    public let kind: String; public let sessionActor: String; public let transport: String; public let capabilities: [Capability]; public let limits: Limits; public let eventCursor: String; public let oldestEventCursor: String; public let schemas: [String]
    enum CodingKeys: String, CodingKey { case kind, sessionActor = "session_actor", transport, capabilities, limits, eventCursor = "event_cursor", oldestEventCursor = "oldest_event_cursor", schemas }
}

public struct PageInfo: Codable, Sendable { public let limit: Int; public let hasMore: Bool; public let nextCursor: String?; public let cursorExpiresAt: String?; enum CodingKeys: String, CodingKey { case limit, hasMore = "has_more", nextCursor = "next_cursor", cursorExpiresAt = "cursor_expires_at" } }
public struct Operational: Codable, Sendable { public let layer: String; public let actionable: Bool; public let reasons: [String]?; public let ownerGeneration: String?; public let runtimeIncarnation: String?; enum CodingKeys: String, CodingKey { case layer, actionable, reasons, ownerGeneration = "owner_generation", runtimeIncarnation = "runtime_incarnation" } }
public struct Resource: Codable, Sendable, Identifiable {
    public let id: String; public let kind: String; public let revision: String; public let updatedAt: String
    public let operational: Operational?; public let title: String?; public let detail: String?; public let state: String?; public let name: String?; public let path: String?; public let ownerID: String?; public let runtimeID: String?; public let runtimeIncarnation: String?
    enum CodingKeys: String, CodingKey { case id, kind, revision, updatedAt = "updated_at", operational, title, detail, state, name, path, ownerID = "owner_id", runtimeID = "runtime_id", runtimeIncarnation = "runtime_incarnation" }
}
public struct ResourcePage: Codable, Sendable { public let kind: String; public let collection: String; public let items: [Resource]; public let page: PageInfo }

public struct TimelineEntry: Codable, Sendable, Identifiable { public let id: String; public let sequence: UInt64; public let revision: UInt; public let timestamp: String; public let role: String; public let type: String; public let isFinal: Bool; public let body: [String: JSONValue]; enum CodingKeys: String, CodingKey { case id, sequence, revision, timestamp, role, type, isFinal = "final", body } }
public struct TimelinePage: Codable, Sendable { public let kind: String; public let sessionID: String; public let items: [TimelineEntry]; public let page: PageInfo; enum CodingKeys: String, CodingKey { case kind, sessionID = "session_id", items, page } }
public struct ProjectionEvent: Codable, Sendable, Identifiable { public let id: String; public let epoch: String; public let sequence: UInt64; public let previousCursor: String; public let nextCursor: String; public let timestamp: String; public let type: String; public let resourceIDs: [String]; public let snapshotID: String; public let body: [String: JSONValue]; enum CodingKeys: String, CodingKey { case id, epoch, sequence, previousCursor = "previous_cursor", nextCursor = "next_cursor", timestamp, type, resourceIDs = "resource_ids", snapshotID = "snapshot_id", body } }
public struct EventPage: Codable, Sendable { public let kind: String; public let oldestCursor: String; public let resumeCursor: String; public let items: [ProjectionEvent]; public let hasMore: Bool; enum CodingKeys: String, CodingKey { case kind, oldestCursor = "oldest_cursor", resumeCursor = "resume_cursor", items, hasMore = "has_more" } }

public struct Fence: Codable, Sendable {
    public var snapshotID: String; public var subjectRevisions: [String: String]; public var missionGeneration: String?; public var stepDefinition: String?; public var attempt: UInt?; public var readinessEpoch: UInt64?; public var runtimeIncarnation: String?; public var terminalSequence: UInt64?; public var previewToken: String?
    public init(snapshotID: String, subjectRevisions: [String: String] = [:]) { self.snapshotID = snapshotID; self.subjectRevisions = subjectRevisions }
    enum CodingKeys: String, CodingKey { case snapshotID = "snapshot_id", subjectRevisions = "subject_revisions", missionGeneration = "mission_generation", stepDefinition = "step_definition", attempt, readinessEpoch = "readiness_epoch", runtimeIncarnation = "runtime_incarnation", terminalSequence = "terminal_sequence", previewToken = "preview_token" }
}
public struct ActionRequest: Codable, Sendable {
    public let apiVersion: String; public let id: String; public let type: ActionType; public let idempotencyKey: String; public let fence: Fence; public let parameters: [String: JSONValue]
    public init(id: String, type: ActionType, idempotencyKey: String, fence: Fence, parameters: [String: JSONValue]) { self.apiVersion = st3ClientAPIVersion; self.id = id; self.type = type; self.idempotencyKey = idempotencyKey; self.fence = fence; self.parameters = parameters }
    public init<Parameters: Encodable>(id: String, type: ActionType, idempotencyKey: String, fence: Fence, typedParameters: Parameters) throws {
        let encoded = try JSONEncoder().encode(typedParameters)
        guard case .object(let parameters) = try JSONDecoder().decode(JSONValue.self, from: encoded) else { throw EncodingError.invalidValue(typedParameters, .init(codingPath: [], debugDescription: "Action parameters must encode as an object")) }
        self.init(id: id, type: type, idempotencyKey: idempotencyKey, fence: fence, parameters: parameters)
    }
    enum CodingKeys: String, CodingKey { case apiVersion = "api_version", id, type, idempotencyKey = "idempotency_key", fence, parameters }
}

public struct TargetParameters: Codable, Sendable { public var targetID: String; public var reason: String?; public var summary: String?; public var evidence: [String]; public init(targetID: String, reason: String? = nil, summary: String? = nil, evidence: [String] = []) { self.targetID = targetID; self.reason = reason; self.summary = summary; self.evidence = evidence }; enum CodingKeys: String, CodingKey { case targetID = "target_id", reason, summary, evidence } }
public struct AttentionResolveParameters: Codable, Sendable { public var attentionID: String; public var outcome: String; public var reason: String?; public init(attentionID: String, outcome: String, reason: String? = nil) { self.attentionID = attentionID; self.outcome = outcome; self.reason = reason }; enum CodingKeys: String, CodingKey { case attentionID = "attention_id", outcome, reason } }
public struct MessageSendParameters: Codable, Sendable { public var to: String; public var content: String; public var title: String?; public var inReplyTo: String?; public var tags: [String]; public init(to: String, content: String, title: String? = nil, inReplyTo: String? = nil, tags: [String] = []) { self.to = to; self.content = content; self.title = title; self.inReplyTo = inReplyTo; self.tags = tags }; enum CodingKeys: String, CodingKey { case to, content, title, inReplyTo = "in_reply_to", tags } }
public struct LaunchTarget: Codable, Sendable {
    public enum Kind: String, Codable, Sendable { case newMission = "new-mission", missionRun = "mission-run" }
    public let kind: Kind
    public let missionID: String?
    public let workspace: String?
    public let missionRunID: String?
    public let generationID: String?
    private init(kind: Kind, missionID: String? = nil, workspace: String? = nil, missionRunID: String? = nil, generationID: String? = nil) { self.kind = kind; self.missionID = missionID; self.workspace = workspace; self.missionRunID = missionRunID; self.generationID = generationID }
    public static func newMission(_ missionID: String, workspace: String) -> Self { .init(kind: .newMission, missionID: missionID, workspace: workspace) }
    public static func missionRun(_ missionRunID: String, generationID: String) -> Self { .init(kind: .missionRun, missionRunID: missionRunID, generationID: generationID) }
    enum CodingKeys: String, CodingKey { case kind = "type", missionID = "mission_id", workspace, missionRunID = "mission_run_id", generationID = "generation_id" }
}
public struct LaunchCreateParameters: Codable, Sendable { public var title: String; public var request: String; public var target: LaunchTarget; public init(title: String, request: String, target: LaunchTarget) { self.title = title; self.request = request; self.target = target } }
public struct LaunchReviseParameters: Codable, Sendable { public var launchID: String; public var feedback: String; public init(launchID: String, feedback: String) { self.launchID = launchID; self.feedback = feedback }; enum CodingKeys: String, CodingKey { case launchID = "launch_id", feedback } }
public struct LaunchVariantParameters: Codable, Sendable { public var launchID: String; public var variantID: String; public init(launchID: String, variantID: String) { self.launchID = launchID; self.variantID = variantID }; enum CodingKeys: String, CodingKey { case launchID = "launch_id", variantID = "variant_id" } }
public struct MissionStartParameters: Codable, Sendable { public var missionID: String; public var workspace: String; public var inputs: [String: String]; public init(missionID: String, workspace: String, inputs: [String: String] = [:]) { self.missionID = missionID; self.workspace = workspace; self.inputs = inputs }; enum CodingKeys: String, CodingKey { case missionID = "mission_id", workspace, inputs } }
public struct MissionReviseParameters: Codable, Sendable { public var missionRunID: String; public var launchID: String; public init(missionRunID: String, launchID: String) { self.missionRunID = missionRunID; self.launchID = launchID }; enum CodingKeys: String, CodingKey { case missionRunID = "mission_run_id", launchID = "launch_id" } }
public struct WorkPublishMissionParameters: Codable, Sendable { public var targetID: String; public var name: String; public var mission: [String: JSONValue]; public init(targetID: String, name: String, mission: [String: JSONValue]) { self.targetID = targetID; self.name = name; self.mission = mission }; enum CodingKeys: String, CodingKey { case targetID = "target_id", name, mission } }
public struct RuntimeSignalParameters: Codable, Sendable { public var targetID: String; public var signal: String; public init(targetID: String, signal: String) { self.targetID = targetID; self.signal = signal }; enum CodingKeys: String, CodingKey { case targetID = "target_id", signal } }
public enum TerminalInputMode: String, Codable, Sendable { case line, raw, key }
public struct TerminalInputParameters: Codable, Sendable { public var terminalID: String; public var mode: TerminalInputMode; public var value: String; public init(terminalID: String, mode: TerminalInputMode, value: String) { self.terminalID = terminalID; self.mode = mode; self.value = value }; enum CodingKeys: String, CodingKey { case terminalID = "terminal_id", mode, value } }
public struct TerminalResizeParameters: Codable, Sendable { public var terminalID: String; public var rows: UInt16; public var columns: UInt16; public init(terminalID: String, rows: UInt16, columns: UInt16) { self.terminalID = terminalID; self.rows = rows; self.columns = columns }; enum CodingKeys: String, CodingKey { case terminalID = "terminal_id", rows, columns } }
public struct ActionResult: Codable, Sendable { public let kind: String; public let actionID: String; public let operationID: String; public let status: String; public let affectedIDs: [String]; public let snapshotID: String; enum CodingKeys: String, CodingKey { case kind, actionID = "action_id", operationID = "operation_id", status, affectedIDs = "affected_ids", snapshotID = "snapshot_id" } }

public struct PairingBegin: Codable, Sendable { public let apiVersion: String; public let deviceName: String; enum CodingKeys: String, CodingKey { case apiVersion = "api_version", deviceName = "device_name" } }
public struct PairingChallenge: Codable, Sendable { public let kind: String; public let pairingID: String; public let code: String; public let expiresAt: String; enum CodingKeys: String, CodingKey { case kind, pairingID = "pairing_id", code, expiresAt = "expires_at" } }
public struct PairingComplete: Codable, Sendable { public let apiVersion: String; public let code: String; public let devicePublicKey: String; enum CodingKeys: String, CodingKey { case apiVersion = "api_version", code, devicePublicKey = "device_public_key" } }
public struct PairedSession: Codable, Sendable { public let kind: String; public let deviceID: String; public let sessionActor: String; public let credential: String; public let scopes: [String]; public let expiresAt: String; enum CodingKeys: String, CodingKey { case kind, deviceID = "device_id", sessionActor = "session_actor", credential, scopes, expiresAt = "expires_at" } }

public struct TerminalLine: Codable, Sendable { public let row: Int; public let text: String; public let redacted: Bool; public let truncated: Bool }
public struct TerminalCursor: Codable, Sendable { public let row: Int; public let column: Int; public let visible: Bool }
public struct TerminalScreen: Codable, Sendable { public let kind: String; public let terminalID: String; public let runtimeIncarnation: String; public let rows: Int; public let columns: Int; public let cursor: TerminalCursor; public let title: String; public let lines: [TerminalLine]; public let nextSequence: UInt64; public let truncated: Bool; enum CodingKeys: String, CodingKey { case kind, terminalID = "terminal_id", runtimeIncarnation = "runtime_incarnation", rows, columns, cursor, title, lines, nextSequence = "next_sequence", truncated } }
public struct TerminalFrame: Codable, Sendable, Identifiable { public let id: String; public let terminalID: String; public let runtimeIncarnation: String; public let sequence: UInt64; public let type: String; public let timestamp: String; public let body: [String: JSONValue]; enum CodingKeys: String, CodingKey { case id, terminalID = "terminal_id", runtimeIncarnation = "runtime_incarnation", sequence, type, timestamp, body } }
public struct TerminalFramePage: Codable, Sendable { public let kind: String; public let terminalID: String; public let runtimeIncarnation: String; public let frames: [TerminalFrame]; public let resumeSequence: UInt64; enum CodingKeys: String, CodingKey { case kind, terminalID = "terminal_id", runtimeIncarnation = "runtime_incarnation", frames, resumeSequence = "resume_sequence" } }
