import Foundation
#if canImport(FoundationNetworking)
import FoundationNetworking
#endif

public actor St3Client {
    public let baseURL: URL
    private let session: URLSession
    private var credential: String?

    public init(fabricLoopbackURL: URL, credential: String? = nil, session: URLSession = .shared) {
        self.baseURL = fabricLoopbackURL
        self.credential = credential
        self.session = session
    }

    public func setCredential(_ credential: String?) { self.credential = credential }
    public func capabilities() async throws -> Envelope<Capabilities> { try await get("v1/client/capabilities") }
    public func list(_ collection: String, cursor: String? = nil, limit: Int? = nil, history: Bool = false) async throws -> Envelope<ResourcePage> {
        var query: [URLQueryItem] = []
        if let cursor { query.append(.init(name: "cursor", value: cursor)) }
        if let limit { query.append(.init(name: "limit", value: String(limit))) }
        if history { query.append(.init(name: "history", value: "true")) }
        return try await get("v1/client/\(collection)", query: query)
    }
    public func resource(_ collection: String, id: String) async throws -> Envelope<Resource> { let routedID = collection == "launches" ? id.replacingOccurrences(of: "launch/", with: "") : id; return try await get("v1/client/\(collection)/\(routedID)") }
    public func timeline(sessionID: String, limit: Int? = nil) async throws -> Envelope<TimelinePage> { try await get("v1/client/sessions/\(sessionID)/timeline", query: limit.map { [.init(name: "limit", value: String($0))] } ?? []) }
    public func events(after: String? = nil, limit: Int? = nil, waitMS: UInt64? = nil) async throws -> Envelope<EventPage> {
        var query: [URLQueryItem] = []
        if let after { query.append(.init(name: "after", value: after)) }; if let limit { query.append(.init(name: "limit", value: String(limit))) }; if let waitMS { query.append(.init(name: "wait_ms", value: String(waitMS))) }
        return try await get("v1/client/events", query: query)
    }
    public func submit(_ action: ActionRequest) async throws -> Envelope<ActionResult> { try await post("v1/client/actions", action) }
    public func beginPairing(deviceName: String) async throws -> Envelope<PairingChallenge> { try await post("v1/client/pairings", PairingBegin(apiVersion: st3ClientAPIVersion, deviceName: deviceName)) }
    public func completePairing(pairingID: String, code: String, devicePublicKey: String) async throws -> Envelope<PairedSession> { try await post("v1/client/pairings/\(pairingID.replacingOccurrences(of: "pairing/", with: ""))/complete", PairingComplete(apiVersion: st3ClientAPIVersion, code: code, devicePublicKey: devicePublicKey)) }
    public func terminalScreen(_ id: String) async throws -> Envelope<TerminalScreen> { try await get("v1/client/terminals/\(id.replacingOccurrences(of: "terminal/", with: ""))/screen") }
    public func terminalFrames(_ id: String, after: UInt64? = nil, incarnation: String? = nil) async throws -> Envelope<TerminalFramePage> {
        var query: [URLQueryItem] = []; if let after { query.append(.init(name: "after", value: String(after))) }; if let incarnation { query.append(.init(name: "incarnation", value: incarnation)) }
        return try await get("v1/client/terminals/\(id.replacingOccurrences(of: "terminal/", with: ""))/stream", query: query)
    }

    private func get<T: Decodable & Sendable>(_ path: String, query: [URLQueryItem] = []) async throws -> T { try await request(path, query: query, method: "GET", body: Optional<Data>.none) }
    private func post<T: Decodable & Sendable, Body: Encodable>(_ path: String, _ body: Body) async throws -> T { try await request(path, query: [], method: "POST", body: try JSONEncoder().encode(body)) }
    private func request<T: Decodable & Sendable>(_ path: String, query: [URLQueryItem], method: String, body: Data?) async throws -> T {
        var components = URLComponents(url: baseURL.appending(path: path), resolvingAgainstBaseURL: false)!; if !query.isEmpty { components.queryItems = query }
        var request = URLRequest(url: components.url!); request.httpMethod = method; request.httpBody = body; request.setValue("application/json", forHTTPHeaderField: "Accept")
        if body != nil { request.setValue("application/json", forHTTPHeaderField: "Content-Type") }; if let credential { request.setValue("Bearer \(credential)", forHTTPHeaderField: "Authorization") }
        let (data, response) = try await session.data(for: request); let status = (response as? HTTPURLResponse)?.statusCode ?? 0
        if !(200..<300).contains(status) { throw try JSONDecoder().decode(ErrorEnvelope.self, from: data) }
        return try JSONDecoder().decode(T.self, from: data)
    }
}
