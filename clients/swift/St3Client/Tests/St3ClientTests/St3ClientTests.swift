import XCTest
@testable import St3Client

final class St3ClientTests: XCTestCase {
    func testCapabilitiesFixtureDecodes() throws {
        let json = #"{"api_version":"st3.client.v0","request_id":"request/1","snapshot":{"id":"snapshot/host/1/a","host_id":"host/a","store_index":1,"projection_version":"client-projection.v0","created_at":"2026-09-20T12:00:00Z"},"value":{"kind":"capabilities","session_actor":"person/a/session/b","transport":"fabric-loopback","capabilities":[],"limits":{"max_page_items":200,"max_event_items":500,"max_response_bytes":1048576,"max_wait_ms":30000},"event_cursor":"event-cursor/a/1","oldest_event_cursor":"event-cursor/a/0","schemas":["schema.json"]}}"#
        let value = try JSONDecoder().decode(Envelope<Capabilities>.self, from: Data(json.utf8))
        XCTAssertEqual(value.value.limits.maxPageItems, 200)
    }

    func testGeneratedActionCoverage() {
        XCTAssertEqual(ActionType.allCases.count, 30)
        XCTAssertEqual(ReadOperation.allCases.count, 27)
    }
}
