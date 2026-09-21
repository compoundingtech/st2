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

    func testAuthorityErrorCodesAreTypedAndRoundTrip() throws {
        for (raw, expected) in [
            ("runtime-not-local", ErrorCode.runtimeNotLocal),
            ("runtime-authority-indeterminate", ErrorCode.runtimeAuthorityIndeterminate),
        ] {
            let decoded = try JSONDecoder().decode(ErrorCode.self, from: Data("\"\(raw)\"".utf8))
            XCTAssertEqual(decoded, expected)
            XCTAssertEqual(String(decoding: try JSONEncoder().encode(decoded), as: UTF8.self), "\"\(raw)\"")
        }
    }

    func testDiscriminatedResourceFixturePreservesTypedDetailsAndVisualization() throws {
        var root = URL(fileURLWithPath: #filePath)
        for _ in 0..<6 { root.deleteLastPathComponent() }
        let data = try Data(contentsOf: root.appendingPathComponent("docs/st3/client-v0/fixtures/resources.json"))
        let resources = try JSONDecoder().decode([Resource].self, from: data)
        XCTAssertEqual(resources.count, 13)
        guard case .attention(let attention) = resources[0] else { return XCTFail("attention discriminator lost") }
        XCTAssertEqual(attention.priority, "high")
        XCTAssertEqual(attention.actions, ["attention.resolve"])
        guard case .launch(let launch) = resources[2] else { return XCTFail("launch discriminator lost") }
        XCTAssertEqual(launch.variants, ["launch-variant/release/default"])
        XCTAssertEqual(launch.visualization?.version, "st3.visualization.v0")
        XCTAssertEqual(launch.visualization?.nodes.first?.goals, ["Build artifacts"])
        XCTAssertEqual(launch.visualization?.decisions.first?.decisionType, .singleChoice)
        XCTAssertEqual(launch.visualization?.diffs.first?.changes.count, 1)
        XCTAssertEqual(launch.visualization?.swimlanes.first?.nodes, ["step/build"])
        guard case .mission(let mission) = resources[6] else { return XCTFail("mission discriminator lost") }
        XCTAssertEqual(mission.runGenerations["mission-run/release/1"], "run-generation/release/1/g1")
        XCTAssertEqual(mission.visualization?.mission, "mission/release")
        guard case .work(let work) = resources[7] else { return XCTFail("work discriminator lost") }
        XCTAssertEqual(work.readinessEpoch, 3)
        XCTAssertEqual(work.goals, ["Build artifacts"])
        guard case .agent(let agent) = resources[8] else { return XCTFail("agent discriminator lost") }
        XCTAssertEqual(agent.runtimeIDs, ["runtime/release-agent"])
        guard case .runtime(let runtime) = resources[9] else { return XCTFail("runtime discriminator lost") }
        XCTAssertEqual(runtime.ownerHostID, "host/host-a")
        XCTAssertEqual(runtime.incarnationID, "runtime-9:2026-09-20T11:06:30Z")
        guard case .history(let history) = resources[11] else { return XCTFail("history discriminator lost") }
        XCTAssertEqual(history.storeIndex, 1842)
        guard case .session(let session) = resources[12] else { return XCTFail("session discriminator lost") }
        XCTAssertEqual(session.timelineCursor, "timeline-cursor/release-agent/9/8")
    }
}
