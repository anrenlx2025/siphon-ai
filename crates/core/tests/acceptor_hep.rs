//! HEP lifecycle chunks (#604): drive a call from `prepare_call` →
//! `run_call` with HEP telemetry teed into an in-memory sink, and confirm
//! the call ships a `call_started` and a `call_ended` Log (0x64) chunk
//! keyed by the SIP Call-ID — the key Homer's call view threads on.

use std::sync::Arc;
use std::time::Duration;

use forge_engine::{MediaBridgeManager, SessionManager, SessionManagerConfig};
use forge_rtp::PortPoolConfig;
use hep_rs::{HepPacket, HepProtocol, HepSink};
use parking_lot::Mutex;
use siphon_ai_bridge::CallId as BridgeCallId;
use siphon_ai_core::{BridgeDefaults, BridgingAcceptor, CallRegistry};
use siphon_ai_media_glue::MediaSetup;
use siphon_ai_sip_glue::InviteFacts;
use siphon_ai_telemetry::{HepTelemetry, HepTelemetryBuild, SinkHandle};

mod common;
use common::{invite, one_route, server_acks_start_then_idles, LINPHONE_PCMU_OFFER};

/// Records every packet handed to the sink.
#[derive(Default)]
struct Capture {
    seen: Mutex<Vec<HepPacket>>,
}

impl HepSink for Capture {
    fn send(&self, packet: HepPacket) {
        self.seen.lock().push(packet);
    }
}

#[tokio::test]
async fn bridged_call_ships_start_and_end_log_chunks_by_sip_call_id() {
    let (port_tx, port_rx) = tokio::sync::oneshot::channel();
    tokio::spawn(server_acks_start_then_idles(port_tx));
    let port = port_rx.await.unwrap();
    let ws_url = format!("ws://127.0.0.1:{port}/");

    let session_mgr = SessionManager::new(
        SessionManagerConfig {
            port_pool_config: PortPoolConfig::new(60700, 60800).unwrap(),
            ..Default::default()
        },
        None,
    );
    let media = Arc::new(MediaSetup::new(
        Arc::clone(&session_mgr),
        Arc::new(MediaBridgeManager::new()),
        Arc::new(forge_core::EventBus::new()),
        "192.168.1.10",
    ));
    let registry = CallRegistry::new();

    // No collector: the capture leg is the whole sink, so nothing opens a
    // socket and every packet the daemon composes lands here.
    let capture = Arc::new(Capture::default());
    let (hep, worker) = HepTelemetry::build(HepTelemetryBuild {
        collector: None,
        capture_id: 2001,
        capture_password: None,
        queue_capacity: 64,
        node_id: "hep-node".into(),
        extra_sinks: vec![Arc::clone(&capture) as SinkHandle],
    })
    .await
    .expect("HEP telemetry builds");
    assert!(worker.is_none());

    let acceptor = BridgingAcceptor::new(media, BridgeDefaults::default(), registry.clone())
        .with_call_id_factory(Arc::new(|| BridgeCallId::new("siphon-hep-test")))
        .with_hep_telemetry(Some(Arc::new(hep)));

    let routes = one_route("main_reception", &ws_url);
    let route = routes.iter().next().unwrap();
    let req = invite(
        LINPHONE_PCMU_OFFER,
        "sip:5000@siphon.example.com",
        "abc-hep@pbx.example.com",
    );
    let facts = InviteFacts::extract(&req);
    let prepared = acceptor
        .prepare_call(&req, route, &facts, sip_transaction::TransportKind::Udp)
        .await
        .expect("prepare");

    let run_handle = acceptor.run_call(prepared, "main_reception", None);
    tokio::time::sleep(Duration::from_millis(150)).await;
    registry
        .lookup("abc-hep@pbx.example.com")
        .expect("registered")
        .shutdown();
    tokio::time::timeout(Duration::from_secs(3), run_handle)
        .await
        .expect("run_call completes")
        .expect("task does not panic");

    let seen = capture.seen.lock();
    let logs: Vec<&HepPacket> = seen
        .iter()
        .filter(|p| p.protocol == HepProtocol::Log)
        .collect();
    assert_eq!(logs.len(), 2, "one start + one end line; got {logs:?}");
    for pkt in &logs {
        assert_eq!(
            pkt.correlation_id.as_deref(),
            Some("abc-hep@pbx.example.com"),
            "lifecycle chunks must correlate by SIP Call-ID, not the bridge id"
        );
        assert_eq!(pkt.capture_id, 2001);
    }
    let start = std::str::from_utf8(&logs[0].payload).unwrap();
    assert_eq!(
        start,
        "call_started call_id=siphon-hep-test direction=inbound node=hep-node \
         route=main_reception from=+13125551234 to=5000"
    );
    let end = std::str::from_utf8(&logs[1].payload).unwrap();
    assert!(
        end.starts_with(
            "call_ended call_id=siphon-hep-test direction=inbound node=hep-node \
             cause=local_shutdown duration_ms="
        ),
        "{end}"
    );
}
