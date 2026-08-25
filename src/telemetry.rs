//! OpenTelemetry export for st2, per `docs/vrs/06-observability/`.
//!
//! Zero-overhead no-op unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set; the exporter then ships
//! OTLP/HTTP JSON to the fleet's local Alloy forwarder (normally `127.0.0.1:4318`). The process
//! model is sync (no tokio), so the blocking reqwest client is mandatory — enabling both
//! reqwest client features of `opentelemetry-otlp` 0.30 compiles but fails at runtime with
//! `NoHttpClient` (all client cfg arms exclude each other).

use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;

/// Guard holding the tracer provider for a process lifetime. Dropping it flushes and shuts the
/// exporter down so short-lived CLI invocations still deliver their spans.
pub struct Telemetry {
    provider: Option<SdkTracerProvider>,
}

impl Telemetry {
    /// Initialize telemetry for one process unit (`supervisor`, `cli`, ...). The service name
    /// follows the central observability contract's process-unit boundary: `st2-<unit>`.
    pub fn init(unit: &str) -> Self {
        if std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_none() {
            return Self { provider: None };
        }

        let exporter = match opentelemetry_otlp::SpanExporter::builder()
            .with_http()
            .with_protocol(opentelemetry_otlp::Protocol::HttpJson)
            .build()
        {
            Ok(exporter) => exporter,
            // Export setup must never take the runner down: telemetry is best-effort.
            Err(err) => {
                eprintln!("st2: otel exporter unavailable, continuing without telemetry: {err}");
                return Self { provider: None };
            }
        };

        let resource = opentelemetry_sdk::Resource::builder()
            .with_service_name(format!("st2-{unit}"))
            .with_attribute(KeyValue::new(
                "service.version",
                crate::version::machine_version(),
            ))
            .with_attribute(KeyValue::new("host.name", crate::run::detect_host()))
            .build();

        let provider = SdkTracerProvider::builder()
            .with_batch_exporter(exporter)
            .with_resource(resource)
            .build();
        opentelemetry::global::set_tracer_provider(provider.clone());
        Self {
            provider: Some(provider),
        }
    }

    /// Whether export is active (endpoint configured and exporter initialized).
    pub fn enabled(&self) -> bool {
        self.provider.is_some()
    }

    /// Flush pending spans and stop the exporter. Safe to call multiple times.
    pub fn shutdown(&mut self) {
        if let Some(provider) = self.provider.take() {
            let _ = provider.force_flush();
            let _ = provider.shutdown();
        }
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        self.shutdown();
    }
}
