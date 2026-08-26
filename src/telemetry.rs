//! OpenTelemetry export for st2, per `docs/vrs/06-observability/`.
//!
//! Zero-overhead no-op unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set; the exporter then ships
//! OTLP/HTTP JSON to the fleet's local Alloy forwarder (normally `127.0.0.1:4318`). The process
//! model is sync (no tokio), so the blocking reqwest client is mandatory — enabling both
//! reqwest client features of `opentelemetry-otlp` 0.30 compiles but fails at runtime with
//! `NoHttpClient` (all client cfg arms exclude each other).

use std::sync::atomic::{AtomicBool, Ordering};

use opentelemetry::trace::{Span as _, Tracer as _};
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;

static ENABLED: AtomicBool = AtomicBool::new(false);

/// Process-wide export gate. Spans and other instrumented work check this before allocating
/// anything, so the unset-endpoint path stays allocation-free (the provider guard alone cannot
/// remove no-op span construction on the supervisor hot path).
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

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
        ENABLED.store(true, Ordering::Relaxed);
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
        ENABLED.store(false, Ordering::Relaxed);
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// A root `st2.reconcile_pass` span for one bounded pass, or nothing when telemetry is
/// disabled (construction is skipped entirely — see [`enabled`]). Each pass gets its own
/// trace; the supervisor loop never holds an endless root open.
pub struct PassSpan(Option<opentelemetry::global::BoxedSpan>);

impl PassSpan {
    pub fn start(this_host: &str) -> Self {
        if !enabled() {
            return Self(None);
        }
        let tracer = opentelemetry::global::tracer("st2");
        let span = tracer
            .span_builder("st2.reconcile_pass")
            .with_attributes(vec![KeyValue::new("st2.host", this_host.to_string())])
            .start(&tracer);
        Self(Some(span))
    }

    /// Record pass outcomes and end the span. Early-drop paths end it without attributes.
    pub fn finish(mut self, crash_loops: usize, unparked: usize) {
        if let Some(span) = self.0.as_mut() {
            let to_i64 = |n: usize| i64::try_from(n).unwrap_or(i64::MAX);
            span.set_attribute(KeyValue::new("st2.crash_loops", to_i64(crash_loops)));
            span.set_attribute(KeyValue::new("st2.unparked", to_i64(unparked)));
        }
        self.end();
    }
}

impl Drop for PassSpan {
    fn drop(&mut self) {
        self.end();
    }
}

impl PassSpan {
    fn end(&mut self) {
        if let Some(mut span) = self.0.take() {
            span.end();
        }
    }
}
