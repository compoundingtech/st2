//! OpenTelemetry export for st2, per `docs/vrs/06-observability/`.
//!
//! Zero-overhead no-op unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set; the exporters then ship
//! OTLP/HTTP JSON to the fleet's local Alloy forwarder (normally `127.0.0.1:4318`). The process
//! model is sync (no tokio), so the blocking reqwest client is mandatory — enabling both
//! reqwest client features of `opentelemetry-otlp` 0.30 compiles but fails at runtime with
//! `NoHttpClient` (all client cfg arms exclude each other).
//!
//! Spans and logs share one `tracing` subscriber (interview decision Q6): a human-readable
//! stderr fmt layer runs unconditionally so diagnostics stay visible, and — behind the endpoint
//! guard — a `tracing-opentelemetry` layer exports spans while `opentelemetry-appender-tracing`
//! exports log records via an SDK logger provider. Logs emitted inside a span automatically
//! carry its trace/span ids.

use std::sync::atomic::{AtomicBool, Ordering};

use opentelemetry::trace::{Span as _, Tracer as _};
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::{Aggregation, Instrument, SdkMeterProvider, Stream};
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::layer::{Layer as _, SubscriberExt};

static ENABLED: AtomicBool = AtomicBool::new(false);

/// Process-wide export gate. Spans and other instrumented work check this before allocating
/// anything, so the unset-endpoint path stays allocation-free (the provider guard alone cannot
/// remove no-op span construction on the supervisor hot path).
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Seconds-scale explicit bucket boundaries for the duration histograms. The SDK's default
/// boundaries are millisecond-tuned (`[0, 5, 10, …, 10000]`), so sub-second reconcile passes
/// and session spawns would collapse into the lowest buckets and be indistinguishable.
const DURATION_BUCKET_BOUNDARIES: [f64; 12] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// View mapping each duration instrument onto seconds-scale explicit buckets (see
/// [`DURATION_BUCKET_BOUNDARIES`]); every other instrument keeps its default aggregation.
fn duration_view(instrument: &Instrument) -> Option<Stream> {
    match instrument.name() {
        "reconcile_pass_duration_seconds" | "session_start_duration_seconds" => Stream::builder()
            .with_aggregation(Aggregation::ExplicitBucketHistogram {
                boundaries: DURATION_BUCKET_BOUNDARIES.into(),
                record_min_max: true,
            })
            .build()
            .ok(),
        _ => None,
    }
}

/// Guard holding the tracer, meter, and logger providers for a process lifetime. Dropping it
/// flushes and shuts all three exporters down so short-lived CLI invocations still deliver
/// their spans, metric points, and log records.
pub struct Telemetry {
    tracer_provider: Option<SdkTracerProvider>,
    meter_provider: Option<SdkMeterProvider>,
    logger_provider: Option<SdkLoggerProvider>,
}

impl Telemetry {
    /// Initialize telemetry for one process unit (`supervisor`, `cli`, ...). The service name
    /// follows the central observability contract's process-unit boundary: `st2-<unit>`.
    pub fn init(unit: &str) -> Self {
        // The stderr fmt layer is installed unconditionally — with or without an endpoint (see
        // `install_subscriber`). Migrated `tracing` diagnostics must stay visible exactly when
        // the old `eprintln!` calls were, so the unset-endpoint case keeps human-visible output
        // rather than PR1's literal zero-output behavior (documented deviation, spec.md "Log
        // bridge"). Level filtering defaults to INFO; `RUST_LOG` overrides it — on the stderr
        // fmt layer ONLY (see `install_subscriber`), so stderr verbosity can never silence
        // OTel span/log export (export-side sampling is a separate decision).
        let filter = tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));

        if std::env::var_os("OTEL_EXPORTER_OTLP_ENDPOINT").is_none() {
            install_subscriber(
                tracing_subscriber::registry(),
                None,
                None,
                filter,
            );
            return Self {
                tracer_provider: None,
                meter_provider: None,
                logger_provider: None,
            };
        }

        let span_exporter = match build_span_exporter() {
            Ok(exporter) => exporter,
            // Export setup must never take the runner down: telemetry is best-effort.
            Err(err) => {
                eprintln!("st2: otel exporter unavailable, continuing without telemetry: {err}");
                install_subscriber(
                    tracing_subscriber::registry(),
                    None,
                    None,
                    filter,
                );
                return Self {
                    tracer_provider: None,
                    meter_provider: None,
                    logger_provider: None,
                };
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

        let tracer_provider = SdkTracerProvider::builder()
            .with_batch_exporter(span_exporter)
            .with_resource(resource.clone())
            .build();
        opentelemetry::global::set_tracer_provider(tracer_provider.clone());
        let otel_span_layer = OpenTelemetryLayer::new(tracer_provider.tracer("st2"));

        // Metrics share endpoint, protocol, and resource with traces. Metric setup is
        // best-effort: if its exporter fails to build (e.g. malformed
        // `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`), metrics are disabled but span and log
        // export must continue. The periodic reader's default interval only governs
        // background collection — `shutdown` below force-flushes, so short-lived CLI runs
        // still deliver their points.
        let meter_provider = match build_metric_exporter() {
            Ok(exporter) => {
                let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter).build();
                let meter_provider = SdkMeterProvider::builder()
                    .with_reader(reader)
                    .with_view(duration_view)
                    .with_resource(resource.clone())
                    .build();
                opentelemetry::global::set_meter_provider(meter_provider.clone());
                crate::metrics::set_enabled(true);
                ENABLED.store(true, Ordering::Relaxed);
                Some(meter_provider)
            }
            Err(err) => {
                eprintln!("st2: otel metric exporter unavailable, metrics disabled: {err}");
                None
            }
        };


        // Log records share endpoint, protocol, and resource too; the appender bridge maps
        // `tracing` events onto OTLP logs and stamps the current span context onto them.
        let logger_provider = match build_log_exporter() {
            Ok(exporter) => Some(
                SdkLoggerProvider::builder()
                    .with_batch_exporter(exporter)
                    .with_resource(resource)
                    .build(),
            ),
            Err(err) => {
                eprintln!("st2: otel log exporter unavailable, logs disabled: {err}");
                None
            }
        };

        install_subscriber(
            tracing_subscriber::registry(),
            Some(otel_span_layer),
            logger_provider.as_ref(),
            filter,
        );

        Self {
            tracer_provider: Some(tracer_provider),
            meter_provider,
            logger_provider,
        }
    }

    /// Whether export is active (endpoint configured and exporters initialized).
    pub fn enabled(&self) -> bool {
        self.tracer_provider.is_some()
    }

    /// Flush pending spans, metric points, and log records, then stop all exporters. Safe to
    /// call multiple times.
    pub fn shutdown(&mut self) {
        if let Some(provider) = self.meter_provider.take() {
            let _ = provider.force_flush();
            let _ = provider.shutdown();
            crate::metrics::set_enabled(false);
        }
        if let Some(provider) = self.logger_provider.take() {
            let _ = provider.force_flush();
            let _ = provider.shutdown();
        }
        if let Some(provider) = self.tracer_provider.take() {
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

/// Install the global subscriber once: stderr fmt (always), span exporter layer, and log-record
/// bridge behind the endpoint guard. A second `Telemetry::init` in the same process cannot
/// replace it (`set_global_default` errors), which is fine: init runs once per entrypoint.
fn install_subscriber<S>(
    base: S,
    span_layer: Option<OpenTelemetryLayer<S, opentelemetry_sdk::trace::Tracer>>,
    logger_provider: Option<&SdkLoggerProvider>,
    filter: tracing_subscriber::EnvFilter,
) where
    S: tracing::Subscriber + Send + Sync + 'static,
    for<'a> S: tracing_subscriber::registry::LookupSpan<'a>,
{
    // The stderr layer's `EnvFilter` is scoped to that layer only: `RUST_LOG` is a
    // stderr-noise knob and can never silently disable OTLP export.
    //
    // The OpenTelemetry layers carry their own static filter, likewise independent of
    // `RUST_LOG`, that silences ONLY the exporters' own instrumentation targets. Those
    // cannot pass through the layers they feed: `opentelemetry_sdk`'s BatchLogProcessor
    // emits tracing events from inside its own `emit` (channel-full/shutdown notices), so
    // an event arriving via the log bridge would re-enter the same processor and recurse
    // until the export thread overflows its stack. App signals are exported unfiltered;
    // export-side sampling remains a separate decision.
    let otel_filter = tracing_subscriber::EnvFilter::new(
        "trace,opentelemetry=off,opentelemetry_sdk=off,opentelemetry_http=off,opentelemetry_otlp=off",
    );
    let _ = tracing::subscriber::set_global_default(
        base.with(span_layer.with_filter(otel_filter.clone()))
            .with(
                logger_provider
                    .map(OpenTelemetryTracingBridge::new)
                    .with_filter(otel_filter),
            )
            .with(
                tracing_subscriber::fmt::layer()
                    .with_writer(std::io::stderr)
                    .with_filter(filter),
            ),
    );
}

/// OTLP/HTTP-JSON span exporter; blocking reqwest client only (module docs).
fn build_span_exporter() -> Result<SpanExporter, opentelemetry_otlp::ExporterBuildError> {
    SpanExporter::builder()
        .with_http()
        .with_protocol(opentelemetry_otlp::Protocol::HttpJson)
        .build()
}

/// OTLP/HTTP-JSON metric exporter; same wire and client constraints as traces.
fn build_metric_exporter() -> Result<MetricExporter, opentelemetry_otlp::ExporterBuildError> {
    MetricExporter::builder()
        .with_http()
        .with_protocol(opentelemetry_otlp::Protocol::HttpJson)
        .build()
}

/// OTLP/HTTP-JSON log exporter; same wire and client constraints as traces/metrics.
fn build_log_exporter() -> Result<LogExporter, opentelemetry_otlp::ExporterBuildError> {
    LogExporter::builder()
        .with_http()
        .with_protocol(opentelemetry_otlp::Protocol::HttpJson)
        .build()
}
