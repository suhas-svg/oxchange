//! Shared metrics, tracing and logging (spec: "Observability").
//!
//! Every service calls [`init`] first thing in `main` and keeps the returned
//! [`Telemetry`] alive until it exits. That gives it:
//!
//! - **Logs**: JSON lines on stdout, each with the spans it happened in
//!   (and so their `trace_id`), plus the same records over OTLP to the
//!   collector, which stores them in Loki linked to their trace.
//! - **Traces**: `tracing` spans become OpenTelemetry spans, exported over
//!   OTLP/HTTP to the collector, which keeps errors, slow traces and a 1%
//!   sample in Tempo. Nothing is exported when no collector is configured.
//! - **Metrics**: the `metrics` macros anywhere in the process, served for
//!   Prometheus at `METRICS_ADDR/metrics`, every series labelled with the
//!   service name. Process memory and CPU are included.
//!
//! Trace context travels as a W3C `traceparent`: in HTTP and gRPC headers
//! ([`server::ServerLayer`], [`inject_metadata`]) and in Kafka headers
//! ([`current_traceparent`], [`follow`]).
//!
//! | Variable | Default | Meaning |
//! |---|---|---|
//! | `RUST_LOG` | `info` | log filter |
//! | `OTEL_EXPORTER_OTLP_ENDPOINT` | unset (no export) | e.g. `http://otel-collector:4318` |
//! | `TRACE_SAMPLE_RATIO` | `1.0` | share of new traces recorded (the collector samples again) |
//! | `METRICS_ADDR` | `0.0.0.0:9464` | Prometheus listener, `off` to disable |

use std::net::SocketAddr;
use std::time::Duration;

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use opentelemetry::propagation::{Extractor, TextMapPropagator};
use opentelemetry::trace::{TraceContextExt, TracerProvider as _};
use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

#[cfg(feature = "sqlx")]
mod pool;
pub mod push;
pub mod server;

#[cfg(feature = "sqlx")]
pub use pool::watch_pool;

/// Histogram buckets, in seconds, for every `*_seconds` metric: from the
/// matcher's microseconds to settlement retries.
pub const SECONDS_BUCKETS: &[f64] = &[
    0.000_005, 0.000_01, 0.000_025, 0.000_05, 0.000_1, 0.000_25, 0.000_5, 0.001, 0.002_5, 0.005,
    0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// What kind of process this is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// A long-running service: metrics served at `METRICS_ADDR` for
    /// Prometheus to scrape, traces and logs exported.
    Serve,
    /// A batch job: metrics only recorded (the caller pushes them with
    /// [`push`]), traces and logs exported.
    Batch,
    /// A short command (`migrate`, `health`, `keygen`): JSON logs on stdout
    /// only, so a healthcheck run every few seconds stays cheap and never
    /// competes with the service for its metrics port.
    Cli,
}

/// Keeps the exporters running; flushes them when dropped.
#[must_use = "telemetry stops when this is dropped"]
pub struct Telemetry {
    tracer: SdkTracerProvider,
    logger: Option<SdkLoggerProvider>,
    metrics: Option<PrometheusHandle>,
}

impl Telemetry {
    /// The recorded metrics in Prometheus text format.
    pub fn render_metrics(&self) -> String {
        self.metrics
            .as_ref()
            .map(|h| h.render())
            .unwrap_or_default()
    }
}

impl Drop for Telemetry {
    fn drop(&mut self) {
        let tracer = self.tracer.clone();
        let logger = self.logger.take();
        // Shutdown blocks while the last batch is sent; keep that off any
        // async runtime thread.
        let _ = std::thread::spawn(move || {
            let _ = tracer.shutdown();
            if let Some(l) = logger {
                let _ = l.shutdown();
            }
        })
        .join();
    }
}

impl Mode {
    /// [`Mode::Serve`] for a service binary run with no subcommand or
    /// `serve`, [`Mode::Cli`] for any other subcommand.
    pub fn for_command(subcommand: Option<&str>) -> Self {
        match subcommand {
            None | Some("serve") => Mode::Serve,
            Some(_) => Mode::Cli,
        }
    }
}

/// Sets up logs, traces and metrics for a service. Call once, first thing.
pub fn init(service: &'static str) -> Telemetry {
    init_with(service, Mode::Serve)
}

/// [`init`] for a given kind of process.
pub fn init_with(service: &'static str, mode: Mode) -> Telemetry {
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .filter(|v| !v.trim().is_empty() && mode != Mode::Cli);
    let resource = Resource::builder().with_service_name(service).build();

    // The OTLP/HTTP exporters use a blocking HTTP client, which must not be
    // created inside an async runtime; build them on a plain thread.
    let exporters = endpoint.as_ref().map(|_| {
        std::thread::spawn(|| {
            let spans = opentelemetry_otlp::SpanExporter::builder()
                .with_http()
                .build();
            let logs = opentelemetry_otlp::LogExporter::builder()
                .with_http()
                .build();
            (spans, logs)
        })
        .join()
        .expect("exporter thread")
    });

    let mut tracer = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_sampler(Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(
            sample_ratio(),
        ))));
    let mut logger = None;
    let mut export_error = None;
    if let Some((spans, logs)) = exporters {
        match spans {
            Ok(e) => tracer = tracer.with_batch_exporter(e),
            Err(e) => export_error = Some(format!("trace exporter: {e}")),
        }
        match logs {
            Ok(e) => {
                logger = Some(
                    SdkLoggerProvider::builder()
                        .with_resource(resource)
                        .with_batch_exporter(e)
                        .build(),
                );
            }
            Err(e) => export_error = Some(format!("log exporter: {e}")),
        }
    }
    let tracer = tracer.build();

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let stdout = tracing_subscriber::fmt::layer()
        .json()
        .with_current_span(false)
        .with_span_list(true);
    let spans = tracing_opentelemetry::layer().with_tracer(tracer.tracer(service));
    // The exporters' own HTTP client logs too; never feed those back in.
    let otlp_logs = logger.as_ref().map(|l| {
        OpenTelemetryTracingBridge::new(l).with_filter(tracing_subscriber::filter::filter_fn(|m| {
            !is_exporter_noise(m.target())
        }))
    });
    tracing_subscriber::registry()
        .with(filter)
        .with(stdout)
        .with(spans)
        .with(otlp_logs)
        .with(server::SlowQueryCounter)
        .init();

    let metrics = match mode {
        Mode::Cli => None,
        Mode::Serve | Mode::Batch => install_metrics(service, mode),
    };
    if let Some(e) = export_error {
        tracing::warn!(error = %e, "OTLP export is off");
    } else if let Some(e) = endpoint {
        tracing::info!(endpoint = %e, "exporting traces and logs over OTLP");
    }
    Telemetry {
        tracer,
        logger,
        metrics,
    }
}

fn is_exporter_noise(target: &str) -> bool {
    ["opentelemetry", "reqwest", "hyper", "h2", "tower", "tonic"]
        .iter()
        .any(|p| target.starts_with(p))
}

fn sample_ratio() -> f64 {
    std::env::var("TRACE_SAMPLE_RATIO")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .filter(|r| (0.0..=1.0).contains(r))
        .unwrap_or(1.0)
}

fn install_metrics(service: &'static str, mode: Mode) -> Option<PrometheusHandle> {
    let builder = match PrometheusBuilder::new()
        .add_global_label("service", service)
        .set_buckets(SECONDS_BUCKETS)
    {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(error = %e, "metrics are off");
            return None;
        }
    };
    let handle = match mode {
        Mode::Batch | Mode::Cli => builder.install_recorder().map_err(|e| e.to_string()),
        Mode::Serve => {
            let addr = std::env::var("METRICS_ADDR").unwrap_or_else(|_| "0.0.0.0:9464".into());
            if addr == "off" {
                return None;
            }
            match addr.parse::<SocketAddr>() {
                Ok(a) => match serve_metrics(builder.with_http_listener(a)) {
                    Ok(recorder) => {
                        let handle = recorder.handle();
                        match metrics::set_global_recorder(recorder) {
                            Ok(()) => {
                                tracing::info!(%a, "serving metrics");
                                Ok(handle)
                            }
                            Err(e) => Err(e.to_string()),
                        }
                    }
                    Err(e) => Err(e),
                },
                Err(e) => Err(format!("METRICS_ADDR {addr:?}: {e}")),
            }
        }
    };
    match handle {
        Ok(h) => {
            spawn_upkeep(h.clone());
            Some(h)
        }
        Err(e) => {
            tracing::warn!(error = %e, "metrics are off");
            None
        }
    }
}

/// Builds the exporter and runs its HTTP listener. Building needs a tokio
/// runtime: the caller's if there is one, otherwise a small one of its own
/// on a background thread (the matcher's main loop is plain threads).
fn serve_metrics(
    builder: PrometheusBuilder,
) -> Result<metrics_exporter_prometheus::PrometheusRecorder, String> {
    async fn listen(server: metrics_exporter_prometheus::ExporterFuture) {
        if let Err(e) = server.await {
            tracing::warn!(error = ?e, "metrics listener stopped");
        }
    }
    if let Ok(rt) = tokio::runtime::Handle::try_current() {
        let _entered = rt.enter();
        let (recorder, server) = builder.build().map_err(|e| e.to_string())?;
        rt.spawn(listen(server));
        return Ok(recorder);
    }
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("metrics".into())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = tx.send(Err(e.to_string()));
                    return;
                }
            };
            rt.block_on(async move {
                match builder.build() {
                    Ok((recorder, server)) => {
                        let _ = tx.send(Ok(recorder));
                        listen(server).await;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e.to_string()));
                    }
                }
            });
        })
        .map_err(|e| e.to_string())?;
    rx.recv().map_err(|e| e.to_string())?
}

/// Every few seconds: drain histogram buffers and sample process memory/CPU.
fn spawn_upkeep(handle: PrometheusHandle) {
    let _ = std::thread::Builder::new()
        .name("metrics-upkeep".into())
        .spawn(move || {
            let process = metrics_process::Collector::default();
            process.describe();
            loop {
                process.collect();
                handle.run_upkeep();
                std::thread::sleep(Duration::from_secs(5));
            }
        });
}

// ------------------------------------------------------------ trace context

/// The current span's context as a W3C `traceparent`, if there is one.
pub fn current_traceparent() -> Option<String> {
    traceparent_of(&tracing::Span::current())
}

/// A span's context as a W3C `traceparent`, if it has one.
pub fn traceparent_of(span: &tracing::Span) -> Option<String> {
    let cx = span.context();
    let sc = cx.span().span_context().clone();
    sc.is_valid().then(|| {
        format!(
            "00-{}-{}-{:02x}",
            sc.trace_id(),
            sc.span_id(),
            sc.trace_flags().to_u8()
        )
    })
}

/// A span's trace ID (32 hex digits), if it has one.
pub fn trace_id_of(span: &tracing::Span) -> Option<String> {
    let cx = span.context();
    let sc = cx.span().span_context().clone();
    sc.is_valid().then(|| sc.trace_id().to_string())
}

/// Makes `span` a child of the remote `traceparent` (when it is valid) and
/// records its `trace_id` field, so its logs carry the ID. Call before the
/// span is first entered.
pub fn follow(span: &tracing::Span, traceparent: Option<&str>) {
    if let Some(cx) = traceparent.and_then(parse_traceparent) {
        let _ = span.set_parent(cx);
    }
    if let Some(id) = trace_id_of(span) {
        span.record("trace_id", id.as_str());
    }
}

/// Links `span` to the remote `traceparent` (when it is valid) without
/// making it the parent: a span that handles several messages at once
/// follows the first and links the rest. Call before the span is first
/// entered.
pub fn link(span: &tracing::Span, traceparent: Option<&str>) {
    if let Some(cx) = traceparent.and_then(parse_traceparent) {
        span.add_link(cx.span().span_context().clone());
    }
}

/// Whether a `traceparent` asks for the trace to be recorded.
pub fn is_sampled(traceparent: &str) -> bool {
    traceparent.len() == 55 && traceparent.ends_with("01")
}

fn parse_traceparent(tp: &str) -> Option<opentelemetry::Context> {
    struct One<'a>(&'a str);
    impl Extractor for One<'_> {
        fn get(&self, key: &str) -> Option<&str> {
            (key == "traceparent").then_some(self.0)
        }
        fn keys(&self) -> Vec<&str> {
            vec!["traceparent"]
        }
    }
    let cx = TraceContextPropagator::new().extract(&One(tp));
    cx.span().span_context().is_valid().then_some(cx)
}

/// Adds the current span's `traceparent` to outgoing gRPC request metadata.
#[cfg(feature = "grpc")]
pub fn inject_metadata(metadata: &mut tonic::metadata::MetadataMap) {
    if let Some(v) = current_traceparent().and_then(|tp| tp.parse().ok()) {
        metadata.insert("traceparent", v);
    }
}

/// A gRPC request for `message` carrying the current span's trace context.
#[cfg(feature = "grpc")]
pub fn grpc_request<T>(message: T) -> tonic::Request<T> {
    let mut r = tonic::Request::new(message);
    inject_metadata(r.metadata_mut());
    r
}

/// The value of a `traceparent` Kafka header, if present.
pub fn traceparent_header(headers: &[(String, Vec<u8>)]) -> Option<&str> {
    headers
        .iter()
        .find(|(k, _)| k == "traceparent")
        .and_then(|(_, v)| std::str::from_utf8(v).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traceparents_parse_and_sampling_reads_the_flags() {
        let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        assert!(parse_traceparent(tp).is_some());
        assert!(is_sampled(tp));
        assert!(!is_sampled(
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00"
        ));
        assert!(parse_traceparent("garbage").is_none());
        assert!(
            parse_traceparent("00-00000000000000000000000000000000-00f067aa0ba902b7-01").is_none()
        );
    }

    #[test]
    fn metrics_serve_without_a_runtime() {
        // The matcher calls init outside any tokio runtime.
        let b = PrometheusBuilder::new().with_http_listener(([127, 0, 0, 1], 0));
        assert!(serve_metrics(b).is_ok());
    }

    #[test]
    fn kafka_header_lookup() {
        let h = vec![
            ("other".to_owned(), b"x".to_vec()),
            ("traceparent".to_owned(), b"00-ab".to_vec()),
        ];
        assert_eq!(traceparent_header(&h), Some("00-ab"));
        assert_eq!(traceparent_header(&[]), None);
    }

    /// A log line written inside a span carries that span's trace id, so
    /// Loki can link it to the trace in Tempo.
    #[test]
    fn logs_carry_the_trace_id() {
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::logs::{InMemoryLogExporter, SdkLoggerProvider};
        use opentelemetry_sdk::trace::SdkTracerProvider;
        use tracing_subscriber::prelude::*;

        let logs = InMemoryLogExporter::default();
        let logger = SdkLoggerProvider::builder()
            .with_simple_exporter(logs.clone())
            .build();
        let tracer = SdkTracerProvider::builder().build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(tracer.tracer("test")))
            .with(OpenTelemetryTracingBridge::new(&logger));
        let want = tracing::subscriber::with_default(subscriber, || {
            let span = tracing::info_span!("request");
            let _entered = span.enter();
            tracing::warn!("inside the request");
            trace_id_of(&span)
        });
        let emitted = logs.get_emitted_logs().expect("logs");
        let got = emitted
            .first()
            .and_then(|l| l.record.trace_context())
            .map(|c| c.trace_id.to_string());
        assert!(want.is_some());
        assert_eq!(got, want);
    }
}
