// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

mod api_coordinator;
mod file_coordinator;
mod live_counters;
mod object_trace;

use std::sync::Arc;
use std::{net, path::PathBuf};

use clap::Parser;
use url::Url;

use api_coordinator::{ApiCoordinator, ApiCoordinatorConfig};
use file_coordinator::FileCoordinator;
use moq_relay_ietf::{
    Coordinator, DataPriorityMapping, Relay, RelayConfig, SessionConfig, Web, WebConfig,
};

#[derive(Parser, Clone)]
pub struct Cli {
    /// Listen on this address
    #[arg(long, default_value = "[::]:443")]
    pub bind: net::SocketAddr,

    /// The TLS configuration.
    #[command(flatten)]
    pub tls: moq_native_ietf::tls::Args,

    /// Directory to write qlog files (one per connection)
    #[arg(long)]
    pub qlog_dir: Option<PathBuf>,

    /// Directory to write mlog files (one per connection)
    #[arg(long)]
    pub mlog_dir: Option<PathBuf>,

    /// Bounded object boundary trace (verification only; Linux; new path only).
    #[arg(long, requires = "object_trace_run_id")]
    pub object_trace: Option<PathBuf>,

    #[arg(long, requires = "object_trace")]
    pub object_trace_run_id: Option<String>,
    /// Preallocated relay object-trace record bound (four boundaries per
    /// forwarded object). Sized by the runner from the planned object count.
    #[arg(long, default_value_t = 4096)]
    pub object_trace_capacity: usize,

    /// Live per-track object-boundary counters (JSONL sidecar, create-only):
    /// one cumulative snapshot line per period and a final line at shutdown.
    /// Off when absent.
    #[arg(long)]
    pub live_counters_out: Option<PathBuf>,

    /// Live counter snapshot period in milliseconds.
    #[arg(long, default_value_t = 100, value_parser = clap::value_parser!(u64).range(1..))]
    pub live_counters_period_ms: u64,

    /// Maximum request ID plus one advertised in MoQT setup.
    #[arg(long, default_value_t = 100)]
    pub max_request_id: u64,

    /// Translation from MoQT publisher priority to quinn stream priority.
    ///
    /// legacy-v1 preserves historical evidence; moqt-v2 preserves MoQT's
    /// lower-number-first ordering at quinn's higher-number-first scheduler.
    #[arg(long, default_value = "legacy-v1")]
    pub data_priority_mapping: DataPriorityMapping,

    /// Forward all PUBLISH_NAMESPACE messages to the provided server for auth/routing.
    /// If not provided, the relay accepts every unique namespace publish.
    #[arg(long)]
    pub announce: Option<Url>,

    /// The URL of the moq-api server in order to run a cluster.
    /// Must be used in conjunction with --node to advertise the origin
    #[arg(long)]
    pub api: Option<Url>,

    /// The hostname that we advertise to other origins.
    /// The provided certificate must be valid for this address.
    #[arg(long)]
    pub node: Option<Url>,

    /// Enable development mode.
    /// This hosts a HTTPS web server via TCP to serve the fingerprint of the certificate.
    #[arg(long)]
    pub dev: bool,

    /// Serve qlog files over HTTPS at /qlog/:cid
    /// Requires --dev to enable the web server. Only serves files by exact CID - no index.
    #[arg(long)]
    pub qlog_serve: bool,

    /// Serve mlog files over HTTPS at /mlog/:cid
    /// Requires --dev to enable the web server. Only serves files by exact CID - no index.
    #[arg(long)]
    pub mlog_serve: bool,

    /// Path to the shared coordinator file for multi-relay coordination.
    /// Multiple relay instances can share namespace/track registration via this file.
    /// User doesn't have to explicitly create and populate anything. This path will be
    /// used by file coordinator to store namespace/track registration information.
    /// User need to make sure if multiple relay's are being used all of them have same path
    /// to this file.
    #[arg(long, default_value = "/tmp/moq-coordinator.json")]
    pub coordinator_file: PathBuf,

    /// URL of the moq-api server for coordination (e.g., "http://localhost:8080").
    /// When specified, uses moq-api HTTP server instead of file-based coordination.
    /// This is useful when running a cluster of relays with a centralized API server.
    #[arg(long)]
    pub api_url: Option<Url>,

    /// TTL in seconds for namespace registrations in the API.
    /// Only used when --api-url is specified.
    #[arg(long, default_value = "600")]
    pub api_ttl: u64,

    /// Address to expose Prometheus metrics on (e.g., "127.0.0.1:9090").
    /// Requires the `metrics-prometheus` feature to be enabled.
    /// When set, serves metrics at http://<addr>/metrics
    #[arg(long)]
    pub metrics_addr: Option<net::SocketAddr>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing with env filter (respects RUST_LOG environment variable)
    // Default to info level, but suppress quinn's verbose output
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,quinn=warn")),
        )
        .init();

    let cli = Cli::parse();

    // Initialize Prometheus metrics exporter if --metrics-addr is provided
    #[cfg(feature = "metrics-prometheus")]
    if let Some(metrics_addr) = cli.metrics_addr {
        use metrics_exporter_prometheus::PrometheusBuilder;

        // Configure histogram buckets for subscribe latency (1ms to 10s)
        let subscribe_latency_buckets = vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0, 10.0,
        ];

        PrometheusBuilder::new()
            .with_http_listener(metrics_addr)
            .set_buckets_for_metric(
                metrics_exporter_prometheus::Matcher::Full(
                    "moq_relay_subscribe_latency_seconds".to_string(),
                ),
                &subscribe_latency_buckets,
            )?
            .install()
            .expect("failed to install Prometheus metrics exporter");

        // Register metric descriptions (shows as # HELP in Prometheus output)
        moq_relay_ietf::metrics::describe_metrics();

        tracing::info!(
            "metrics exporter listening on http://{}/metrics",
            metrics_addr
        );
    }

    #[cfg(not(feature = "metrics-prometheus"))]
    if cli.metrics_addr.is_some() {
        tracing::warn!(
            "--metrics-addr was provided but the metrics-prometheus feature is not enabled. \
             Rebuild with --features metrics-prometheus to enable the Prometheus exporter."
        );
    }

    let tls = cli.tls.load()?;

    if tls.server.is_none() {
        anyhow::bail!("missing TLS certificates");
    }

    // Determine qlog directory for both relay and web server
    let qlog_dir_for_relay = cli.qlog_dir.clone();
    let qlog_dir_for_web = if cli.qlog_serve {
        cli.qlog_dir.clone()
    } else {
        None
    };

    // Determine mlog directory for both relay and web server
    let mlog_dir_for_relay = cli.mlog_dir.clone();
    let mlog_dir_for_web = if cli.mlog_serve {
        cli.mlog_dir.clone()
    } else {
        None
    };

    // Build the relay URL from the node or bind address
    let relay_url = cli
        .node
        .clone()
        .unwrap_or_else(|| Url::parse(&format!("https://{}", cli.bind)).unwrap());

    // Create the coordinator based on CLI arguments
    // Priority: api-url > file coordinator
    let coordinator: Arc<dyn Coordinator> = if let Some(api_url) = &cli.api_url {
        let config = ApiCoordinatorConfig::new(api_url.clone(), relay_url).with_ttl(cli.api_ttl);
        let api_coordinator = ApiCoordinator::new(config);
        tracing::info!("using API coordinator: {}", api_url);
        Arc::new(api_coordinator)
    } else {
        tracing::info!("using file coordinator: {}", cli.coordinator_file.display());
        Arc::new(FileCoordinator::new(&cli.coordinator_file, relay_url))
    };

    // Create a QUIC server for media.
    let relay = Relay::new(RelayConfig {
        tls: tls.clone(),
        bind: Some(cli.bind),
        endpoints: vec![],
        qlog_dir: qlog_dir_for_relay,
        mlog_dir: mlog_dir_for_relay,
        node: cli.node,
        announce: cli.announce,
        coordinator,
        session: SessionConfig {
            max_request_id: cli.max_request_id,
            data_priority_mapping: cli.data_priority_mapping,
        },
    })?;

    if cli.dev {
        // Create a web server too.
        // Currently this only contains the certificate fingerprint (for development only).
        let web = Web::new(WebConfig {
            bind: cli.bind,
            tls,
            qlog_dir: qlog_dir_for_web,
            mlog_dir: mlog_dir_for_web,
        });

        tokio::spawn(async move {
            web.run().await.expect("failed to run web server");
        });
    }

    if cli.object_trace.is_none() && cli.live_counters_out.is_none() {
        return relay.run().await;
    }
    // Only the opt-in verification modes intercept SIGTERM. The harness
    // sends it after endpoints finish, then checks the sealed trace footer
    // and/or the live counters' final line.
    let live = cli.live_counters_out.as_ref()
        .map(|_| Arc::new(live_counters::Registry::new(live_counters::REGISTRY_CAPACITY)));
    let recorder = Arc::new(object_trace::Recorder::for_relay(
        cli.object_trace_capacity, cli.object_trace.is_some(), live.clone()));
    let trace = match &cli.object_trace {
        Some(path) => Some(object_trace::Trace::open(path, cli.object_trace_run_id.as_deref().unwrap(),
            cli.object_trace_capacity, cli.data_priority_mapping, recorder.clone())?),
        None => None,
    };
    let writer = match (&cli.live_counters_out, live) {
        (Some(path), Some(registry)) => Some(live_counters::LiveWriter::start(
            path, std::time::Duration::from_millis(cli.live_counters_period_ms), registry)?),
        _ => None,
    };
    object_trace::install(recorder.clone())?;
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let (ending, result) = tokio::select! {
        result = relay.run() => ("relay_returned", result),
        _ = terminate.recv() => ("sigterm", Ok(())),
    };
    // One admission cut for both: the final counters and the trace rows
    // cover exactly the same callbacks.
    recorder.close_admission()?;
    if let Some(writer) = writer {
        writer.finish()?;
    }
    if let Some(trace) = trace {
        trace.finish(ending)?;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_trace_is_opt_in_and_requires_identity() {
        assert!(Cli::try_parse_from(["moq-relay-ietf"]).unwrap().object_trace.is_none());
        assert!(Cli::try_parse_from(["moq-relay-ietf", "--object-trace", "/tmp/trace"]).is_err());
        assert!(Cli::try_parse_from(["moq-relay-ietf", "--object-trace-run-id", "run"]).is_err());
        assert!(Cli::try_parse_from(["moq-relay-ietf", "--object-trace", "/tmp/trace",
                                    "--object-trace-run-id", "run"]).is_ok());
    }

    #[test]
    fn max_request_id_flag_overrides_default() {
        let cli = Cli::try_parse_from(["moq-relay-ietf", "--max-request-id", "7"]).unwrap();

        assert_eq!(cli.max_request_id, 7);
    }

    #[test]
    fn live_counters_are_opt_in_with_a_positive_period() {
        let default_cli = Cli::try_parse_from(["moq-relay-ietf"]).unwrap();
        assert!(default_cli.live_counters_out.is_none(), "off by default");
        assert_eq!(default_cli.live_counters_period_ms, 100);
        let on = Cli::try_parse_from(["moq-relay-ietf", "--live-counters-out", "/tmp/live.jsonl",
                                      "--live-counters-period-ms", "250"]).unwrap();
        assert_eq!(on.live_counters_out.as_deref(), Some(std::path::Path::new("/tmp/live.jsonl")));
        assert_eq!(on.live_counters_period_ms, 250);
        assert!(on.object_trace.is_none(), "independent of the object trace");
        assert!(Cli::try_parse_from(["moq-relay-ietf", "--live-counters-period-ms", "0"]).is_err());
    }

    #[test]
    fn data_priority_mapping_flag_is_explicit_and_validated() {
        let default_cli = Cli::try_parse_from(["moq-relay-ietf"]).unwrap();
        assert_eq!(
            default_cli.data_priority_mapping,
            DataPriorityMapping::LegacyV1
        );

        let v2 =
            Cli::try_parse_from(["moq-relay-ietf", "--data-priority-mapping", "moqt-v2"]).unwrap();
        assert_eq!(v2.data_priority_mapping, DataPriorityMapping::MoqtV2);
        assert!(Cli::try_parse_from(["moq-relay-ietf", "--data-priority-mapping", "v2"]).is_err());
    }
}
