// SPDX-FileCopyrightText: 2024-2026 Cloudflare Inc., Luke Curley, Mike English and contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

use std::{
    collections::HashSet,
    fmt,
    fs::File,
    io::BufWriter,
    net::{self, IpAddr},
    path::PathBuf,
    sync::{Arc, Mutex},
    time,
};

use anyhow::Context;
use clap::Parser;
use socket2::{Domain, Protocol, Socket, Type};
use url::Url;

use moq_transport::session::Transport;

use crate::tls;

use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::FutureExt;

/// Represents the address family of the local QUIC socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressFamily {
    Ipv4,
    Ipv6,
    /// IPv6 with dual-stack support (IPV6_V6ONLY=false)
    Ipv6DualStack,
}

pub enum Host {
    Ip(IpAddr),
    Name(String),
}

impl fmt::Display for AddressFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AddressFamily::Ipv4 => write!(f, "IPv4"),
            AddressFamily::Ipv6 => write!(f, "IPv6"),
            AddressFamily::Ipv6DualStack => write!(f, "IPv6 (dual stack)"),
        }
    }
}

/// Bind a UDP socket, attempting dual-stack if the address is IPv6.
///
/// For IPv6 addresses, attempts to set `IPV6_V6ONLY = false` to enable
/// dual-stack operation (accepting both IPv4 and IPv6 traffic). This is
/// the default on Linux but must be explicitly requested on macOS/Windows.
///
/// Returns `(socket, is_dual_stack)` where `is_dual_stack` indicates
/// whether the socket can handle both IPv4 and IPv6 destinations.
fn bind_smart(addr: net::SocketAddr) -> anyhow::Result<(net::UdpSocket, bool)> {
    let domain = if addr.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };
    let socket = Socket::new(domain, Type::DGRAM, Some(Protocol::UDP))
        .context("failed to create UDP socket")?;

    let mut is_dual_stack = false;

    if addr.is_ipv6() {
        match socket.set_only_v6(false) {
            Ok(()) => {
                is_dual_stack = true;
                tracing::debug!(addr = %addr, "IPv6 dual-stack enabled (IPV6_V6ONLY=false)");
            }
            Err(e) => {
                tracing::warn!(
                    addr = %addr,
                    error = %e,
                    "Could not enable dual-stack on IPv6 socket; \
                     IPv4-only destinations may be unreachable"
                );
            }
        }
    }

    socket
        .bind(&addr.into())
        .with_context(|| format!("failed to bind UDP socket to {}", addr))?;

    let local_addr = match socket.local_addr() {
        Ok(a) => a
            .as_socket()
            .map(|s| s.to_string())
            .unwrap_or_else(|| "<non-IP address>".to_string()),
        Err(e) => {
            tracing::warn!(error = %e, "failed to get local address after successful bind");
            "<unknown>".to_string()
        }
    };

    tracing::info!(
        bind = %addr,
        local = %local_addr,
        dual_stack = is_dual_stack,
        "UDP socket bound"
    );

    Ok((socket.into(), is_dual_stack))
}

/// QUIC congestion controller used by every connection of an [`Endpoint`].
///
/// The default, [`CongestionChoice::Bbr`], is the controller moq-rs has always
/// set explicitly; quinn's own default would be CUBIC. Both variants use the
/// quinn config defaults (`BbrConfig::default()` / `CubicConfig::default()`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum CongestionChoice {
    /// quinn BBR (`BbrConfig::default()`): the historical moq-rs setting.
    #[default]
    Bbr,
    /// quinn CUBIC (`CubicConfig::default()`).
    Cubic,
}

impl CongestionChoice {
    /// The CLI spelling (`bbr` / `cubic`).
    pub fn as_str(self) -> &'static str {
        match self {
            CongestionChoice::Bbr => "bbr",
            CongestionChoice::Cubic => "cubic",
        }
    }
}

impl fmt::Display for CongestionChoice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Build a TransportConfig with our standard settings
///
/// This is used both for the base endpoint config and when creating
/// per-connection configs with qlog enabled.
fn build_transport_config(congestion: CongestionChoice) -> quinn::TransportConfig {
    let mut transport = quinn::TransportConfig::default();
    transport.max_idle_timeout(Some(time::Duration::from_secs(10).try_into().unwrap()));
    transport.keep_alive_interval(Some(time::Duration::from_secs(4))); // TODO make this smarter
    match congestion {
        CongestionChoice::Bbr => transport
            .congestion_controller_factory(Arc::new(quinn::congestion::BbrConfig::default())),
        CongestionChoice::Cubic => transport
            .congestion_controller_factory(Arc::new(quinn::congestion::CubicConfig::default())),
    };
    transport.mtu_discovery_config(None); // Disable MTU discovery
    transport
}

/// CLOCK_MONOTONIC in µs, the clock of the skew logs (`t_gen`/`t_recv`).
/// Used only to anchor a client qlog's relative event times.
fn clock_monotonic_us() -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid, writable timespec; CLOCK_MONOTONIC always exists on Linux.
    unsafe {
        libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts);
    }
    (ts.tv_sec as u64) * 1_000_000 + (ts.tv_nsec as u64) / 1000
}

#[derive(Parser, Clone)]
pub struct Args {
    /// Listen for UDP packets on the given address.
    ///
    /// Defaults to [::]:0 (IPv6 with dual-stack). If the default IPv6 bind
    /// fails, automatically falls back to 0.0.0.0 (IPv4-only) with a warning.
    /// Explicitly provided IPv6 addresses will not fall back.
    #[arg(long, default_value = Args::DEFAULT_BIND)]
    pub bind: net::SocketAddr,

    /// Directory to write qlog files (one per connection)
    #[arg(long)]
    pub qlog_dir: Option<PathBuf>,

    #[command(flatten)]
    pub tls: tls::Args,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            bind: Self::DEFAULT_BIND.parse().unwrap(),
            qlog_dir: None,
            tls: Default::default(),
        }
    }
}

impl Args {
    /// The default bind address used when `--bind` is not explicitly provided.
    const DEFAULT_BIND: &str = "[::]:0";

    pub fn load(&self) -> anyhow::Result<Config> {
        let tls = self.tls.load()?;

        match Config::new(self.bind, self.qlog_dir.clone(), tls.clone()) {
            Ok(config) => Ok(config),
            Err(e) if self.bind.to_string() == Self::DEFAULT_BIND => {
                // IPv6 default bind failed -- try falling back to IPv4.
                // Only do this for the default; if the user explicitly
                // requested an IPv6 address, respect that and propagate
                // the error.
                let fallback = net::SocketAddr::new(
                    net::IpAddr::V4(net::Ipv4Addr::UNSPECIFIED),
                    self.bind.port(),
                );
                tracing::warn!(
                    requested = %self.bind,
                    fallback = %fallback,
                    error = %e,
                    "IPv6 bind failed, falling back to IPv4"
                );
                Config::new(fallback, self.qlog_dir.clone(), tls).with_context(|| {
                    format!("IPv4 fallback also failed (original IPv6 error: {})", e)
                })
            }
            Err(e) => Err(e),
        }
    }
}

/// A hook to wrap the endpoint's [`quinn::AsyncUdpSocket`] before it is handed
/// to quinn.
///
/// This lets callers interpose custom socket behavior — for example,
/// byte-counting for metrics — without this crate depending on the caller's
/// instrumentation. The closure receives the runtime-wrapped socket and
/// returns the socket quinn should actually use (typically the input wrapped
/// in a decorator).
///
/// Construct one via [`Config::with_socket_wrapper`], which accepts any
/// matching closure; this boxed alias is the stored form. `Box` (rather than
/// `Arc`) is sufficient because `Config` is not `Clone` and the wrapper is
/// invoked exactly once, during [`Endpoint::new`].
pub type SocketWrapperFn = Box<
    dyn Fn(Arc<dyn quinn::AsyncUdpSocket>) -> Arc<dyn quinn::AsyncUdpSocket>
        + Send
        + Sync
        + 'static,
>;

pub struct Config {
    pub bind: Option<net::SocketAddr>,
    pub socket: net::UdpSocket,
    pub is_dual_stack: bool,
    pub qlog_dir: Option<PathBuf>,
    pub tls: tls::Config,
    pub tags: HashSet<String>,
    /// Optional hook to wrap the [`quinn::AsyncUdpSocket`] before endpoint
    /// creation. Defaults to `None` (no wrapping). See [`SocketWrapperFn`].
    pub socket_wrapper: Option<SocketWrapperFn>,
    /// Congestion controller for every connection (base and per-connection
    /// qlog transport configs). Defaults to BBR. See [`Config::with_congestion`].
    pub congestion: CongestionChoice,
    /// Also write a qlog for each outgoing (client) connection into
    /// `qlog_dir`. Defaults to `false`: `qlog_dir` alone covers only accepted
    /// (server) connections, as before. See [`Config::with_client_qlog`].
    pub client_qlog: bool,
}

impl Config {
    pub fn new(
        bind: net::SocketAddr,
        qlog_dir: Option<PathBuf>,
        tls: tls::Config,
    ) -> anyhow::Result<Self> {
        let (socket, is_dual_stack) = bind_smart(bind)?;
        Ok(Self {
            bind: Some(bind),
            socket,
            is_dual_stack,
            qlog_dir,
            tls,
            tags: HashSet::new(),
            socket_wrapper: None,
            congestion: CongestionChoice::default(),
            client_qlog: false,
        })
    }

    pub fn with_socket(
        socket: net::UdpSocket,
        qlog_dir: Option<PathBuf>,
        tls: tls::Config,
    ) -> Self {
        // Probe the socket to detect dual-stack capability rather than assuming.
        let is_dual_stack = socket.local_addr().is_ok_and(|addr| {
            addr.is_ipv6() && {
                let sock_ref = socket2::SockRef::from(&socket);
                sock_ref.only_v6().map(|v6only| !v6only).unwrap_or(false)
            }
        });

        Self {
            bind: None,
            socket,
            is_dual_stack,
            qlog_dir,
            tls,
            tags: HashSet::new(),
            socket_wrapper: None,
            congestion: CongestionChoice::default(),
            client_qlog: false,
        }
    }

    pub fn with_tag(mut self, tag: String) -> Self {
        self.tags.insert(tag);
        self
    }

    /// Attach a closure that wraps the endpoint's [`quinn::AsyncUdpSocket`]
    /// before it is handed to quinn. See [`SocketWrapperFn`].
    pub fn with_socket_wrapper<F>(mut self, wrapper: F) -> Self
    where
        F: Fn(Arc<dyn quinn::AsyncUdpSocket>) -> Arc<dyn quinn::AsyncUdpSocket>
            + Send
            + Sync
            + 'static,
    {
        self.socket_wrapper = Some(Box::new(wrapper));
        self
    }

    /// Select the QUIC congestion controller. Without this call the endpoint
    /// uses [`CongestionChoice::Bbr`], the historical moq-rs setting.
    pub fn with_congestion(mut self, congestion: CongestionChoice) -> Self {
        self.congestion = congestion;
        self
    }

    /// When `enabled` and `qlog_dir` is set, every outgoing connection made
    /// through [`Client::connect`] writes `<initial-dcid>_client.qlog` into
    /// `qlog_dir` (the same CID names the peer's `_server.qlog`). Without this
    /// call client connections write no qlog, exactly as before.
    pub fn with_client_qlog(mut self, enabled: bool) -> Self {
        self.client_qlog = enabled;
        self
    }
}

pub struct Endpoint {
    pub client: Client,
    pub server: Option<Server>,
    /// Tags associated with this endpoint
    /// These are used to filter endpoints for different purposes, for eg-
    /// "server" tag is used to filter endpoints for relay server
    /// "forward" tag is used to filter endpoints for forwarder
    /// This is upto the user to define and use
    pub tags: HashSet<String>,
}

impl Endpoint {
    pub fn new(config: Config) -> anyhow::Result<Self> {
        // Validate qlog directory if provided

        if let Some(qlog_dir) = &config.qlog_dir {
            if !qlog_dir.exists() {
                anyhow::bail!("qlog directory does not exist: {}", qlog_dir.display());
            }
            if !qlog_dir.is_dir() {
                anyhow::bail!("qlog path is not a directory: {}", qlog_dir.display());
            }
            tracing::info!("qlog output enabled: {}", qlog_dir.display());
        }

        // Build transport config with our standard settings
        let transport = Arc::new(build_transport_config(config.congestion));

        let mut server_config = None;

        if let Some(mut config) = config.tls.server {
            config.alpn_protocols = vec![
                web_transport_quinn::ALPN.as_bytes().to_vec(),
                moq_transport::setup::ALPN.to_vec(),
            ];
            config.key_log = Arc::new(rustls::KeyLogFile::new());

            let config: quinn::crypto::rustls::QuicServerConfig = config.try_into()?;
            let mut config = quinn::ServerConfig::with_crypto(Arc::new(config));
            config.transport_config(transport.clone());

            server_config = Some(config);
        }

        // There's a bit more boilerplate to make a generic endpoint.
        let runtime = quinn::default_runtime().context("no async runtime")?;
        let endpoint_config = quinn::EndpointConfig::default();
        let socket = config.socket;

        // Create the generic QUIC endpoint. When a socket wrapper is configured,
        // wrap the std socket into quinn's AsyncUdpSocket and let the caller
        // interpose (e.g. for byte-counting metrics) before quinn takes it.
        let quic = match config.socket_wrapper {
            Some(wrap) => {
                let async_socket = runtime
                    .wrap_udp_socket(socket)
                    .context("failed to wrap UDP socket")?;
                let wrapped = wrap(async_socket);
                quinn::Endpoint::new_with_abstract_socket(
                    endpoint_config,
                    server_config.clone(),
                    wrapped,
                    runtime,
                )
                .context("failed to create QUIC endpoint")?
            }
            None => quinn::Endpoint::new(endpoint_config, server_config.clone(), socket, runtime)
                .context("failed to create QUIC endpoint")?,
        };

        let qlog_dir = config.qlog_dir.map(Arc::new);
        let client_qlog_dir = if config.client_qlog {
            qlog_dir.clone()
        } else {
            None
        };

        let server = server_config.map(|base_server_config| Server {
            quic: quic.clone(),
            accept: Default::default(),
            qlog_dir,
            base_server_config: Arc::new(base_server_config),
            congestion: config.congestion,
        });

        let client = Client {
            quic,
            config: config.tls.client,
            transport,
            is_dual_stack: config.is_dual_stack,
            qlog_dir: client_qlog_dir,
            congestion: config.congestion,
        };

        Ok(Self {
            client,
            server,
            tags: config.tags,
        })
    }
}

pub struct Server {
    quic: quinn::Endpoint,
    accept: FuturesUnordered<
        BoxFuture<'static, anyhow::Result<(web_transport::Session, String, Transport)>>,
    >,
    qlog_dir: Option<Arc<PathBuf>>,
    base_server_config: Arc<quinn::ServerConfig>,
    congestion: CongestionChoice,
}

impl Server {
    pub async fn accept(&mut self) -> Option<(web_transport::Session, String, Transport)> {
        loop {
            tokio::select! {
                res = self.quic.accept() => {
                    let conn = res?;
                    let qlog_dir = self.qlog_dir.clone();
                    let base_server_config = self.base_server_config.clone();
                    let congestion = self.congestion;
                    self.accept.push(Self::accept_session(conn, qlog_dir, base_server_config, congestion).boxed());
                },
                res = self.accept.next(), if !self.accept.is_empty() => {
                    match res? {
                        Ok(result) => return Some(result),
                        Err(err) => {
                            tracing::warn!("failed to accept QUIC connection: {}", err.root_cause());
                            continue;
                        }
                    }
                }
            }
        }
    }

    async fn accept_session(
        conn: quinn::Incoming,
        qlog_dir: Option<Arc<PathBuf>>,
        base_server_config: Arc<quinn::ServerConfig>,
        congestion: CongestionChoice,
    ) -> anyhow::Result<(web_transport::Session, String, Transport)> {
        // Capture the original destination connection ID BEFORE accepting
        // This is the actual QUIC CID that can be used for qlog/mlog correlation
        let orig_dst_cid = conn.orig_dst_cid();
        let connection_id_hex = orig_dst_cid.to_string();

        // Configure per-connection qlog if enabled
        let mut conn = if let Some(qlog_dir) = qlog_dir {
            // Create qlog file path using connection ID
            let qlog_path = qlog_dir.join(format!("{}_server.qlog", connection_id_hex));

            // Create transport config with our standard settings plus qlog
            let mut transport = build_transport_config(congestion);

            let file = File::create(&qlog_path).context("failed to create qlog file")?;
            let writer = BufWriter::new(file);

            let mut qlog = quinn::QlogConfig::default();
            qlog.writer(Box::new(writer))
                .title(Some("moq-relay".into()));
            transport.qlog_stream(qlog.into_stream());

            // Create custom server config with qlog-enabled transport
            let mut server_config = (*base_server_config).clone();
            server_config.transport_config(Arc::new(transport));

            tracing::debug!(
                "qlog enabled: cid={} path={}",
                connection_id_hex,
                qlog_path.display()
            );

            // Accept with custom config
            conn.accept_with(Arc::new(server_config))?
        } else {
            // No qlog - use default config
            conn.accept()?
        };

        let handshake = conn
            .handshake_data()
            .await?
            .downcast::<quinn::crypto::rustls::HandshakeData>()
            .unwrap();

        let alpn = handshake.protocol.context("missing ALPN")?;
        let alpn = String::from_utf8_lossy(&alpn);
        let server_name = handshake.server_name.unwrap_or_default();

        tracing::debug!(
            "received QUIC handshake: cid={} ip={} alpn={} server={}",
            connection_id_hex,
            conn.remote_address(),
            alpn,
            server_name,
        );

        // Wait for the QUIC connection to be established.
        let conn = conn.await.context("failed to establish QUIC connection")?;

        tracing::debug!(
            "established QUIC connection: cid={} stable_id={} ip={} alpn={} server={}",
            connection_id_hex,
            conn.stable_id(),
            conn.remote_address(),
            alpn,
            server_name,
        );

        let alpn_bytes = alpn.as_bytes();
        let (session, transport) = if alpn_bytes == web_transport_quinn::ALPN.as_bytes() {
            // Wait for the WebTransport CONNECT request (includes H3 SETTINGS exchange).
            let request = web_transport_quinn::Request::accept(conn)
                .await
                .context("failed to receive WebTransport request")?;

            let moqt_protocol = std::str::from_utf8(moq_transport::setup::ALPN)
                .context("invalid MoQT ALPN")?
                .to_string();
            let response = if request.protocols.contains(&moqt_protocol) {
                web_transport_quinn::proto::ConnectResponse::OK.with_protocol(moqt_protocol)
            } else {
                web_transport_quinn::proto::ConnectResponse::OK
            };

            // Accept the CONNECT request.
            let session = request
                .respond(response)
                .await
                .context("failed to respond to WebTransport request")?;
            (session, Transport::WebTransport)
        } else if alpn_bytes == moq_transport::setup::ALPN {
            // Raw QUIC mode — create a "fake" WebTransport session with no H3 framing.
            let request = url::Url::parse("moqt://localhost").unwrap();
            let session = web_transport_quinn::Session::raw(
                conn,
                request,
                web_transport_quinn::proto::ConnectResponse::default(),
            );
            (session, Transport::RawQuic)
        } else {
            anyhow::bail!("unsupported ALPN: {}", alpn)
        };

        Ok((session.into(), connection_id_hex, transport))
    }

    pub fn local_addr(&self) -> anyhow::Result<net::SocketAddr> {
        self.quic
            .local_addr()
            .context("failed to get local address")
    }
}

#[derive(Clone)]
pub struct Client {
    quic: quinn::Endpoint,
    config: rustls::ClientConfig,
    transport: Arc<quinn::TransportConfig>,
    is_dual_stack: bool,
    /// Set only by [`Config::with_client_qlog`]: per-connection client qlog.
    qlog_dir: Option<Arc<PathBuf>>,
    congestion: CongestionChoice,
}

impl Client {
    /// Returns the local address of the QUIC socket.
    pub fn local_addr(&self) -> anyhow::Result<net::SocketAddr> {
        self.quic
            .local_addr()
            .context("failed to get local address")
    }

    /// Returns the address family of the local QUIC socket.
    ///
    /// Uses the dual-stack state determined at bind time rather than
    /// compile-time platform assumptions.
    pub fn address_family(&self) -> anyhow::Result<AddressFamily> {
        let local_addr = self
            .quic
            .local_addr()
            .context("failed to get local socket address")?;

        if local_addr.is_ipv4() {
            Ok(AddressFamily::Ipv4)
        } else if self.is_dual_stack {
            Ok(AddressFamily::Ipv6DualStack)
        } else {
            Ok(AddressFamily::Ipv6)
        }
    }

    pub async fn connect(
        &self,
        url: &Url,
        socket_addr: Option<net::SocketAddr>,
    ) -> anyhow::Result<(web_transport::Session, String, Transport)> {
        let mut config = self.config.clone();

        // TODO support connecting to both ALPNs at the same time
        config.alpn_protocols = vec![match url.scheme() {
            "https" => web_transport_quinn::ALPN.as_bytes().to_vec(),
            "moqt" => moq_transport::setup::ALPN.to_vec(),
            _ => anyhow::bail!("url scheme must be 'https' or 'moqt'"),
        }];

        config.key_log = Arc::new(rustls::KeyLogFile::new());

        let config: quinn::crypto::rustls::QuicClientConfig = config.try_into()?;
        let mut config = quinn::ClientConfig::new(Arc::new(config));

        // Client qlog (opt-in, Config::with_client_qlog): the file is named by
        // the initial destination CID, so that CID is drawn here, before the
        // transport config is built, and handed to the provider below.
        // Without client qlog this is None and nothing below changes.
        let preset_cid = match &self.qlog_dir {
            Some(qlog_dir) => {
                let cid = random_initial_dst_cid();
                let qlog_path = qlog_dir.join(format!("{}_client.qlog", cid));
                let file = File::create(&qlog_path).context("failed to create client qlog file")?;
                let mut qlog = quinn::QlogConfig::default();
                // qlog event times are ms relative to `start`; record the
                // CLOCK_MONOTONIC µs of that instant (read back to back) so
                // they can be aligned with the skew logs' t_gen/t_recv.
                let start = time::Instant::now();
                let start_mono_us = clock_monotonic_us();
                qlog.writer(Box::new(BufWriter::new(file)))
                    .title(Some("moq-client".into()))
                    .description(Some(format!(
                        "congestion={} start_time_clock_monotonic_us={}",
                        self.congestion, start_mono_us
                    )))
                    .start_time(start);
                let mut transport = build_transport_config(self.congestion);
                transport.qlog_stream(qlog.into_stream());
                config.transport_config(Arc::new(transport));
                tracing::info!(
                    "client qlog enabled: cid={} path={} congestion={} start_time_clock_monotonic_us={}",
                    cid,
                    qlog_path.display(),
                    self.congestion,
                    start_mono_us
                );
                Some(cid)
            }
            None => {
                config.transport_config(self.transport.clone());
                None
            }
        };

        // Capture the initial destination CID that will be sent to the server
        // This is the CID used for qlog/mlog correlation on the server side
        let cid_capture: Arc<Mutex<Option<quinn::ConnectionId>>> = Arc::new(Mutex::new(None));
        let cid_capture_clone = cid_capture.clone();
        config.initial_dst_cid_provider(Arc::new(move || {
            // Generate a random CID (Quinn's default behavior), unless the
            // client qlog already drew the one its file is named after.
            let cid = preset_cid.unwrap_or_else(random_initial_dst_cid);
            *cid_capture_clone.lock().unwrap() = Some(cid);
            cid
        }));

        let host = match url.host().context("missing host")? {
            url::Host::Domain(d) => d.to_string(),
            url::Host::Ipv4(ip) => ip.to_string(),
            url::Host::Ipv6(ip) => ip.to_string(), // No brackets
        };
        let port = url.port().unwrap_or(443);

        // Look up the DNS entry and filter by socket address family.
        let addr = match socket_addr {
            Some(addr) => addr,
            None => {
                // Default DNS resolution logic
                self.resolve_dns(&host, port, self.address_family()?)
                    .await?
            }
        };

        let connection = self.quic.connect_with(config, addr, &host)?.await?;

        // Extract the CID that was used
        let connection_id_hex = cid_capture
            .lock()
            .unwrap()
            .as_ref()
            .context("CID not captured")?
            .to_string();

        let (session, transport) = match url.scheme() {
            "https" => {
                let moqt_protocol = std::str::from_utf8(moq_transport::setup::ALPN)
                    .context("invalid MoQT ALPN")?
                    .to_string();
                let request = web_transport_quinn::proto::ConnectRequest::new(url.clone())
                    .with_protocol(moqt_protocol);
                (
                    web_transport_quinn::Session::connect(connection, request).await?,
                    Transport::WebTransport,
                )
            }
            "moqt" => (
                web_transport_quinn::Session::raw(
                    connection,
                    url.clone(),
                    web_transport_quinn::proto::ConnectResponse::default(),
                ),
                Transport::RawQuic,
            ),
            _ => unreachable!(),
        };

        Ok((session.into(), connection_id_hex, transport))
    }

    /// Default DNS resolution logic that filters results by address family.
    async fn resolve_dns(
        &self,
        host: &str,
        port: u16,
        address_family: AddressFamily,
    ) -> anyhow::Result<net::SocketAddr> {
        let local_addr = self.local_addr()?;

        // Collect all DNS results
        let addrs: Vec<net::SocketAddr> = match Self::parse_socket_addr(host, port) {
            Ok(addr) => {
                vec![addr]
            }
            Err(_) => tokio::net::lookup_host((host, port))
                .await
                .context("failed DNS lookup")?
                .collect(),
        };

        if addrs.is_empty() {
            anyhow::bail!("DNS lookup for host '{}' returned no addresses", host);
        }

        // Log all DNS results for debugging
        tracing::debug!(
            "DNS lookup for {}, family {:?}: found {} results",
            host,
            address_family,
            addrs.len()
        );
        for (i, addr) in addrs.iter().enumerate() {
            tracing::debug!(
                "  DNS[{}]: {} ({})",
                i,
                addr,
                if addr.is_ipv4() { "IPv4" } else { "IPv6" }
            );
        }

        // Filter DNS results to match our local socket's address family
        let compatible_addr = match address_family {
            AddressFamily::Ipv4 => {
                // IPv4 socket: filter to IPv4 addresses
                addrs
                    .iter()
                    .find(|a| a.is_ipv4())
                    .cloned()
                    .context(format!(
                        "No IPv4 address found for host '{}' (local socket is IPv4: {})",
                        host, local_addr
                    ))?
            }
            AddressFamily::Ipv6DualStack => {
                // Dual-stack socket: any address family works, use first result
                tracing::debug!("Using first DNS result (IPv6 dual-stack): {}", addrs[0]);
                addrs[0]
            }
            AddressFamily::Ipv6 => {
                // IPv6-only socket: filter to IPv6 addresses
                addrs
                    .iter()
                    .find(|a| a.is_ipv6())
                    .cloned()
                    .context(format!(
                        "No IPv6 address found for host '{}' (local socket is IPv6: {})",
                        host, local_addr
                    ))?
            }
        };

        tracing::debug!(
            "Connecting from {} to {} (selected from {} DNS results)",
            local_addr,
            compatible_addr,
            addrs.len()
        );

        Ok(compatible_addr)
    }

    fn parse_socket_addr(host: &str, port: u16) -> Result<net::SocketAddr, net::AddrParseError> {
        let host = format!("{}:{}", host, port);
        host.parse::<net::SocketAddr>()
    }
}

/// A random 16-byte initial destination CID (Quinn's default behavior).
fn random_initial_dst_cid() -> quinn::ConnectionId {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let random_bytes: [u8; 16] = rng.gen();
    quinn::ConnectionId::new(&random_bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Installing a pass-through socket wrapper must still produce a working
    /// endpoint. Exercises the `socket_wrapper` branch of `Endpoint::new`,
    /// including `wrap_udp_socket` and `new_with_abstract_socket`, and verifies
    /// the wrapper closure is actually invoked.
    #[tokio::test]
    async fn socket_wrapper_passthrough_builds_endpoint() {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind UDP socket");

        // Client-only TLS config: no cert/key, so `tls.server` is `None` and
        // `Endpoint::new` builds a client without needing certificates.
        let tls = tls::Args {
            disable_verify: true,
            ..Default::default()
        }
        .load()
        .expect("load client TLS config");

        let called = Arc::new(AtomicBool::new(false));
        let called_in_wrapper = called.clone();
        let config = Config::with_socket(socket, None, tls).with_socket_wrapper(move |inner| {
            called_in_wrapper.store(true, Ordering::SeqCst);
            inner // pass through unchanged
        });

        let endpoint = Endpoint::new(config).expect("endpoint builds with wrapper");

        assert!(
            called.load(Ordering::SeqCst),
            "the socket wrapper closure should have been invoked"
        );
        assert!(
            endpoint.server.is_none(),
            "client-only TLS config should yield no server"
        );
    }

    fn fixture(name: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../moq-relay-ietf/tests/fixtures")
            .join(name)
    }

    fn test_tls(server: bool) -> tls::Config {
        let mut args = tls::Args {
            disable_verify: true,
            ..Default::default()
        };
        if server {
            args.cert = vec![fixture("localhost-test-only.crt")];
            args.key = vec![fixture("localhost-test-only.key")];
        }
        args.load().expect("test TLS config")
    }

    /// Without the new builders a `Config` keeps today's behaviour: BBR and
    /// no client qlog. The builders set exactly their own field.
    #[test]
    fn congestion_defaults_to_bbr_and_client_qlog_is_opt_in() {
        assert_eq!(CongestionChoice::default(), CongestionChoice::Bbr);
        assert_eq!(CongestionChoice::Bbr.as_str(), "bbr");
        assert_eq!(CongestionChoice::Cubic.to_string(), "cubic");

        let config = Config::new("127.0.0.1:0".parse().unwrap(), None, test_tls(false)).unwrap();
        assert_eq!(config.congestion, CongestionChoice::Bbr);
        assert!(!config.client_qlog);
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let config = Config::with_socket(socket, None, test_tls(false));
        assert_eq!(config.congestion, CongestionChoice::Bbr);
        assert!(!config.client_qlog);

        let config = config
            .with_congestion(CongestionChoice::Cubic)
            .with_client_qlog(true);
        assert_eq!(config.congestion, CongestionChoice::Cubic);
        assert!(config.client_qlog);
    }

    /// First `congestion_window` value recorded in a qlog file, once the
    /// buffered writer has flushed it (the stream is flushed when the
    /// connection state is dropped, so poll).
    async fn first_cwnd(path: &std::path::Path) -> (u64, String) {
        let deadline = time::Instant::now() + time::Duration::from_secs(10);
        loop {
            let text = std::fs::read_to_string(path).unwrap_or_default();
            if let Some(at) = text.find("\"congestion_window\":") {
                let digits: String = text[at + "\"congestion_window\":".len()..]
                    .chars()
                    .take_while(|c| c.is_ascii_digit())
                    .collect();
                return (digits.parse().expect("cwnd value"), text);
            }
            assert!(
                time::Instant::now() < deadline,
                "no congestion_window in {}",
                path.display()
            );
            tokio::time::sleep(time::Duration::from_millis(50)).await;
        }
    }

    /// Loopback (127.0.0.1, raw QUIC): the selected controller reaches both
    /// per-connection qlog transport configs, visible as the initial window in
    /// the first metrics_updated (quinn 0.11.13: BBR 200 x 1200 = 240000 B,
    /// CUBIC 14720 clamped to 10 x 1200 = 12000 B). The client qlog exists only
    /// with `with_client_qlog(true)`, is named by the CID `connect` returns
    /// (the server names its file by the same CID), and records the
    /// CLOCK_MONOTONIC anchor of its relative times.
    async fn loopback_qlog(congestion: CongestionChoice, client_qlog: bool) {
        let dir = std::env::temp_dir().join(format!(
            "moq-native-qlog-{}-{}-{}",
            congestion,
            client_qlog,
            std::process::id()
        ));
        std::fs::create_dir(&dir).expect("fresh qlog dir");

        let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let addr = socket.local_addr().unwrap();
        let mut server_endpoint = Endpoint::new(
            Config::with_socket(socket, Some(dir.clone()), test_tls(true))
                .with_congestion(congestion),
        )
        .unwrap();
        let mut server = server_endpoint.server.take().expect("server");
        let accept = tokio::spawn(async move { server.accept().await.expect("inbound session") });

        let mut client_config =
            Config::new("127.0.0.1:0".parse().unwrap(), Some(dir.clone()), test_tls(false))
                .unwrap()
                .with_congestion(congestion);
        if client_qlog {
            client_config = client_config.with_client_qlog(true);
        }
        let client_endpoint = Endpoint::new(client_config).unwrap();
        let url = Url::parse(&format!("moqt://{addr}")).unwrap();
        let (client_session, client_cid, _) = client_endpoint
            .client
            .connect(&url, None)
            .await
            .expect("connect");
        let (server_session, server_cid, _) =
            tokio::time::timeout(time::Duration::from_secs(10), accept)
                .await
                .expect("accept in time")
                .unwrap();
        assert_eq!(client_cid, server_cid, "both ends name the connection by one CID");
        drop((client_session, server_session, client_endpoint, server_endpoint));

        let expected = match congestion {
            CongestionChoice::Bbr => 240_000,
            CongestionChoice::Cubic => 12_000,
        };
        let (server_cwnd, _) = first_cwnd(&dir.join(format!("{server_cid}_server.qlog"))).await;
        assert_eq!(server_cwnd, expected, "server qlog controller ({congestion})");

        let client_path = dir.join(format!("{client_cid}_client.qlog"));
        if client_qlog {
            let (client_cwnd, text) = first_cwnd(&client_path).await;
            assert_eq!(client_cwnd, expected, "client qlog controller ({congestion})");
            assert!(text.contains("\"title\":\"moq-client\""));
            assert!(
                text.contains(&format!("congestion={congestion} start_time_clock_monotonic_us=")),
                "anchor in the client qlog description"
            );
        } else {
            assert!(
                std::fs::symlink_metadata(&client_path).is_err(),
                "no client qlog without with_client_qlog"
            );
            let names: Vec<_> = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().file_name()).collect();
            assert_eq!(names.len(), 1, "only the server qlog: {names:?}");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn loopback_qlog_bbr_default_server_only() {
        loopback_qlog(CongestionChoice::Bbr, false).await;
    }

    #[tokio::test]
    async fn loopback_qlog_bbr_with_client_qlog() {
        loopback_qlog(CongestionChoice::Bbr, true).await;
    }

    #[tokio::test]
    async fn loopback_qlog_cubic_with_client_qlog() {
        loopback_qlog(CongestionChoice::Cubic, true).await;
    }
}
