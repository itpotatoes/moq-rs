// Skew experiment — MoQ spike.
//
// Proves the five spike gate items for the paper's "divergence extreme" (MoQ) arm,
// using cloudflare/moq-rs (draft-ietf-moq-transport-16) as the transport:
//   1. cargo build succeeds (workspace member).
//   2. single QUIC connection carries TWO tracks (pc + haptic) pub->relay->sub.
//   3. receive callback exposes raw payload bytes -> we parse the 32B skew header.
//   4. per-track priority is settable (subgroup priority, u8).
//   5. observation path: relay writes qlog/mlog; SSLKEYLOGFILE honored by client.
//
// Topology: publisher session + subscriber session, both to a local moq-relay-ietf.
// This mirrors the Phase 3 deployment (tc shaping goes on the relay link) and this
// binary is the seed for moq_sender / moq_receiver.
//
// Wire header is byte-identical to skew_logging.py: "<BBHIQIQI", 32 bytes, LE.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use moq_native_ietf::{quic, tls};
use moq_transport::{
    coding::TrackNamespace,
    serve::{TrackReaderMode, Tracks},
    session::Session,
};
use url::Url;

const NS: &str = "skew/spike";
const HDR: usize = 32;

// One track descriptor: id, name, priority, object count, body size, pacing.
struct TrackPlan {
    track_id: u8,
    name: &'static str,
    priority: u8, // lower = higher urgency (QUIC send priority)
    count: u32,
    body: usize,
    pace: Duration,
}

fn now_us() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as u64
}

// Pack the 32B skew header: "<BBHIQIQI" = version, track_id, tier, seq, pts_us,
// event_id, gen_ts_us, payload_len.
fn pack_header(track_id: u8, tier: u16, seq: u32, pts_us: u64, event_id: u32, payload_len: u32) -> [u8; HDR] {
    let mut b = [0u8; HDR];
    b[0] = 1; // version
    b[1] = track_id;
    b[2..4].copy_from_slice(&tier.to_le_bytes());
    b[4..8].copy_from_slice(&seq.to_le_bytes());
    b[8..16].copy_from_slice(&pts_us.to_le_bytes());
    b[16..20].copy_from_slice(&event_id.to_le_bytes());
    b[20..28].copy_from_slice(&now_us().to_le_bytes());
    b[28..32].copy_from_slice(&payload_len.to_le_bytes());
    b
}

struct ParsedHdr {
    track_id: u8,
    seq: u32,
    event_id: u32,
    payload_len: u32,
}

fn parse_header(buf: &[u8]) -> Option<ParsedHdr> {
    if buf.len() < HDR {
        return None;
    }
    Some(ParsedHdr {
        track_id: buf[1],
        seq: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
        event_id: u32::from_le_bytes(buf[16..20].try_into().unwrap()),
        payload_len: u32::from_le_bytes(buf[28..32].try_into().unwrap()),
    })
}

async fn connect(relay: &Url) -> Result<(web_transport::Session, moq_transport::session::Transport)> {
    let tls_args = tls::Args {
        disable_verify: true,
        ..Default::default()
    };
    let tls = tls_args.load()?;
    let bind: SocketAddr = "[::]:0".parse().unwrap();
    let quic = quic::Endpoint::new(quic::Config::new(bind, None, tls)?)?;
    let (session, _cid, transport) = quic.client.connect(relay, None).await?;
    Ok((session, transport))
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,quinn=warn,moq_transport=warn")),
        )
        .init();

    let relay: Url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "https://localhost:4443".to_string())
        .parse()
        .context("relay url")?;

    // pc: 8000B body, 30 fps pacing; haptic: 160B body, 100 Hz pacing, highest priority.
    let plans = vec![
        TrackPlan { track_id: 0, name: "pc",     priority: 1, count: 60,  body: 8000, pace: Duration::from_millis(16) },
        TrackPlan { track_id: 1, name: "haptic", priority: 0, count: 200, body: 160,  pace: Duration::from_millis(5) },
    ];

    println!("== skew-moq-spike ==  relay={relay}");
    for p in &plans {
        println!("  track {:>6} id={} priority={} count={} body={}B", p.name, p.track_id, p.priority, p.count, p.body);
    }

    // Two sessions over the ONE relay: one publisher, one subscriber.
    let (pub_sess, pub_tp) = connect(&relay).await.context("publisher connect")?;
    let (pub_session, mut publisher, _p_sub) =
        Session::connect(pub_sess, None, pub_tp).await.context("publisher SETUP")?;

    let (sub_sess, sub_tp) = connect(&relay).await.context("subscriber connect")?;
    let (sub_session, _s_pub, mut subscriber) =
        Session::connect(sub_sess, None, sub_tp).await.context("subscriber SETUP")?;

    // Drive both session IO loops from the start; control-message exchanges
    // (PUBLISH_OK / SUBSCRIBE_OK) below depend on them running.
    let mut pub_run = tokio::spawn(pub_session.run());
    let mut sub_run = tokio::spawn(sub_session.run());

    let namespace = TrackNamespace::from_utf8_path(NS);

    // Received-object counters + one "bad header" counter, per track index.
    let recv_counts: Vec<Arc<AtomicU64>> = plans.iter().map(|_| Arc::new(AtomicU64::new(0))).collect();
    let bad_counts: Vec<Arc<AtomicU64>> = plans.iter().map(|_| Arc::new(AtomicU64::new(0))).collect();

    let total_target: u64 = plans.iter().map(|p| p.count as u64).sum();

    // ---- Publisher side: announce namespace, create tracks, write objects. ----
    // The relay routes an inbound SUBSCRIBE to the namespace publisher, which
    // serves whatever track the TracksWriter has created. So we must announce +
    // create tracks BEFORE the subscriber subscribes.
    let (mut pub_tracks_w, _preq, pub_tracks_r) = Tracks::new(namespace.clone()).produce();
    let mut write_tasks = Vec::new();
    for p in &plans {
        let writer = pub_tracks_w
            .create(p.name)
            .ok_or_else(|| anyhow::anyhow!("create pub track {}", p.name))?;

        // Writer task: one subgroup, N sequential objects carrying header+body.
        let track_id = p.track_id;
        let priority = p.priority;
        let count = p.count;
        let body = p.body;
        let pace = p.pace;
        write_tasks.push(tokio::spawn(async move {
            let mut subgroups = writer.subgroups().context("subgroups mode")?;
            let mut sg = subgroups.append(priority).context("append subgroup")?;
            for seq in 0..count {
                let mut buf = Vec::with_capacity(HDR + body);
                let hdr = pack_header(track_id, /*tier*/ 8, seq, /*pts*/ now_us(), /*event_id*/ seq + 1, body as u32);
                buf.extend_from_slice(&hdr);
                buf.resize(HDR + body, (seq & 0xff) as u8);
                sg.write(Bytes::from(buf)).context("write object")?;
                tokio::time::sleep(pace).await;
            }
            drop(sg);
            drop(subgroups);
            Ok::<(), anyhow::Error>(())
        }));
    }
    // Announce the namespace (serves subscribes on demand) in the background.
    let mut ns_publisher = publisher.clone();
    let mut ns_task = tokio::spawn(async move { ns_publisher.publish_namespace(pub_tracks_r).await });

    // Give the relay a moment to register the PUBLISH_NAMESPACE before subscribing.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // ---- Subscriber side: subscribe each track, spawn drain readers. ----
    let (mut sub_tracks, _req, mut sub_reader) = Tracks::new(namespace.clone()).produce();
    let mut sub_handles = Vec::new(); // keep Subscribe handles alive
    let mut drain_tasks = Vec::new();
    for (i, p) in plans.iter().enumerate() {
        let sub_track = sub_tracks.create(p.name).ok_or_else(|| anyhow::anyhow!("create sub track"))?;
        let received = sub_reader
            .get_track_reader(&namespace, p.name)
            .ok_or_else(|| anyhow::anyhow!("get_track_reader"))?;
        let sub = subscriber.subscribe_open(sub_track).await.context("subscribe_open")?;
        sub_handles.push(sub);

        let count = recv_counts[i].clone();
        let bad = bad_counts[i].clone();
        let want_id = p.track_id;
        drain_tasks.push(tokio::spawn(async move {
            let mode = received.mode().await?;
            let mut subgroups = match mode {
                TrackReaderMode::Subgroups(s) => s,
                _ => bail!("non-subgroup delivery"),
            };
            while let Some(mut sg) = subgroups.next().await? {
                while let Some(obj) = sg.read_next().await? {
                    match parse_header(&obj) {
                        Some(h) if h.track_id == want_id && (h.payload_len as usize) == obj.len() - HDR => {
                            count.fetch_add(1, Ordering::Relaxed);
                        }
                        _ => {
                            bad.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        }));
    }

    // ---- Wait for roundtrip completion (sessions already running). ----
    let start = Instant::now();
    let progress = {
        let recv_counts = recv_counts.clone();
        async move {
            loop {
                let got: u64 = recv_counts.iter().map(|c| c.load(Ordering::Relaxed)).sum();
                if got >= total_target {
                    return Ok::<(), anyhow::Error>(());
                }
                if start.elapsed() > Duration::from_secs(20) {
                    bail!("timeout: only {got}/{total_target} objects round-tripped");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    };

    let result = tokio::select! {
        r = progress => r,
        r = &mut pub_run => match r { Ok(Ok(_)) => bail!("publisher session ended early"), Ok(Err(e)) => Err(e).context("publisher session"), Err(e) => bail!("publisher task: {e}") },
        r = &mut sub_run => match r { Ok(Ok(_)) => bail!("subscriber session ended early"), Ok(Err(e)) => Err(e).context("subscriber session"), Err(e) => bail!("subscriber task: {e}") },
        r = &mut ns_task => match r { Ok(Ok(_)) => bail!("publish_namespace ended early"), Ok(Err(e)) => Err(e).context("publish_namespace"), Err(e) => bail!("ns task: {e}") },
    };
    pub_run.abort();
    sub_run.abort();
    ns_task.abort();

    let elapsed = start.elapsed();

    // Join writer tasks (should be done); abort drains/serves.
    for t in write_tasks { let _ = t.await; }

    println!("\n== result ({:.2}s) ==", elapsed.as_secs_f64());
    let mut all_ok = result.is_ok();
    for (i, p) in plans.iter().enumerate() {
        let got = recv_counts[i].load(Ordering::Relaxed);
        let bad = bad_counts[i].load(Ordering::Relaxed);
        let ok = got == p.count as u64 && bad == 0;
        all_ok &= ok;
        println!(
            "  track {:>6} id={} prio={} : sent={} received={} bad_hdr={}  {}",
            p.name, p.track_id, p.priority, p.count, got, bad, if ok { "OK" } else { "FAIL" }
        );
    }

    if let Err(e) = &result {
        println!("  progress: {e:#}");
    }
    if all_ok {
        println!("\nSPIKE PASS: 2 tracks, 1 connection, header bytes parsed, priorities set.");
        Ok(())
    } else {
        bail!("SPIKE FAIL");
    }
}
