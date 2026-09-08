//! Diagnostic only: real QUIC, 3 PC/9 haptic opportunities, one partial PC stall.
//! Not a paced workload, quality experiment or efficacy benchmark.
use std::{fs::{File, OpenOptions}, io::Write, path::{Path, PathBuf}, sync::{Arc, Mutex},
          sync::atomic::{AtomicUsize, Ordering}, time::Duration};
use anyhow::{Context, Result, ensure, bail};
use bytes::Bytes;
use clap::Parser;
use moq_native_ietf::{quic, tls};
use moq_transport::{coding::{TrackNamespace, KeyValuePairs},
    serve::{Tracks, TrackReaderMode, ServeError},
    session::{Session, SessionConfig, DataPriorityMapping}};
use serde_json::json;
use skew_moq::{now_us, pack_header, unpack_header};
use url::Url;

#[derive(Parser)]
struct Args {
    #[arg(long, value_parser = ["tx", "rx"])] role: String,
    #[arg(long)] relay: Url,
    #[arg(long)] run_id: String,
    #[arg(long)] directory: PathBuf,
}
type Log = Arc<Mutex<File>>;
type Tasks = Vec<tokio::task::JoinHandle<Result<()>>>;
fn log(output: &Log, value: serde_json::Value) -> Result<()> {
    let mut file = output.lock().map_err(|_| anyhow::anyhow!("log poisoned"))?;
    writeln!(file, "{value}")?;
    file.flush()?;
    Ok(())
}
fn mark(directory: &Path, name: &str) -> Result<()> {
    let mut file = OpenOptions::new().write(true).create_new(true).open(directory.join(name))?;
    writeln!(file, "{}", now_us())?;
    Ok(())
}
async fn wait_mark(directory: &Path, name: &str) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !directory.join(name).exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.context(format!("waiting for {name}"))?;
    Ok(())
}
fn payload(track: &str, seq: u32) -> Vec<u8> {
    let pc = track == "pc";
    let size = if pc { 1000 } else { 2 * (((seq+1)*8000/90) - seq*8000/90) };
    let event = if pc { seq+1 } else if seq%3 == 0 {seq/3+1} else {0};
    let pts = u64::from(seq)*1_000_000/if pc {30} else {90};
    let header = pack_header(if pc {0} else {1}, if pc {2} else {0}, seq, pts, event, now_us(), size);
    let mut data = header.to_vec();
    data.resize(32 + size as usize, seq as u8);
    data
}

async fn work(args: &Args, output: Log, tasks: &mut Tasks) -> Result<()> {
    let tls = tls::Args { disable_verify: true, ..Default::default() }.load()?;
    let endpoint = quic::Endpoint::new(quic::Config::new("0.0.0.0:0".parse()?, None, tls)?)?;
    let (connection, _, transport) = endpoint.client.connect(&args.relay, None).await?;
    let (session, mut publisher, mut subscriber) = Session::connect_with_config(
        connection, None, transport, SessionConfig {
            data_priority_mapping: DataPriorityMapping::MoqtV2, ..Default::default()
        }).await?;
    tasks.push(tokio::spawn(async move { session.run().await.map_err(Into::into) }));
    let namespace = TrackNamespace::from_utf8_path(&args.run_id);
    let (mut tracks, _requests, mut readers) = Tracks::new(namespace.clone()).produce();
    if args.role == "tx" {
        let mut pc = tracks.create("pc").context("pc track")?.subgroups()?;
        let mut haptic = tracks.create("haptic").context("haptic track")?.subgroups()?;
        tasks.push(tokio::spawn(async move { publisher.publish_namespace(readers).await.map_err(Into::into) }));
        // Registration settle time only; data waits for actual SUBSCRIBE_OK on both tracks.
        tokio::time::sleep(Duration::from_millis(300)).await;
        mark(&args.directory, "tx_started")?;
        wait_mark(&args.directory, "rx_ready").await?;
        let mut haptic_group = haptic.append(0)?;
        for seq in 0..9 {
            let data = payload("haptic", seq);
            let header = unpack_header(&data).context("generated header")?;
            haptic_group.write(Bytes::from(data.clone()))?;
            log(&output, json!({"role":"generated", "track":"haptic", "seq":seq,
                "group_id":0, "subgroup_id":0, "object_id":seq, "size":data.len(),
                "t_gen":header.gen_ts_us, "submitted_bytes":data.len()}))?;
        }
        drop(haptic_group);
        for seq in 0..3 {
            let data = payload("pc", seq);
            let header = unpack_header(&data).context("generated header")?;
            let mut group = pc.append(1)?;
            let mut object = group.create(data.len(), None)?;
            if seq == 1 {
                // 32-byte application header + 500 bytes of 1000-byte PC body.
                object.write(Bytes::copy_from_slice(&data[..532]))?;
                log(&output, json!({"role":"fault_injected", "track":"pc", "seq":seq,
                    "kind":"hold_partial_until_receiver_reset", "submitted_bytes":532,
                    "expected_bytes":data.len(), "t_us":now_us()}))?;
                wait_mark(&args.directory, "reset_observed").await?;
                // Finish upstream *after* downstream expiry; do not reopen the expired subgroup.
                object.write(Bytes::copy_from_slice(&data[532..]))?;
            } else {
                object.write(Bytes::from(data.clone()))?;
            }
            drop(object);
            drop(group);
            log(&output, json!({"role":"generated", "track":"pc", "seq":seq,
                "group_id":seq, "subgroup_id":0, "object_id":0, "size":data.len(),
                "t_gen":header.gen_ts_us, "submitted_bytes":data.len()}))?;
        }
        // Keep track writers alive until all planned receive outcomes were observed.
        wait_mark(&args.directory, "rx_done").await?;
        // Bounded diagnostic drain for the intentionally late upstream tail.
        tokio::time::sleep(Duration::from_millis(100)).await;
        mark(&args.directory, "tx_done")?;
        drop(pc);
        drop(haptic);
    } else {
        let mut subscriptions = Vec::new();
        let done = Arc::new(AtomicUsize::new(0));
        for name in ["pc", "haptic"] {
            let writer = tracks.create(name).context("receiver track")?;
            let reader = readers.get_track_reader(&namespace, name).context("track reader")?;
            let mut params = KeyValuePairs::default();
            if name == "pc" { params.set_delivery_timeout(67); }
            subscriptions.push(subscriber.subscribe_open_with_params(writer, params).await?);
            let output = output.clone();
            let directory = args.directory.clone();
            let done = done.clone();
            tasks.push(tokio::spawn(async move {
                let mut groups = match reader.mode().await? {
                    TrackReaderMode::Subgroups(groups) => groups,
                    _ => bail!("unexpected delivery mode"),
                };
                let target = if name == "pc" {3} else {9};
                let mut count = 0;
                while count < target {
                    let mut group = groups.next().await?.context("early track end")?;
                    while let Some(mut object) = group.next().await? {
                        let (gid, sgid, oid, size) = (object.group_id, object.subgroup_id, object.object_id, object.size);
                        let mut data = Vec::new();
                        let terminal = loop {
                            match object.read().await {
                                Ok(Some(chunk)) => data.extend_from_slice(&chunk),
                                Ok(None) => break "complete",
                                Err(ServeError::Closed(0x2)) => break "delivery_timeout",
                                Err(error) => return Err(error.into()),
                            }
                        };
                        let header = unpack_header(&data).context("no complete application header observed")?;
                        let seq = header.seq;
                        ensure!(header.version == 1, "header version mismatch");
                        ensure!(header.track_id == if name == "pc" {0} else {1}, "track mismatch");
                        ensure!(size == 32 + header.payload_len as usize, "declared size mismatch");
                        ensure!(data[32..].iter().all(|b| *b == seq as u8), "payload corruption");
                        let expected_terminal = if name == "pc" && seq == 1 {"delivery_timeout"} else {"complete"};
                        ensure!(terminal == expected_terminal, "unexpected terminal outcome");
                        if terminal == "complete" { ensure!(data.len() == size, "partial success"); }
                        else { ensure!(data.len() >= 32 && data.len() < size, "not a partial object"); }
                        log(&output, json!({"role":"terminal", "track":name, "seq":seq,
                            "group_id":gid, "subgroup_id":sgid, "object_id":oid, "size":size,
                            "pts_us":header.pts_us, "event_id":header.event_id, "t_gen":header.gen_ts_us,
                            "t_terminal":now_us(), "received_bytes":data.len(), "outcome":terminal,
                            "reset_code":if terminal == "delivery_timeout" {Some(2)} else {None}}))?;
                        count += 1;
                        if terminal == "delivery_timeout" { mark(&directory, "reset_observed")?; break; }
                        if count == target { break; }
                    }
                }
                done.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }));
        }
        mark(&args.directory, "rx_ready")?;
        tokio::time::timeout(Duration::from_secs(5), async {
            while done.load(Ordering::SeqCst) != 2 { tokio::time::sleep(Duration::from_millis(10)).await; }
        }).await.context("receive outcomes incomplete")?;
        mark(&args.directory, "rx_done")?;
        wait_mark(&args.directory, "tx_done").await?;
        drop(subscriptions);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let output = Arc::new(Mutex::new(OpenOptions::new().write(true).create_new(true)
        .open(args.directory.join(format!("{}_probe.jsonl", args.role)))?));
    log(&output, json!({"role":"meta", "schema":"event-pair-quic-fault-v1",
        "run_id":args.run_id, "side":args.role, "clock":"monotonic_ns/1000",
        "pc_delivery_timeout_ms":67, "planned_pc":3, "planned_haptic":9,
        "evidence_class":"verification_only_not_paced"}))?;
    let mut tasks = Vec::new();
    let result = tokio::time::timeout(Duration::from_secs(12), work(&args, output.clone(), &mut tasks))
        .await.context("probe outer timeout").and_then(|r| r);
    let mut joined = true;
    for task in tasks {
        task.abort();
        match tokio::time::timeout(Duration::from_secs(2), task).await {
            Ok(Ok(result)) => log(&output, json!({"role":"task_end", "detail":format!("{result:?}")}))?,
            Ok(Err(error)) if error.is_cancelled() => {},
            _ => joined = false,
        }
    }
    log(&output, json!({"role":"shutdown", "work_ok":result.is_ok(), "tasks_joined":joined}))?;
    ensure!(joined, "probe task join failure");
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn diagnostic_header_and_exact_anchor_pairing_are_preserved() {
        for seq in 0..3 {
            let pc = payload("pc", seq);
            let hap = payload("haptic", seq*3);
            let p = unpack_header(&pc).unwrap();
            let h = unpack_header(&hap).unwrap();
            assert_eq!(pc.len(), 1032);
            assert_eq!(p.version, 1);
            assert_eq!(p.event_id, seq+1);
            assert_eq!((p.pts_us, p.event_id), (h.pts_us, h.event_id));
            assert_eq!(p.payload_len, 1000);
        }
    }
    #[test]
    fn partial_prefix_contains_header_but_not_full_body() {
        let bytes = payload("pc", 1);
        let prefix = &bytes[..532];
        assert_eq!(unpack_header(prefix).unwrap().seq, 1);
        assert_eq!(prefix.len()-32, 500);
        assert!(prefix.len() < bytes.len());
        assert!(prefix[32..].iter().all(|b| *b == 1));
    }
}
