//! Bounded, non-paced diagnostic: explicit source object abort versus source session close.
//! The receiver records the observed error, never infers a cancellation reason from Size.
use std::{fs::{File, OpenOptions}, io::Write, path::{Path, PathBuf}, sync::{Arc, Mutex},
    sync::atomic::{AtomicUsize, Ordering}, time::Duration};
use anyhow::{Context, Result, ensure, bail};
use bytes::Bytes;
use clap::Parser;
use moq_native_ietf::{quic, tls};
use moq_transport::{coding::TrackNamespace, serve::{Tracks, TrackReaderMode, ServeError},
    session::{Session, SessionConfig, DataPriorityMapping}};
use serde_json::json;
use skew_moq::{now_us, pack_header, unpack_header};
use url::Url;

#[derive(Parser)]
struct Args {
    #[arg(long, value_parser=["tx", "rx"])] role: String,
    #[arg(long, value_parser=["object_abort", "session_close"])] fault: String,
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
    let size = if pc {1000} else {2*(((seq+1)*8000/90)-seq*8000/90)};
    let event = if pc {seq+1} else if seq%3==0 {seq/3+1} else {0};
    let pts = u64::from(seq)*1_000_000/if pc {30} else {90};
    let mut data = pack_header(if pc {0} else {1}, if pc {2} else {0}, seq,
        pts, event, now_us(), size).to_vec();
    data.resize(32+size as usize, seq as u8);
    data
}
fn generated(output: &Log, track: &str, seq: u32, data: &[u8], submitted: usize) -> Result<()> {
    let h = unpack_header(data).context("generated header")?;
    log(output, json!({"role":"generated", "track":track, "seq":seq,
        "group_id":if track=="pc" {seq} else {0}, "subgroup_id":0,
        "object_id":if track=="pc" {0} else {seq}, "size":data.len(),
        "t_gen":h.gen_ts_us, "pts_us":h.pts_us, "event_id":h.event_id,
        "submitted_bytes":submitted}))
}
async fn work(args: &Args, output: Log, tasks: &mut Tasks) -> Result<()> {
    let tls = tls::Args {disable_verify:true, ..Default::default()}.load()?;
    let endpoint = quic::Endpoint::new(quic::Config::new("0.0.0.0:0".parse()?, None, tls)?)?;
    let (connection, _, transport) = endpoint.client.connect(&args.relay, None).await?;
    let close_handle = connection.clone();
    let (session, mut publisher, mut subscriber) = Session::connect_with_config(
        connection, None, transport, SessionConfig {
            data_priority_mapping:DataPriorityMapping::MoqtV2, ..Default::default()
        }).await?;
    tasks.push(tokio::spawn(async move {session.run().await.map_err(Into::into)}));
    let namespace = TrackNamespace::from_utf8_path(&args.run_id);
    let (mut tracks, _requests, mut readers) = Tracks::new(namespace.clone()).produce();
    if args.role == "tx" {
        let mut pc = tracks.create("pc").context("pc")?.subgroups()?;
        let mut haptic = tracks.create("haptic").context("haptic")?.subgroups()?;
        tasks.push(tokio::spawn(async move {publisher.publish_namespace(readers).await.map_err(Into::into)}));
        tokio::time::sleep(Duration::from_millis(300)).await;
        mark(&args.directory, "tx_started")?;
        wait_mark(&args.directory, "rx_ready").await?;
        let mut hg = haptic.append(0)?;
        for seq in 0..6 {
            let data = payload("haptic", seq);
            hg.write(Bytes::copy_from_slice(&data))?;
            generated(&output, "haptic", seq, &data, data.len())?;
        }
        drop(hg);
        let data = payload("pc", 0);
        pc.append(1)?.write(Bytes::copy_from_slice(&data))?;
        generated(&output, "pc", 0, &data, data.len())?;
        let data = payload("pc", 1);
        let mut group = pc.append(1)?;
        let mut object = group.create(data.len(), None)?;
        object.write(Bytes::copy_from_slice(&data[..532]))?;
        generated(&output, "pc", 1, &data, 532)?;
        // Do not race interruption against initial QUIC payload delivery.
        wait_mark(&args.directory, "partial_observed").await?;
        wait_mark(&args.directory, "controls_observed").await?;
        log(&output, json!({"role":"fault_requested", "kind":args.fault,
            "track":"pc", "seq":1, "t_us":now_us(),
            "api":if args.fault=="object_abort" {"SubgroupObjectWriter::abort(Cancel)"}
                  else {"web_transport::Session::close"},
            "close_code":if args.fault=="session_close" {Some(0x51u32)} else {None}}))?;
        if args.fault == "object_abort" {
            object.abort(ServeError::Cancel)?;
            log(&output, json!({"role":"fault_applied", "kind":args.fault, "t_us":now_us()}))?;
        } else {
            // Hold the incomplete writer alive until the RX terminal record exists:
            // this distinguishes transport close from implicit writer-drop Size.
            close_handle.close(0x51, "event-pair diagnostic source close");
            log(&output, json!({"role":"fault_applied", "kind":args.fault, "t_us":now_us()}))?;
            log(&output, json!({"role":"connection_closed", "t_us":now_us(),
                "detail":format!("{:?}", close_handle.closed().await)}))?;
            wait_mark(&args.directory, "rx_done").await?;
            drop(object);
        }
        wait_mark(&args.directory, "rx_done").await?;
        drop(group);
        mark(&args.directory, "tx_done")?;
        drop(pc); drop(haptic);
    } else {
        let complete = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(AtomicUsize::new(0));
        let mut subscriptions = Vec::new();
        for name in ["pc", "haptic"] {
            let writer = tracks.create(name).context("receiver track")?;
            let reader = readers.get_track_reader(&namespace, name).context("track reader")?;
            // No delivery timeout in either interruption scenario.
            subscriptions.push(subscriber.subscribe_open(writer).await?);
            let output=output.clone(); let directory=args.directory.clone();
            let complete=complete.clone(); let done=done.clone();
            tasks.push(tokio::spawn(async move {
                let mut groups = match reader.mode().await? {
                    TrackReaderMode::Subgroups(groups) => groups,
                    _ => bail!("unexpected mode"),
                };
                let target=if name=="pc" {2} else {6}; let mut count=0;
                while count<target {
                    let mut group=groups.next().await?.context("early track end")?;
                    while let Some(mut object)=group.next().await? {
                        let (gid,sgid,oid,size)=(object.group_id,object.subgroup_id,object.object_id,object.size);
                        let mut data=Vec::new(); let mut partial_marked=false;
                        let error=loop {
                            match object.read().await {
                                Ok(Some(chunk)) => {
                                    data.extend_from_slice(&chunk);
                                    if name=="pc" && gid==1 && data.len()>=532 && !partial_marked {
                                        log(&output,json!({"role":"partial_observed", "track":name,
                                            "seq":1, "received_bytes":data.len(), "t_us":now_us()}))?;
                                        mark(&directory,"partial_observed")?; partial_marked=true;
                                    }
                                },
                                Ok(None) => break None,
                                Err(error) => break Some(error),
                            }
                        };
                        let h=unpack_header(&data).context("missing header")?;
                        ensure!(h.version==1 && h.track_id==if name=="pc" {0} else {1},"header mismatch");
                        ensure!(size==32+h.payload_len as usize,"size mismatch");
                        ensure!(data[32..].iter().all(|b| *b==h.seq as u8),"payload corruption");
                        let fault=name=="pc" && h.seq==1;
                        if fault {
                            ensure!(error.is_some() && data.len()==532,"missing partial interruption");
                            ensure!(!matches!(error,Some(ServeError::Closed(0x2))),"unexpected delivery timeout");
                        } else {ensure!(error.is_none() && data.len()==size,"control failed");}
                        log(&output,json!({"role":"terminal", "track":name, "seq":h.seq,
                            "group_id":gid, "subgroup_id":sgid, "object_id":oid, "size":size,
                            "pts_us":h.pts_us, "event_id":h.event_id, "t_gen":h.gen_ts_us,
                            "t_terminal":now_us(), "received_bytes":data.len(),
                            "outcome":if fault {"interrupted"} else {"complete"},
                            "observed_error":error.as_ref().map(|e|format!("{e:?}")),
                            "reset_code":match error {Some(ServeError::Closed(code))=>Some(code),_=>None}}))?;
                        count+=1;
                        if !fault && complete.fetch_add(1,Ordering::SeqCst)+1==7 {
                            mark(&directory,"controls_observed")?;
                        }
                        if fault || count==target {break;}
                    }
                }
                done.fetch_add(1,Ordering::SeqCst); Ok(())
            }));
        }
        mark(&args.directory,"rx_ready")?;
        tokio::time::timeout(Duration::from_secs(5),async {
            while done.load(Ordering::SeqCst)!=2 {tokio::time::sleep(Duration::from_millis(10)).await;}
        }).await.context("receive outcomes incomplete")?;
        mark(&args.directory,"rx_done")?;
        wait_mark(&args.directory,"tx_done").await?;
        drop(subscriptions);
    }
    Ok(())
}
#[tokio::main]
async fn main() -> Result<()> {
    let args=Args::parse();
    let output=Arc::new(Mutex::new(OpenOptions::new().write(true).create_new(true)
        .open(args.directory.join(format!("{}_probe.jsonl",args.role)))?));
    log(&output,json!({"role":"meta", "schema":"event-pair-quic-interrupt-v1",
        "run_id":args.run_id, "side":args.role, "fault":args.fault,
        "clock":"monotonic_ns/1000", "pc_delivery_timeout_ms":null,
        "planned_pc":2, "planned_haptic":6, "evidence_class":"verification_only_not_paced"}))?;
    let mut tasks=Vec::new();
    let result=tokio::time::timeout(Duration::from_secs(12),work(&args,output.clone(),&mut tasks))
        .await.context("probe outer timeout").and_then(|r|r);
    let mut joined=true;
    for task in tasks {
        task.abort();
        match tokio::time::timeout(Duration::from_secs(2),task).await {
            Ok(Ok(result))=>log(&output,json!({"role":"task_end","detail":format!("{result:?}")}))?,
            Ok(Err(error)) if error.is_cancelled()=>{},
            _=>joined=false,
        }
    }
    log(&output,json!({"role":"shutdown","work_ok":result.is_ok(),"tasks_joined":joined}))?;
    ensure!(joined,"task join failure"); result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn planned_headers_and_pairing() {
        for seq in 0..2 {
            let p=unpack_header(&payload("pc",seq)).unwrap();
            let h=unpack_header(&payload("haptic",seq*3)).unwrap();
            assert_eq!((p.event_id,p.pts_us),(h.event_id,h.pts_us));
            assert_eq!(p.event_id,seq+1); assert_eq!(p.payload_len,1000);
        }
    }
    #[test]
    fn partial_object_is_not_complete() {
        let p=payload("pc",1);
        assert_eq!(p.len(),1032); assert_eq!(unpack_header(&p[..532]).unwrap().seq,1);
        assert_eq!(p[32..532],vec![1;500]);
    }
}
