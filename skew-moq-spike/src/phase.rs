//! Durable runner→sender phase receipts for the registered pre-t0 workload.

use std::fs::OpenOptions;
use std::io::Read;
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::now_us;

pub const WARMUP_DURATION_US: u64 = 3_000_000;
pub const PHASE_CONTROL_LEAD_US: u64 = 250_000;
pub const WARMUP_DECISION_LEAD_US: u64 = 250_000;
pub const WARMUP_GAP_LIMIT_US: u64 = 1_000_000;
const MAX_RECEIPT_BYTES: u64 = 64 * 1024;
const POLL: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PhaseControl {
    pub schema_version: u32,
    pub batch_id: String,
    pub run_id: String,
    pub arm: String,
    pub created_us: u64,
    pub warmup_start_us: u64,
    pub t0_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct TrackEvidence {
    event_count: u64,
    first_recv_us: u64,
    last_recv_us: u64,
    max_gap_us: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct TracksEvidence {
    pc: TrackEvidence,
    haptic: TrackEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
struct WarmupPass {
    schema_version: u32,
    batch_id: String,
    run_id: String,
    arm: String,
    phase_sha256: String,
    warmup_start_us: u64,
    t0_us: u64,
    validated_at_us: u64,
    warmup_stall: bool,
    tracks: TracksEvidence,
}

fn safe_id(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

fn snapshot(metadata: &std::fs::Metadata) -> (u64, u64, u64, i64, i64, i64, i64) {
    (
        metadata.dev(),
        metadata.ino(),
        metadata.len(),
        metadata.mtime(),
        metadata.mtime_nsec(),
        metadata.ctime(),
        metadata.ctime_nsec(),
    )
}

fn read_stable(path: &Path) -> Result<Vec<u8>> {
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path)
        .with_context(|| format!("open immutable phase receipt {}", path.display()))?;
    let before = file.metadata().context("stat phase receipt before read")?;
    ensure!(
        before.file_type().is_file(),
        "phase receipt is not a regular file"
    );
    ensure!(
        (1..=MAX_RECEIPT_BYTES).contains(&before.len()),
        "phase receipt size is outside the registered bound"
    );
    let mut bytes = Vec::with_capacity(before.len() as usize);
    file.read_to_end(&mut bytes).context("read phase receipt")?;
    let after = file.metadata().context("stat phase receipt after read")?;
    ensure!(
        snapshot(&before) == snapshot(&after) && bytes.len() as u64 == before.len(),
        "phase receipt changed while being read"
    );
    Ok(bytes)
}

impl PhaseControl {
    pub fn load(path: &Path, batch_id: &str, run_id: &str, arm: &str) -> Result<Self> {
        let bytes = read_stable(path)?;
        let phase: Self = serde_json::from_slice(&bytes).context("parse phase-control JSON")?;
        ensure!(
            phase.schema_version == 1,
            "unsupported phase-control schema"
        );
        ensure!(
            safe_id(&phase.batch_id)
                && safe_id(&phase.run_id)
                && matches!(phase.arm.as_str(), "M" | "Md" | "W"),
            "unsafe phase-control identity"
        );
        ensure!(
            (
                phase.batch_id.as_str(),
                phase.run_id.as_str(),
                phase.arm.as_str()
            ) == (batch_id, run_id, arm),
            "phase-control execution identity mismatch"
        );
        ensure!(
            phase.t0_us.checked_sub(phase.warmup_start_us) == Some(WARMUP_DURATION_US),
            "warmup window must be exactly 3,000,000 us"
        );
        ensure!(
            phase
                .warmup_start_us
                .checked_sub(phase.created_us)
                .is_some_and(|lead| lead >= PHASE_CONTROL_LEAD_US),
            "phase-control lead is shorter than 250,000 us"
        );
        Ok(phase)
    }

    pub fn digest(&self) -> String {
        // Python publishes sort_keys=True canonical JSON. IDs are restricted to
        // an ASCII subset that needs no escaping, so this byte sequence is exact.
        let canonical = format!(
            "{{\"arm\":\"{}\",\"batch_id\":\"{}\",\"created_us\":{},\"run_id\":\"{}\",\"schema_version\":1,\"t0_us\":{},\"warmup_start_us\":{}}}\n",
            self.arm,
            self.batch_id,
            self.created_us,
            self.run_id,
            self.t0_us,
            self.warmup_start_us,
        );
        format!("{:x}", Sha256::digest(canonical.as_bytes()))
    }

    pub fn observed_through_us(&self) -> u64 {
        self.t0_us - WARMUP_DECISION_LEAD_US
    }
}

fn validate_track(track: &str, value: &TrackEvidence, phase: &PhaseControl) -> Result<()> {
    ensure!(
        value.event_count > 0,
        "{track} warmup pass has no receive events"
    );
    ensure!(
        phase.warmup_start_us <= value.first_recv_us
            && value.first_recv_us <= value.last_recv_us
            && value.last_recv_us <= phase.observed_through_us(),
        "{track} warmup evidence lies outside the observed window"
    );
    ensure!(
        value.max_gap_us < WARMUP_GAP_LIMIT_US,
        "{track} warmup pass carries a registered stall gap"
    );
    Ok(())
}

fn load_pass(path: &Path, phase: &PhaseControl) -> Result<()> {
    let bytes = read_stable(path)?;
    let pass: WarmupPass = serde_json::from_slice(&bytes).context("parse warmup-pass JSON")?;
    ensure!(pass.schema_version == 1, "unsupported warmup-pass schema");
    ensure!(
        (
            pass.batch_id.as_str(),
            pass.run_id.as_str(),
            pass.arm.as_str()
        ) == (
            phase.batch_id.as_str(),
            phase.run_id.as_str(),
            phase.arm.as_str()
        ),
        "warmup-pass execution identity mismatch"
    );
    ensure!(
        pass.phase_sha256 == phase.digest(),
        "warmup-pass phase digest mismatch"
    );
    ensure!(
        pass.warmup_start_us == phase.warmup_start_us
            && pass.t0_us == phase.t0_us
            && pass.validated_at_us == phase.observed_through_us()
            && !pass.warmup_stall,
        "warmup-pass window or decision mismatch"
    );
    validate_track("pc", &pass.tracks.pc, phase)?;
    validate_track("haptic", &pass.tracks.haptic, phase)?;
    Ok(())
}

fn ensure_phase_observed_on_time(phase: &PhaseControl, observed_at_us: u64) -> Result<()> {
    ensure!(
        observed_at_us <= phase.warmup_start_us,
        "phase-control was observed after the registered warmup start"
    );
    Ok(())
}

fn load_pass_at(path: &Path, phase: &PhaseControl, observed_at_us: u64) -> Result<()> {
    ensure!(
        observed_at_us >= phase.observed_through_us(),
        "warmup pass appeared before the registered observation closed"
    );
    ensure!(
        observed_at_us < phase.t0_us,
        "warmup pass was not accepted before the registered t0"
    );
    load_pass(path, phase)
}

pub async fn wait_phase_control(
    path: PathBuf,
    batch_id: &str,
    run_id: &str,
    arm: &str,
    timeout: Duration,
) -> Result<PhaseControl> {
    let deadline = now_us()
        .checked_add(timeout.as_micros() as u64)
        .context("phase-control wait deadline overflow")?;
    while now_us() < deadline {
        if path.exists() || path.is_symlink() {
            let phase = PhaseControl::load(&path, batch_id, run_id, arm)?;
            ensure_phase_observed_on_time(&phase, now_us())?;
            return Ok(phase);
        }
        tokio::time::sleep(POLL).await;
    }
    bail!("phase-control receipt was not published within the registered timeout")
}

pub async fn require_warmup_pass(path: PathBuf, phase: &PhaseControl) -> Result<()> {
    while now_us() < phase.t0_us {
        if path.exists() || path.is_symlink() {
            return load_pass_at(&path, phase, now_us());
        }
        tokio::time::sleep(POLL).await;
    }
    bail!("warmup pass was absent at the registered t0")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

    struct TestDir(PathBuf);

    impl TestDir {
        fn new(label: &str) -> Self {
            let path = std::env::temp_dir().join(format!(
                "skew-phase-{label}-{}-{}",
                std::process::id(),
                NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn path(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn phase() -> PhaseControl {
        PhaseControl {
            schema_version: 1,
            batch_id: "batch-1".into(),
            run_id: "run-1".into(),
            arm: "M".into(),
            created_us: 9_750_000,
            warmup_start_us: 10_000_000,
            t0_us: 13_000_000,
        }
    }

    fn phase_json() -> String {
        String::from(
            "{\"schema_version\":1,\"batch_id\":\"batch-1\",\"run_id\":\"run-1\",\"arm\":\"M\",\"created_us\":9750000,\"warmup_start_us\":10000000,\"t0_us\":13000000}\n",
        )
    }

    fn pass_json(_phase: &PhaseControl, batch_id: &str, digest: &str) -> String {
        format!(
            "{{\"schema_version\":1,\"batch_id\":\"{batch_id}\",\"run_id\":\"run-1\",\"arm\":\"M\",\"phase_sha256\":\"{digest}\",\"warmup_start_us\":10000000,\"t0_us\":13000000,\"validated_at_us\":12750000,\"warmup_stall\":false,\"tracks\":{{\"pc\":{{\"event_count\":4,\"first_recv_us\":10000001,\"last_recv_us\":12500001,\"max_gap_us\":999999}},\"haptic\":{{\"event_count\":4,\"first_recv_us\":10000001,\"last_recv_us\":12500001,\"max_gap_us\":999999}}}}}}\n"
        )
    }

    #[test]
    fn canonical_digest_matches_the_python_contract() {
        assert_eq!(
            phase().digest(),
            "13ddaae641fbd9aba2351b1d864675ffa2c3e1af6f96ff7160cb35f246545629"
        );
    }

    #[test]
    fn actual_phase_and_pass_files_match_the_python_contract() {
        let temp = TestDir::new("roundtrip");
        let phase_path = temp.path("phase.json");
        std::fs::write(&phase_path, phase_json()).unwrap();
        let loaded = PhaseControl::load(&phase_path, "batch-1", "run-1", "M").unwrap();
        assert_eq!(loaded, phase());
        assert_eq!(
            loaded.digest(),
            "13ddaae641fbd9aba2351b1d864675ffa2c3e1af6f96ff7160cb35f246545629"
        );
        let pass_path = temp.path("pass.json");
        std::fs::write(&pass_path, pass_json(&loaded, "batch-1", &loaded.digest())).unwrap();
        load_pass_at(&pass_path, &loaded, loaded.observed_through_us()).unwrap();
    }

    #[test]
    fn receipt_files_reject_symlinks_duplicate_and_extra_keys() {
        use std::os::unix::fs::symlink;

        let temp = TestDir::new("strict");
        let phase_path = temp.path("phase.json");
        std::fs::write(&phase_path, phase_json()).unwrap();
        let link_path = temp.path("phase-link.json");
        symlink(&phase_path, &link_path).unwrap();
        assert!(PhaseControl::load(&link_path, "batch-1", "run-1", "M").is_err());

        let duplicate_path = temp.path("duplicate.json");
        std::fs::write(
            &duplicate_path,
            phase_json().replacen(
                "\"schema_version\":1",
                "\"schema_version\":1,\"schema_version\":1",
                1,
            ),
        )
        .unwrap();
        assert!(PhaseControl::load(&duplicate_path, "batch-1", "run-1", "M").is_err());

        let extra_path = temp.path("extra.json");
        std::fs::write(
            &extra_path,
            phase_json().replacen("}\n", ",\"extra\":1}\n", 1),
        )
        .unwrap();
        assert!(PhaseControl::load(&extra_path, "batch-1", "run-1", "M").is_err());
    }

    #[test]
    fn pass_rejects_identity_digest_and_premature_authority() {
        let temp = TestDir::new("pass-boundaries");
        let phase = phase();
        let pass_path = temp.path("pass.json");
        std::fs::write(&pass_path, pass_json(&phase, "batch-1", &phase.digest())).unwrap();
        assert!(load_pass_at(&pass_path, &phase, phase.observed_through_us() - 1).is_err());
        load_pass_at(&pass_path, &phase, phase.observed_through_us()).unwrap();
        assert!(load_pass_at(&pass_path, &phase, phase.t0_us).is_err());

        let wrong_digest = temp.path("wrong-digest.json");
        std::fs::write(&wrong_digest, pass_json(&phase, "batch-1", &"0".repeat(64))).unwrap();
        assert!(load_pass_at(&wrong_digest, &phase, phase.observed_through_us()).is_err());

        let wrong_identity = temp.path("wrong-identity.json");
        std::fs::write(
            &wrong_identity,
            pass_json(&phase, "other-batch", &phase.digest()),
        )
        .unwrap();
        assert!(load_pass_at(&wrong_identity, &phase, phase.observed_through_us()).is_err());
    }

    #[test]
    fn phase_observation_boundary_rejects_late_catch_up() {
        let phase = phase();
        ensure_phase_observed_on_time(&phase, phase.warmup_start_us).unwrap();
        assert!(ensure_phase_observed_on_time(&phase, phase.warmup_start_us + 1).is_err());
    }

    #[test]
    fn both_track_evidence_is_strictly_inside_the_stall_boundary() {
        let phase = phase();
        let good = TrackEvidence {
            event_count: 4,
            first_recv_us: 10_000_001,
            last_recv_us: 12_500_001,
            max_gap_us: 999_999,
        };
        validate_track("pc", &good, &phase).unwrap();
        let bad = TrackEvidence {
            max_gap_us: 1_000_000,
            ..good
        };
        assert!(validate_track("pc", &bad, &phase).is_err());
    }
}
