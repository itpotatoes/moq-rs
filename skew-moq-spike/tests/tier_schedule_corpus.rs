//! Rust half of the cross-language `s3np` tier-schedule contract.
//!
//! `scripts/event_pair_tier_schedule.py` writes the replay document and
//! `skew_moq::s3np::TierSchedule::parse` consumes it. The two validators are
//! independent implementations of one rule set, so they are checked against the
//! same corpus: `tests/fixtures/tier_schedule/{valid,invalid}/`. The Python half
//! is `scripts/test_event_pair_tier_schedule.py::CorpusTests`.
//!
//! A document only one side rejects is a contract break: the extractor would
//! emit a schedule the sender refuses partway through a batch, or worse, the
//! sender would accept a schedule the extractor considers malformed.

use std::fs;
use std::path::{Path, PathBuf};

use skew_moq::s3np::{TierSchedule, TIER_SCHEDULE_GENERATION, TIER_SCHEDULE_SCHEMA};

fn corpus(kind: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/tier_schedule")
        .join(kind)
}

fn documents(kind: &str) -> Vec<(String, String)> {
    let dir = corpus(kind);
    let mut found: Vec<(String, String)> = fs::read_dir(&dir)
        .unwrap_or_else(|error| panic!("read corpus {}: {error}", dir.display()))
        .map(|entry| entry.expect("corpus entry"))
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "json"))
        .map(|entry| {
            let path = entry.path();
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .expect("fixture name")
                .to_string();
            (name, fs::read_to_string(&path).expect("read fixture"))
        })
        .collect();
    found.sort();
    assert!(
        !found.is_empty(),
        "corpus {} must not be empty",
        dir.display()
    );
    found
}

#[test]
fn every_valid_corpus_document_is_accepted() {
    let documents = documents("valid");
    assert!(
        documents.len() >= 4,
        "the valid corpus must keep covering more than one shape"
    );
    for (name, document) in documents {
        let schedule = TierSchedule::parse(&document)
            .unwrap_or_else(|error| panic!("valid/{name} must parse, got {error:?}"));
        // Provenance is what ties an s3np run to the registered S3 run of its
        // block, so no accepted document may leave it blank.
        assert_eq!(schedule.generation(), TIER_SCHEDULE_GENERATION, "valid/{name}");
        assert!(!schedule.source_run_id().is_empty(), "valid/{name}");
        assert_eq!(schedule.source_tx_sha256().len(), 64, "valid/{name}");
        assert_eq!(schedule.source_rx_sha256().len(), 64, "valid/{name}");
        assert_eq!(schedule.pc_rate_hz(), 30, "valid/{name}");
        assert_eq!(schedule.haptic_rate_hz(), 90, "valid/{name}");
        assert!(schedule.duration_us() > 0, "valid/{name}");
        let switches = schedule.switches();
        assert!(!switches.is_empty(), "valid/{name}");
        assert_eq!(switches[0].t_offset_us, 0, "valid/{name}");
        for window in switches.windows(2) {
            assert!(
                window[0].t_offset_us < window[1].t_offset_us,
                "valid/{name}: offsets must be strictly increasing"
            );
            assert_ne!(window[0].state, window[1].state, "valid/{name}");
        }
        assert!(
            switches.last().expect("non-empty").t_offset_us < schedule.duration_us(),
            "valid/{name}"
        );
        // The replayed state is defined everywhere inside the run.
        assert_eq!(schedule.state_at(0), switches[0].state, "valid/{name}");
        assert_eq!(
            schedule.state_at(schedule.duration_us() - 1),
            switches.last().expect("non-empty").state,
            "valid/{name}"
        );
    }
}

#[test]
fn every_invalid_corpus_document_is_rejected() {
    let documents = documents("invalid");
    assert!(
        documents.len() >= 20,
        "the invalid corpus must keep covering every refusal rule"
    );
    for (name, document) in documents {
        assert!(
            TierSchedule::parse(&document).is_err(),
            "invalid/{name} must be rejected"
        );
    }
}

#[test]
fn the_corpus_pins_the_schema_and_generation_strings() {
    // If either constant changes, the corpus must be regenerated rather than
    // silently drifting away from what the extractor writes.
    let (_, document) = documents("valid")
        .into_iter()
        .find(|(name, _)| name == "all_three_tiers.json")
        .expect("all_three_tiers.json must exist in the corpus");
    assert!(document.contains(TIER_SCHEDULE_SCHEMA));
    assert!(document.contains(TIER_SCHEDULE_GENERATION));
}
