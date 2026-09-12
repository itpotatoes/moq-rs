# Shared `s3np` tier-schedule corpus

Cross-language contract for the plan-단계-9 event-pair NON-preserving control
(`--arm s3np`). The replay document is produced by
`scripts/event_pair_tier_schedule.py` (Python) and consumed by
`skew_moq::s3np::TierSchedule::parse` (Rust). The two validators are
deliberately independent implementations of the same rules, so they are checked
against the **same files**:

- `valid/` — every document here must be ACCEPTED by both validators.
- `invalid/` — every document here must be REJECTED by both validators. The
  specific error kind is allowed to differ between languages; the verdict is not.

Checked by:

- Rust: `tests/tier_schedule_corpus.rs` (`cargo test -p skew-moq-spike`)
- Python: `scripts/test_event_pair_tier_schedule.py::CorpusTests`
  (`.venv/bin/python -m unittest scripts.test_event_pair_tier_schedule`)

`valid/extractor_real_s3_run.json` is a verbatim extractor output, taken from a
real S3 run's TX/RX logs (`runs/phase4_v5_s3_loopback_20260731_v1`, with a
`measurement_start` epoch supplied for the fixture because that pre-boundary log
predates the row). It is the regression guard that the extractor's own output —
including its provenance and `source_workload` keys, which Rust does not
interpret — still parses.

Adding a rule means adding a fixture here and editing both validators. A fixture
that only one side rejects is a contract break, not a fixture problem.
