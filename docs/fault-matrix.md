# Fault Matrix

## Disk faults

- Truncated WAL tail
  - test: `db::tests::recovery_ignores_truncated_wal_tail`
- Corrupted WAL CRC
  - test: `db::tests::recovery_ignores_crc_corrupted_last_record`
- Interrupted snapshot install (resume required)
  - test: `replication::tests::snapshot_receiver_supports_streaming_resume`

## Network faults

- Raft append mismatch / log divergence
  - test: `replication::tests::wal_shipping_conflict_truncates_and_overwrites`
- Snapshot stream out-of-order chunks / stale offset
  - test: `replication::tests::snapshot_receiver_supports_streaming_resume`
- Raft retry backoff on reject
  - test: `raft::tests::failed_append_response_backs_off_next_index`

## Node restart faults

- Restart with committed data
  - test: `db::tests::recover_after_reopen`
- Restart with uncommitted transaction
  - test: `db::tests::rollback_uncommitted_tx`
- PITR replay from archived WAL
  - test: `db::tests::pitr_restore_to_commit_ts`
