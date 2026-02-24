# Tiny Database

Embedded key-value database in Rust with durable WAL and crash recovery.

## Current MVP

- In-process library (no server)
- Disk-backed WAL (`db.wal`) with:
  - append-only records
  - CRC32 per record
  - crash-tail tolerance (partial/corrupt tail is ignored)
- Atomic transactions:
  - `BEGIN`
  - `SET`/`DELETE`
  - `COMMIT` + `fsync`
  - uncommitted transactions are discarded during recovery
- MVCC (snapshot isolation baseline):
  - transaction reads are stable against later commits
  - write-write conflicts are detected on commit
- In-memory versioned index (BTreeMap + versions)
- LSM phase 2:
  - multiple immutable SSTable segments
  - delta flush on checkpoint (only changes since last flush)
  - tombstones are persisted in SSTable
  - simple compaction when segment count grows
  - read path: memtable + disk merge (`newest -> oldest` segments)
  - phase 2.2: per-segment index sidecar (`min/max key` + bloom filter)
- Recovery on startup from manifest/segments + WAL replay
- Insane phase 1 (foundation):
  - replicated log with `index` + `term` + checksums
  - deterministic state hash from applied commands
  - chunked snapshot install protocol with integrity check

## API

- `Database::open(path)`
- `Database::get(key)`
- `Database::set(key, value)`
- `Database::delete(key)`
- `Database::begin_tx() -> Transaction`
- `Database::checkpoint()`
- `ReplicatedLog::open/append/append_entry/entries_from/truncate_suffix`
- `SnapshotInstaller::begin` + `SnapshotInstall::append_chunk/finalize`
- `deterministic_state_hash(entries)`

## Storage format

- WAL frame:
  - `magic: u32`
  - `payload_len: u32`
  - `crc32(payload): u32`
  - `payload: bytes`
- SSTable:
  - `magic: "TDBSST02"` (`TDBSST01` reader kept for compatibility)
  - `count: u64`
  - repeated entries: `key_len`, tombstone flag, `value_len`, key, value?
- Segment index sidecar:
  - `magic: "TDBIDX01"`
  - optional `min_key` / `max_key`
  - fixed bloom filter bitmap
- Manifest:
  - `magic: "TDBMAN02"` (`TDBMAN01` reader kept for compatibility)
  - `last_commit_ts: u64`
  - `next_tx_id: u64`
  - `last_flushed_commit_ts: u64`
  - list of segment names (oldest -> newest)
- Replication log:
  - frame: `magic`, payload len, crc32, payload
  - payload: `index`, `term`, command (`set/delete`)
- Snapshot install temp flow:
  - write to `*.installing`, validate size + crc, atomic rename
- Legacy snapshot reader:
  - `TDBSNAP1`/`TDBSNAP2` can still be loaded for migration

## Next milestones

1. Raft protocol layer (leader election, match index, commit index)
2. Snapshot install over network (streaming + resume)
3. Serializable isolation (SSI) on top of replicated commits
4. Background compaction + tombstone GC policy by age/level
5. Crash/fault matrix (disk + network partitions + node restarts)
