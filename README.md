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
- Recovery on startup from manifest/segments + WAL replay

## API

- `Database::open(path)`
- `Database::get(key)`
- `Database::set(key, value)`
- `Database::delete(key)`
- `Database::begin_tx() -> Transaction`
- `Database::checkpoint()`

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
- Manifest:
  - `magic: "TDBMAN02"` (`TDBMAN01` reader kept for compatibility)
  - `last_commit_ts: u64`
  - `next_tx_id: u64`
  - `last_flushed_commit_ts: u64`
  - list of segment names (oldest -> newest)
- Legacy snapshot reader:
  - `TDBSNAP1`/`TDBSNAP2` can still be loaded for migration

## Next milestones

1. Leveling strategy (L0/L1...) instead of single segment list
2. Sparse index / bloom filters to avoid scanning many segments
3. Background compaction + tombstone GC policy by age/level
4. Crash tests with process kill during write/checkpoint
5. Reader/writer stress tests (`loom`/property tests)
