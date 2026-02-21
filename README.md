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
- LSM phase 1:
  - immutable SSTable segment (`segment-*.sst`)
  - `MANIFEST` points to active segment and metadata
  - checkpoint writes segment + manifest and truncates WAL
- Recovery on startup from manifest/segment + WAL replay

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
  - `magic: "TDBSST01"`
  - `count: u64`
  - repeated key/value entries with `u32` lengths
- Manifest:
  - `magic: "TDBMAN01"`
  - `last_commit_ts: u64`
  - `next_tx_id: u64`
  - active segment name (optional)
- Legacy snapshot reader:
  - `TDBSNAP1`/`TDBSNAP2` can still be loaded for migration

## Next milestones

1. Multiple SSTables + read merge across levels
2. Background compaction + tombstone GC
3. Crash tests with process kill during write/checkpoint
4. Reader/writer stress tests (`loom`/property tests)
5. Benchmarks and tuning (Bloom filter, compression)
