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
- LSM storage engine:
  - multiple immutable SSTable segments
  - delta flush on checkpoint (only changes since last flush)
  - tombstones are persisted in SSTable
  - simple compaction when segment count grows
  - read path: memtable + disk merge (`newest -> oldest` segments)
  - per-segment index sidecar (`min/max key` + bloom filter)
- Recovery on startup from manifest/segments + WAL replay
- Replication foundation:
  - replicated log with `index` + `term` + checksums
  - deterministic state hash from applied commands
  - chunked snapshot install protocol with integrity check

## API

- `Database::open(path)`
- `Database::get(key)`
- `Database::get_range(start_key, end_key)` (start inclusive, end exclusive)
- `Database::iter_range(start_key, end_key)`
- `Database::set(key, value)`
- `Database::delete(key)`
- `Database::begin_tx() -> Transaction`
- `Database::checkpoint()`
- `ReplicatedLog::open/append/append_entry/entries_from/truncate_suffix`
- `SnapshotInstaller::begin` + `SnapshotInstall::append_chunk/finalize`
- `deterministic_state_hash(entries)`

## C ABI

- Library type: `cdylib`
- Handle lifecycle:
  - `tdb_open(path, &handle)`
  - `tdb_close(handle)`
- KV operations:
  - `tdb_set`, `tdb_get`, `tdb_delete`, `tdb_checkpoint`
- Status codes:
  - `tdb_status_ok()`
  - `tdb_status_not_found()`
  - `tdb_status_null_pointer()`
  - `tdb_status_invalid_argument()`
  - `tdb_status_error()`
  - `tdb_status_conflict()`
- Error/message and memory ownership:
  - `tdb_last_error_copy(&ptr, &len)`
  - `tdb_free_buffer(ptr, len)` for buffers returned by FFI
- Header: `include/tinydatabase.h`
- Example client: `examples/c_client.c`
- Build/run example (Linux/macOS):
  - `cargo build --release`
  - `cc examples/c_client.c -Iinclude -Ltarget/release -ltinydatabase -o c_client`
  - `LD_LIBRARY_PATH=target/release ./c_client`
- Build/run example (Windows + MSVC):
  - `cargo build --release`
  - `cl /I include examples\c_client.c /link /LIBPATH:target\release tinydatabase.dll.lib`
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



