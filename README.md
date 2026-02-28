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
  - leveled flow: `L0` flush + `L0 -> L1` compaction trigger
  - delta flush on checkpoint (only changes since last flush)
  - tombstones are persisted in SSTable
  - optional zstd compression for SSTable payload
  - simple compaction when segment count grows
  - read path: memtable + disk merge (`newest -> oldest` segments)
  - per-segment index sidecar (`min/max key` + bloom filter)
- Recovery on startup from manifest/segments + WAL replay
  - tested against truncated and CRC-corrupted WAL tail
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
- `Database::write_batch(ops: &[BatchOp])`
- `Database::begin_tx() -> Transaction`
- `Database::checkpoint()`
- `Database::gc_old_versions() -> removed_count`
- `Database::start_background_compaction(interval)`
- `Database::stop_background_compaction()`
- `ReplicatedLog::open/append/append_entry/entries_from/truncate_suffix`
- `SnapshotInstaller::begin` + `SnapshotInstall::append_chunk/finalize`
- `deterministic_state_hash(entries)`
- `BatchOp::{Put, Delete}`

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
  - `magic: "TDBSST03"` (`TDBSST01/02` readers kept for compatibility)
  - payload codec: raw or `zstd`
  - payload entries: `count`, repeated `key_len`, tombstone flag, `value_len`, key, value?
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

1. Multi-level LSM planner (`L0/L1/L2+`) with size ratio and overlap rules
2. Raft protocol layer (leader election, match index, commit index)
3. Snapshot install over network (streaming + resume)
4. Serializable isolation (SSI) on top of replicated commits
5. Crash/fault matrix (disk + network partitions + node restarts)

## Benchmarks

- Criterion bench target: `benches/kv_bench.rs`
- Run:
  - `cargo bench --bench kv_bench`
- Current scenarios:
  - `tinydb_put_10k`
  - `tinydb_get_10k`
  - `tinydb_write_batch_10k`
  - `raw_append_file_put_10k` (simple file baseline)
  - `in_memory_btreemap_put_10k` (upper-bound baseline)

### Comparison Table Template

| Engine / Scenario | Throughput (ops/sec) | p99 latency | Disk size |
|---|---:|---:|---:|
| TinyDB put 10k | TBD | TBD | TBD |
| TinyDB get 10k | TBD | TBD | TBD |
| TinyDB write_batch 10k | TBD | TBD | TBD |
| raw append file put 10k | TBD | TBD | TBD |
| sled (optional) | TBD | TBD | TBD |
| rocksdb-rs (optional) | TBD | TBD | TBD |
| redb (optional) | TBD | TBD | TBD |



