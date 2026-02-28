use crate::error::{Error, Result};
use crate::format::Record;
use crate::sstable::{
    build_segment_meta, may_contain_key, read_index, read_manifest, read_table, read_value,
    write_index, write_manifest, write_table, Manifest, SegmentMeta,
};
use crate::wal::Wal;
use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const MAX_SEGMENTS_BEFORE_COMPACT: usize = 4;

#[derive(Clone)]
struct VersionedValue {
    commit_ts: u64,
    value: Option<Vec<u8>>,
}

#[derive(Default)]
struct DbState {
    data: BTreeMap<Vec<u8>, Vec<VersionedValue>>,
    next_tx_id: u64,
    last_commit_ts: u64,
    last_flushed_commit_ts: u64,
    segments: Vec<SegmentEntry>, // oldest -> newest
    active_snapshots: BTreeMap<u64, usize>,
}

#[derive(Clone)]
struct SegmentEntry {
    name: String,
    meta: SegmentMeta,
}

struct DbInner {
    dir: PathBuf,
    manifest_path: PathBuf,
    state: Mutex<DbState>,
    wal: Mutex<Wal>,
    commit_lock: Mutex<()>,
    bg_compaction: Mutex<Option<BackgroundCompaction>>,
}

#[derive(Clone)]
pub struct Database {
    inner: Arc<DbInner>,
}

pub struct RangeIterator {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    pos: usize,
}

pub struct Transaction {
    db: Database,
    tx_id: u64,
    snapshot_ts: u64,
    writes: HashMap<Vec<u8>, Option<Vec<u8>>>,
    closed: bool,
}

struct BackgroundCompaction {
    stop: Arc<AtomicBool>,
    handle: JoinHandle<()>,
}

impl Database {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let wal_path = dir.join("db.wal");
        let manifest_path = dir.join("MANIFEST");
        let snapshot_path = dir.join("db.snapshot");

        if !wal_path.exists() {
            File::create(&wal_path)?;
        }

        let mut state = if manifest_path.exists() {
            load_state_from_manifest(&dir, &manifest_path)?
        } else if snapshot_path.exists() {
            read_legacy_snapshot(&snapshot_path)?
        } else {
            DbState::default()
        };

        let mut pending: HashMap<u64, Vec<Record>> = HashMap::new();
        let mut max_tx_id = state.next_tx_id;
        for record in Wal::read_all(&wal_path)? {
            max_tx_id = max_tx_id.max(record_tx_id(&record));
            apply_record_for_recovery(&mut state, &mut pending, record)?;
        }
        state.next_tx_id = max_tx_id;

        let wal = Wal::open(&wal_path)?;
        let inner = Arc::new(DbInner {
            dir,
            manifest_path,
            state: Mutex::new(state),
            wal: Mutex::new(wal),
            commit_lock: Mutex::new(()),
            bg_compaction: Mutex::new(None),
        });
        Ok(Self { inner })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| Error::Corruption("state lock poisoned"))?;
        let snapshot_ts = state.last_commit_ts;
        let mem_read = read_visible_in_mem(&state, key, snapshot_ts);
        let segments = state.segments.clone();
        drop(state);
        match mem_read {
            MemRead::Found(v) => Ok(v),
            MemRead::NotPresent => {
                match read_from_segments(&self.inner.dir, &segments, key)? {
                    Some(v) => Ok(v),
                    None => Ok(None),
                }
            }
        }
    }

    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut tx = self.begin_tx()?;
        tx.set(key, value);
        tx.commit()
    }

    pub fn get_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if start_key > end_key {
            return Err(Error::InvalidData("start_key must be <= end_key".to_string()));
        }
        let state = self
            .inner
            .state
            .lock()
            .map_err(|_| Error::Corruption("state lock poisoned"))?;
        let snapshot_ts = state.last_commit_ts;
        let segments = state.segments.clone();
        let mem_overlay = materialize_visible_mem_overlay(&state.data, snapshot_ts);
        drop(state);
        let merged = build_visible_view(&self.inner.dir, &segments, mem_overlay)?;
        Ok(range_filter(merged, start_key, end_key))
    }

    pub fn iter_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<RangeIterator> {
        let entries = self.get_range(start_key, end_key)?;
        Ok(RangeIterator { entries, pos: 0 })
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let mut tx = self.begin_tx()?;
        tx.delete(key);
        tx.commit()
    }

    pub fn begin_tx(&self) -> Result<Transaction> {
        let (tx_id, snapshot_ts) = {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| Error::Corruption("state lock poisoned"))?;
            state.next_tx_id += 1;
            (state.next_tx_id, state.last_commit_ts)
        };

        let mut wal = self
            .inner
            .wal
            .lock()
            .map_err(|_| Error::Corruption("wal lock poisoned"))?;
        wal.append(&Record::Begin { tx_id })?;
        drop(wal);

        {
            let mut state = self
                .inner
                .state
                .lock()
                .map_err(|_| Error::Corruption("state lock poisoned"))?;
            register_snapshot(&mut state, snapshot_ts);
        }

        Ok(Transaction {
            db: self.clone(),
            tx_id,
            snapshot_ts,
            writes: HashMap::new(),
            closed: false,
        })
    }

    pub fn gc_old_versions(&self) -> Result<usize> {
        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| Error::Corruption("state lock poisoned"))?;
        let floor = oldest_active_snapshot(&state).unwrap_or(state.last_commit_ts);
        let mut removed = 0usize;
        for versions in state.data.values_mut() {
            if versions.len() <= 1 {
                continue;
            }
            let Some(keep_from) = versions.iter().rposition(|v| v.commit_ts <= floor) else {
                continue;
            };
            if keep_from > 0 {
                removed += keep_from;
                versions.drain(0..keep_from);
            }
        }
        Ok(removed)
    }

    pub fn checkpoint(&self) -> Result<()> {
        let _commit_guard = self
            .inner
            .commit_lock
            .lock()
            .map_err(|_| Error::Corruption("commit lock poisoned"))?;

        let mut state = self
            .inner
            .state
            .lock()
            .map_err(|_| Error::Corruption("state lock poisoned"))?;

        let delta = materialize_delta_since(&state.data, state.last_flushed_commit_ts);
        if !delta.is_empty() {
            let segment_name = format!("segment-{:020}.sst", state.last_commit_ts);
            write_segment_atomically(&self.inner.dir, &segment_name, &delta)?;
            state.segments.push(load_segment_entry(&self.inner.dir, &segment_name)?);
            state.last_flushed_commit_ts = state.last_commit_ts;
        }

        if state.segments.len() > MAX_SEGMENTS_BEFORE_COMPACT {
            let compact_name = format!("compact-{:020}.sst", state.last_commit_ts);
            let compact_data = build_compaction_view(&self.inner.dir, &state.segments, &state.data)?;
            write_segment_atomically(&self.inner.dir, &compact_name, &compact_data)?;
            for old in &state.segments {
                let old_path = self.inner.dir.join(&old.name);
                let old_idx = self.inner.dir.join(format!("{}.idx", &old.name));
                if old_path.exists() {
                    let _ = fs::remove_file(old_path);
                }
                if old_idx.exists() {
                    let _ = fs::remove_file(old_idx);
                }
            }
            state.segments = vec![load_segment_entry(&self.inner.dir, &compact_name)?];
        }

        let manifest_tmp = self.inner.dir.join("MANIFEST.tmp");
        let manifest = Manifest {
            last_commit_ts: state.last_commit_ts,
            next_tx_id: state.next_tx_id,
            last_flushed_commit_ts: state.last_flushed_commit_ts,
            segments: state.segments.iter().map(|x| x.name.clone()).collect(),
        };
        write_manifest(&manifest_tmp, &manifest)?;
        fs::rename(&manifest_tmp, &self.inner.manifest_path)?;
        drop(state);

        let mut wal = self
            .inner
            .wal
            .lock()
            .map_err(|_| Error::Corruption("wal lock poisoned"))?;
        wal.sync()?;
        wal.reset()?;
        Ok(())
    }

    pub fn start_background_compaction(&self, interval: Duration) -> Result<()> {
        if interval.is_zero() {
            return Err(Error::InvalidData(
                "background compaction interval must be > 0".to_string(),
            ));
        }
        let mut guard = self
            .inner
            .bg_compaction
            .lock()
            .map_err(|_| Error::Corruption("bg compaction lock poisoned"))?;
        if guard.is_some() {
            return Err(Error::InvalidData(
                "background compaction already running".to_string(),
            ));
        }

        let stop = Arc::new(AtomicBool::new(false));
        let stop_clone = Arc::clone(&stop);
        let weak_inner: Weak<DbInner> = Arc::downgrade(&self.inner);
        let handle = thread::spawn(move || {
            loop {
                if stop_clone.load(Ordering::Relaxed) {
                    break;
                }
                let Some(inner) = weak_inner.upgrade() else {
                    break;
                };
                let db = Database { inner };
                let _ = db.checkpoint();
                let _ = db.gc_old_versions();
                thread::sleep(interval);
            }
        });

        *guard = Some(BackgroundCompaction { stop, handle });
        Ok(())
    }

    pub fn stop_background_compaction(&self) -> Result<()> {
        let bg = {
            let mut guard = self
                .inner
                .bg_compaction
                .lock()
                .map_err(|_| Error::Corruption("bg compaction lock poisoned"))?;
            guard.take()
        };
        if let Some(bg) = bg {
            bg.stop.store(true, Ordering::Relaxed);
            let _ = bg.handle.join();
        }
        Ok(())
    }
}

impl Transaction {
    pub fn set(&mut self, key: &[u8], value: &[u8]) {
        self.writes.insert(key.to_vec(), Some(value.to_vec()));
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.writes.insert(key.to_vec(), None);
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(v) = self.writes.get(key) {
            return Ok(v.clone());
        }
        let state = self
            .db
            .inner
            .state
            .lock()
            .map_err(|_| Error::Corruption("state lock poisoned"))?;
        let mem_read = read_visible_in_mem(&state, key, self.snapshot_ts);
        let segments = state.segments.clone();
        let visible_flushed = self.snapshot_ts >= state.last_flushed_commit_ts;
        drop(state);
        match mem_read {
            MemRead::Found(v) => Ok(v),
            MemRead::NotPresent if visible_flushed => {
                match read_from_segments(&self.db.inner.dir, &segments, key)? {
                    Some(v) => Ok(v),
                    None => Ok(None),
                }
            }
            MemRead::NotPresent => Ok(None),
        }
    }

    pub fn get_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if start_key > end_key {
            return Err(Error::InvalidData("start_key must be <= end_key".to_string()));
        }
        let state = self
            .db
            .inner
            .state
            .lock()
            .map_err(|_| Error::Corruption("state lock poisoned"))?;
        let segments = state.segments.clone();
        let visible_flushed = self.snapshot_ts >= state.last_flushed_commit_ts;
        let mut mem_overlay = materialize_visible_mem_overlay(&state.data, self.snapshot_ts);
        drop(state);

        for (k, v) in &self.writes {
            mem_overlay.insert(k.clone(), v.clone());
        }

        let merged = if visible_flushed {
            build_visible_view(&self.db.inner.dir, &segments, mem_overlay)?
        } else {
            mem_overlay
        };
        Ok(range_filter(merged, start_key, end_key))
    }

    pub fn iter_range(&self, start_key: &[u8], end_key: &[u8]) -> Result<RangeIterator> {
        let entries = self.get_range(start_key, end_key)?;
        Ok(RangeIterator { entries, pos: 0 })
    }

    pub fn commit(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }

        let mut ordered: Vec<_> = self.writes.iter().collect();
        ordered.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

        let _commit_guard = self
            .db
            .inner
            .commit_lock
            .lock()
            .map_err(|_| Error::Corruption("commit lock poisoned"))?;

        let commit_ts = {
            let state = self
                .db
                .inner
                .state
                .lock()
                .map_err(|_| Error::Corruption("state lock poisoned"))?;
            for (key, _) in &ordered {
                let latest = latest_commit_ts_for_key(&state, key);
                if latest > self.snapshot_ts {
                    return Err(Error::Conflict(format!(
                        "write-write conflict for key {:?}",
                        String::from_utf8_lossy(key)
                    )));
                }
            }
            state.last_commit_ts + 1
        };

        {
            let mut wal = self
                .db
                .inner
                .wal
                .lock()
                .map_err(|_| Error::Corruption("wal lock poisoned"))?;
            for (key, value) in &ordered {
                match value {
                    Some(v) => wal.append(&Record::Set {
                        tx_id: self.tx_id,
                        key: (*key).clone(),
                        value: v.clone(),
                    })?,
                    None => wal.append(&Record::Delete {
                        tx_id: self.tx_id,
                        key: (*key).clone(),
                    })?,
                }
            }
            wal.append(&Record::Commit { tx_id: self.tx_id })?;
            wal.sync()?;
        }

        let mut state = self
            .db
            .inner
            .state
            .lock()
            .map_err(|_| Error::Corruption("state lock poisoned"))?;
        for (key, value) in ordered {
            append_version(&mut state.data, key.clone(), commit_ts, value.clone());
        }
        state.last_commit_ts = commit_ts;
        drop(state);

        self.close();
        Ok(())
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.close();
    }
}

impl Iterator for RangeIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.entries.len() {
            return None;
        }
        let out = self.entries[self.pos].clone();
        self.pos += 1;
        Some(out)
    }
}

impl Transaction {
    fn close(&mut self) {
        if self.closed {
            return;
        }
        if let Ok(mut state) = self.db.inner.state.lock() {
            unregister_snapshot(&mut state, self.snapshot_ts);
        }
        self.closed = true;
    }
}

fn write_segment_atomically(
    dir: &Path,
    segment_name: &str,
    data: &BTreeMap<Vec<u8>, Option<Vec<u8>>>,
) -> Result<()> {
    let tmp_name = format!("{segment_name}.tmp");
    let tmp_path = dir.join(&tmp_name);
    let segment_path = dir.join(segment_name);
    let idx_name = format!("{segment_name}.idx");
    let idx_tmp_name = format!("{idx_name}.tmp");
    let idx_tmp_path = dir.join(&idx_tmp_name);
    let idx_path = dir.join(&idx_name);
    write_table(&tmp_path, data)?;
    let meta = build_segment_meta(data);
    write_index(&idx_tmp_path, &meta)?;
    if segment_path.exists() {
        fs::remove_file(&segment_path)?;
    }
    if idx_path.exists() {
        fs::remove_file(&idx_path)?;
    }
    fs::rename(&tmp_path, &segment_path)?;
    fs::rename(&idx_tmp_path, &idx_path)?;
    Ok(())
}

fn load_state_from_manifest(dir: &Path, manifest_path: &Path) -> Result<DbState> {
    let manifest = read_manifest(manifest_path)?;
    let mut segments = Vec::new();
    for name in manifest.segments {
        if dir.join(&name).exists() {
            segments.push(load_segment_entry(dir, &name)?);
        }
    }
    Ok(DbState {
        data: BTreeMap::new(),
        next_tx_id: manifest.next_tx_id,
        last_commit_ts: manifest.last_commit_ts,
        last_flushed_commit_ts: manifest.last_flushed_commit_ts,
        segments,
        active_snapshots: BTreeMap::new(),
    })
}

fn latest_commit_ts_for_key(state: &DbState, key: &[u8]) -> u64 {
    state
        .data
        .get(key)
        .and_then(|v| v.last().map(|x| x.commit_ts))
        .unwrap_or(0)
}

fn append_version(
    data: &mut BTreeMap<Vec<u8>, Vec<VersionedValue>>,
    key: Vec<u8>,
    commit_ts: u64,
    value: Option<Vec<u8>>,
) {
    data.entry(key).or_default().push(VersionedValue { commit_ts, value });
}

fn register_snapshot(state: &mut DbState, snapshot_ts: u64) {
    let counter = state.active_snapshots.entry(snapshot_ts).or_insert(0);
    *counter += 1;
}

fn unregister_snapshot(state: &mut DbState, snapshot_ts: u64) {
    if let Some(counter) = state.active_snapshots.get_mut(&snapshot_ts) {
        if *counter > 1 {
            *counter -= 1;
        } else {
            state.active_snapshots.remove(&snapshot_ts);
        }
    }
}

fn oldest_active_snapshot(state: &DbState) -> Option<u64> {
    state.active_snapshots.keys().next().copied()
}

enum MemRead {
    Found(Option<Vec<u8>>),
    NotPresent,
}

fn read_visible_in_mem(state: &DbState, key: &[u8], snapshot_ts: u64) -> MemRead {
    let Some(versions) = state.data.get(key) else {
        return MemRead::NotPresent;
    };
    for version in versions.iter().rev() {
        if version.commit_ts <= snapshot_ts {
            return MemRead::Found(version.value.clone());
        }
    }
    MemRead::NotPresent
}

fn read_from_segments(
    dir: &Path,
    segments: &[SegmentEntry],
    key: &[u8],
) -> Result<Option<Option<Vec<u8>>>> {
    for segment in segments.iter().rev() {
        if !may_contain_key(&segment.meta, key) {
            continue;
        }
        let path = dir.join(&segment.name);
        if !path.exists() {
            continue;
        }
        if let Some(v) = read_value(&path, key)? {
            return Ok(Some(v));
        }
    }
    Ok(None)
}

fn materialize_visible_mem_overlay(
    data: &BTreeMap<Vec<u8>, Vec<VersionedValue>>,
    snapshot_ts: u64,
) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
    let mut out = BTreeMap::new();
    for (k, versions) in data {
        if let Some(v) = versions.iter().rev().find(|x| x.commit_ts <= snapshot_ts) {
            out.insert(k.clone(), v.value.clone());
        }
    }
    out
}

fn build_visible_view(
    dir: &Path,
    segments: &[SegmentEntry],
    mem_overlay: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
) -> Result<BTreeMap<Vec<u8>, Option<Vec<u8>>>> {
    let mut out = BTreeMap::new();
    for segment in segments {
        let path = dir.join(&segment.name);
        if !path.exists() {
            continue;
        }
        let table = read_table(&path)?;
        for (k, v) in table {
            out.insert(k, v);
        }
    }
    for (k, v) in mem_overlay {
        out.insert(k, v);
    }
    Ok(out)
}

fn range_filter(
    merged: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    start_key: &[u8],
    end_key: &[u8],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    for (k, v) in merged {
        if k.as_slice() < start_key || k.as_slice() >= end_key {
            continue;
        }
        if let Some(value) = v {
            out.push((k, value));
        }
    }
    out
}

fn apply_record_for_recovery(
    state: &mut DbState,
    pending: &mut HashMap<u64, Vec<Record>>,
    record: Record,
) -> Result<()> {
    match record {
        Record::Begin { tx_id } => {
            pending.entry(tx_id).or_default();
        }
        Record::Set { tx_id, .. } | Record::Delete { tx_id, .. } => {
            pending.entry(tx_id).or_default().push(record);
        }
        Record::Commit { tx_id } => {
            if let Some(records) = pending.remove(&tx_id) {
                state.last_commit_ts += 1;
                let commit_ts = state.last_commit_ts;
                for rec in records {
                    match rec {
                        Record::Set { key, value, .. } => {
                            append_version(&mut state.data, key, commit_ts, Some(value));
                        }
                        Record::Delete { key, .. } => {
                            append_version(&mut state.data, key, commit_ts, None);
                        }
                        _ => return Err(Error::Corruption("unexpected record in tx body")),
                    }
                }
            }
        }
    }
    Ok(())
}

fn record_tx_id(record: &Record) -> u64 {
    match record {
        Record::Begin { tx_id } => *tx_id,
        Record::Set { tx_id, .. } => *tx_id,
        Record::Delete { tx_id, .. } => *tx_id,
        Record::Commit { tx_id } => *tx_id,
    }
}

fn materialize_delta_since(
    data: &BTreeMap<Vec<u8>, Vec<VersionedValue>>,
    from_commit_ts: u64,
) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
    let mut out = BTreeMap::new();
    for (key, versions) in data {
        if let Some(v) = versions.iter().rev().find(|x| x.commit_ts > from_commit_ts) {
            out.insert(key.clone(), v.value.clone());
        }
    }
    out
}

fn build_compaction_view(
    dir: &Path,
    segments: &[SegmentEntry],
    mem: &BTreeMap<Vec<u8>, Vec<VersionedValue>>,
) -> Result<BTreeMap<Vec<u8>, Option<Vec<u8>>>> {
    let mut out = BTreeMap::new();
    for segment in segments {
        let path = dir.join(&segment.name);
        if !path.exists() {
            continue;
        }
        let table = read_table(&path)?;
        for (k, v) in table {
            out.insert(k, v);
        }
    }
    for (k, versions) in mem {
        if let Some(v) = versions.last() {
            out.insert(k.clone(), v.value.clone());
        }
    }
    Ok(out)
}

fn load_segment_entry(dir: &Path, name: &str) -> Result<SegmentEntry> {
    let idx_path = dir.join(format!("{name}.idx"));
    let meta = if idx_path.exists() {
        read_index(&idx_path)?
    } else {
        let table = read_table(&dir.join(name))?;
        build_segment_meta(&table)
    };
    Ok(SegmentEntry {
        name: name.to_string(),
        meta,
    })
}

fn read_legacy_snapshot(path: &Path) -> Result<DbState> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)?;
    match &magic {
        b"TDBSNAP2" => read_legacy_snapshot_v2(file),
        b"TDBSNAP1" => read_legacy_snapshot_v1(file),
        _ => Err(Error::Corruption("snapshot magic mismatch")),
    }
}

fn read_legacy_snapshot_v2(mut file: File) -> Result<DbState> {
    let mut commit_bytes = [0u8; 8];
    let mut next_tx_bytes = [0u8; 8];
    let mut count_bytes = [0u8; 8];
    file.read_exact(&mut commit_bytes)?;
    file.read_exact(&mut next_tx_bytes)?;
    file.read_exact(&mut count_bytes)?;

    let last_commit_ts = u64::from_le_bytes(commit_bytes);
    let next_tx_id = u64::from_le_bytes(next_tx_bytes);
    let count = u64::from_le_bytes(count_bytes);

    let mut data = BTreeMap::new();
    for _ in 0..count {
        let (k, v) = read_entry(&mut file)?;
        append_version(&mut data, k, last_commit_ts.max(1), Some(v));
    }
    Ok(DbState {
        data,
        next_tx_id,
        last_commit_ts,
        last_flushed_commit_ts: last_commit_ts,
        segments: Vec::new(),
        active_snapshots: BTreeMap::new(),
    })
}

fn read_legacy_snapshot_v1(mut file: File) -> Result<DbState> {
    let mut count_bytes = [0u8; 8];
    file.read_exact(&mut count_bytes)?;
    let count = u64::from_le_bytes(count_bytes);
    let mut data = BTreeMap::new();
    for _ in 0..count {
        let (k, v) = read_entry(&mut file)?;
        append_version(&mut data, k, 1, Some(v));
    }
    let ts = if count > 0 { 1 } else { 0 };
    Ok(DbState {
        data,
        next_tx_id: 0,
        last_commit_ts: ts,
        last_flushed_commit_ts: ts,
        segments: Vec::new(),
        active_snapshots: BTreeMap::new(),
    })
}

fn read_entry(file: &mut File) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut kb = [0u8; 4];
    let mut vb = [0u8; 4];
    file.read_exact(&mut kb)?;
    file.read_exact(&mut vb)?;
    let klen = u32::from_le_bytes(kb) as usize;
    let vlen = u32::from_le_bytes(vb) as usize;
    let mut k = vec![0; klen];
    let mut v = vec![0; vlen];
    file.read_exact(&mut k)?;
    file.read_exact(&mut v)?;
    Ok((k, v))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};
    use std::time::Duration;

    fn unique_dir(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("tinydatabase-{name}-{stamp}"))
    }

    fn wal_path(dir: &Path) -> PathBuf {
        dir.join("db.wal")
    }

    #[test]
    fn basic_set_get_delete() {
        let dir = unique_dir("basic");
        let db = Database::open(&dir).expect("open");
        db.set(b"a", b"1").expect("set");
        assert_eq!(db.get(b"a").expect("get"), Some(b"1".to_vec()));
        db.delete(b"a").expect("delete");
        assert_eq!(db.get(b"a").expect("get"), None);
    }

    #[test]
    fn recover_after_reopen() {
        let dir = unique_dir("recover");
        {
            let db = Database::open(&dir).expect("open");
            db.set(b"k1", b"v1").expect("set");
            db.set(b"k2", b"v2").expect("set");
        }
        {
            let db = Database::open(&dir).expect("reopen");
            assert_eq!(db.get(b"k1").expect("get"), Some(b"v1".to_vec()));
            assert_eq!(db.get(b"k2").expect("get"), Some(b"v2".to_vec()));
        }
    }

    #[test]
    fn rollback_uncommitted_tx() {
        let dir = unique_dir("rollback");
        let db = Database::open(&dir).expect("open");
        {
            let mut tx = db.begin_tx().expect("begin");
            tx.set(b"x", b"1");
        }
        let db2 = Database::open(&dir).expect("reopen");
        assert_eq!(db2.get(b"x").expect("get"), None);
    }

    #[test]
    fn checkpoint_roundtrip() {
        let dir = unique_dir("checkpoint");
        let db = Database::open(&dir).expect("open");
        db.set(b"a", b"10").expect("set");
        db.set(b"b", b"20").expect("set");
        db.checkpoint().expect("checkpoint");
        let db2 = Database::open(&dir).expect("reopen");
        assert_eq!(db2.get(b"a").expect("get"), Some(b"10".to_vec()));
        assert_eq!(db2.get(b"b").expect("get"), Some(b"20".to_vec()));
    }

    #[test]
    fn snapshot_read_stability() {
        let dir = unique_dir("snapshot");
        let db = Database::open(&dir).expect("open");
        db.set(b"k", b"v1").expect("set");
        let tx = db.begin_tx().expect("begin");
        db.set(b"k", b"v2").expect("set");
        assert_eq!(tx.get(b"k").expect("tx get"), Some(b"v1".to_vec()));
        assert_eq!(db.get(b"k").expect("db get"), Some(b"v2".to_vec()));
    }

    #[test]
    fn write_conflict_detection() {
        let dir = unique_dir("conflict");
        let db = Database::open(&dir).expect("open");
        db.set(b"c", b"1").expect("seed");

        let mut tx1 = db.begin_tx().expect("tx1");
        let mut tx2 = db.begin_tx().expect("tx2");
        tx1.set(b"c", b"2");
        tx2.set(b"c", b"3");
        tx1.commit().expect("commit tx1");
        let err = tx2.commit().expect_err("must conflict");
        assert!(matches!(err, Error::Conflict(_)));
    }

    #[test]
    fn checkpoint_creates_manifest_and_segments() {
        let dir = unique_dir("segment");
        let db = Database::open(&dir).expect("open");
        db.set(b"alpha", b"1").expect("set");
        db.checkpoint().expect("checkpoint");
        assert!(dir.join("MANIFEST").exists());
        let manifest = read_manifest(&dir.join("MANIFEST")).expect("manifest");
        assert_eq!(manifest.segments.len(), 1);
        assert!(dir.join(&manifest.segments[0]).exists());
        assert!(dir.join(format!("{}.idx", &manifest.segments[0])).exists());
    }

    #[test]
    fn multiple_checkpoints_keep_multiple_segments() {
        let dir = unique_dir("multiseg");
        let db = Database::open(&dir).expect("open");
        db.set(b"a", b"1").expect("set1");
        db.checkpoint().expect("cp1");
        db.set(b"b", b"2").expect("set2");
        db.checkpoint().expect("cp2");
        db.set(b"a", b"3").expect("set3");
        db.checkpoint().expect("cp3");

        let manifest = read_manifest(&dir.join("MANIFEST")).expect("manifest");
        assert!(manifest.segments.len() >= 2);
        let reopened = Database::open(&dir).expect("reopen");
        assert_eq!(reopened.get(b"a").expect("get a"), Some(b"3".to_vec()));
        assert_eq!(reopened.get(b"b").expect("get b"), Some(b"2".to_vec()));
    }

    #[test]
    fn compaction_caps_segment_count() {
        let dir = unique_dir("compact");
        let db = Database::open(&dir).expect("open");
        for i in 0..7 {
            let k = format!("k{i}");
            let v = format!("v{i}");
            db.set(k.as_bytes(), v.as_bytes()).expect("set");
            db.checkpoint().expect("checkpoint");
        }
        let manifest = read_manifest(&dir.join("MANIFEST")).expect("manifest");
        assert!(manifest.segments.len() <= MAX_SEGMENTS_BEFORE_COMPACT);
    }

    #[test]
    fn database_get_range_returns_sorted_slice() {
        let dir = unique_dir("range-db");
        let db = Database::open(&dir).expect("open");
        db.set(b"a", b"1").expect("set a");
        db.set(b"b", b"2").expect("set b");
        db.set(b"c", b"3").expect("set c");
        db.set(b"d", b"4").expect("set d");
        let range = db.get_range(b"b", b"d").expect("range");
        assert_eq!(
            range,
            vec![
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
            ]
        );
    }

    #[test]
    fn transaction_range_is_snapshot_consistent() {
        let dir = unique_dir("range-tx");
        let db = Database::open(&dir).expect("open");
        db.set(b"k1", b"v1").expect("set k1");
        db.set(b"k2", b"v2").expect("set k2");
        let tx = db.begin_tx().expect("begin tx");
        db.set(b"k2", b"v2-new").expect("update k2");
        db.set(b"k3", b"v3").expect("set k3");
        let tx_range = tx.get_range(b"k1", b"k4").expect("tx range");
        assert_eq!(
            tx_range,
            vec![
                (b"k1".to_vec(), b"v1".to_vec()),
                (b"k2".to_vec(), b"v2".to_vec()),
            ]
        );
    }

    #[test]
    fn range_iterator_yields_all_items() {
        let dir = unique_dir("range-iter");
        let db = Database::open(&dir).expect("open");
        db.set(b"a", b"1").expect("set a");
        db.set(b"b", b"2").expect("set b");
        db.set(b"c", b"3").expect("set c");
        let iter = db.iter_range(b"a", b"z").expect("iter");
        let collected: Vec<_> = iter.collect();
        assert_eq!(
            collected,
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec()),
            ]
        );
    }

    #[test]
    fn gc_removes_stale_versions_without_active_snapshots() {
        let dir = unique_dir("gc-basic");
        let db = Database::open(&dir).expect("open");
        db.set(b"k", b"v1").expect("set 1");
        db.set(b"k", b"v2").expect("set 2");
        db.set(b"k", b"v3").expect("set 3");
        let removed = db.gc_old_versions().expect("gc");
        assert_eq!(removed, 2);
        assert_eq!(db.get(b"k").expect("get"), Some(b"v3".to_vec()));
    }

    #[test]
    fn gc_respects_oldest_active_snapshot() {
        let dir = unique_dir("gc-snapshot");
        let db = Database::open(&dir).expect("open");
        db.set(b"k", b"v1").expect("set 1");
        let tx = db.begin_tx().expect("begin");
        db.set(b"k", b"v2").expect("set 2");

        let removed_while_active = db.gc_old_versions().expect("gc active");
        assert_eq!(removed_while_active, 0);
        assert_eq!(tx.get(b"k").expect("tx get"), Some(b"v1".to_vec()));

        drop(tx);
        let removed_after_drop = db.gc_old_versions().expect("gc after");
        assert_eq!(removed_after_drop, 1);
        assert_eq!(db.get(b"k").expect("get"), Some(b"v2".to_vec()));
    }

    #[test]
    fn recovery_ignores_truncated_wal_tail() {
        let dir = unique_dir("wal-tail");
        {
            let db = Database::open(&dir).expect("open");
            db.set(b"a", b"1").expect("set a");
            db.set(b"b", b"2").expect("set b");
        }
        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(wal_path(&dir))
                .expect("open wal");
            file.write_all(&[0x54, 0x44, 0x42]).expect("append tail");
            file.sync_data().expect("sync");
        }
        let db2 = Database::open(&dir).expect("reopen");
        assert_eq!(db2.get(b"a").expect("get a"), Some(b"1".to_vec()));
        assert_eq!(db2.get(b"b").expect("get b"), Some(b"2".to_vec()));
    }

    #[test]
    fn recovery_ignores_crc_corrupted_last_record() {
        let dir = unique_dir("wal-crc");
        {
            let db = Database::open(&dir).expect("open");
            db.set(b"k1", b"v1").expect("set k1");
            db.set(b"k2", b"v2").expect("set k2");
        }
        {
            let wal = wal_path(&dir);
            let mut bytes = std::fs::read(&wal).expect("read wal");
            let n = bytes.len();
            assert!(n > 0);
            bytes[n - 1] ^= 0xFF;
            std::fs::write(&wal, bytes).expect("write wal");
        }
        let db2 = Database::open(&dir).expect("reopen");
        assert_eq!(db2.get(b"k1").expect("get k1"), Some(b"v1".to_vec()));
        assert_eq!(db2.get(b"k2").expect("get k2"), None);
    }

    #[test]
    fn background_compaction_start_stop() {
        let dir = unique_dir("bg-compact");
        let db = Database::open(&dir).expect("open");
        db.set(b"a", b"1").expect("set");
        db.start_background_compaction(Duration::from_millis(10))
            .expect("start");
        std::thread::sleep(Duration::from_millis(30));
        db.stop_background_compaction().expect("stop");
    }

    #[test]
    fn background_compaction_double_start_fails() {
        let dir = unique_dir("bg-compact-2");
        let db = Database::open(&dir).expect("open");
        db.start_background_compaction(Duration::from_millis(50))
            .expect("start");
        let err = db
            .start_background_compaction(Duration::from_millis(50))
            .expect_err("double start should fail");
        assert!(matches!(err, Error::InvalidData(_)));
        db.stop_background_compaction().expect("stop");
    }
}
