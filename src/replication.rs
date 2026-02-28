use crate::error::{Error, Result};
use crc32fast::Hasher;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const LOG_MAGIC: u32 = 0x5444_524c; // TDRL

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Command {
    Set { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub command: Command,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendRequest {
    pub prev_index: u64,
    pub prev_term: u64,
    pub entries: Vec<LogEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendResponse {
    pub success: bool,
    pub last_index: u64,
}

pub struct ReplicatedLog {
    path: PathBuf,
    file: File,
    last_index: u64,
    last_term: u64,
}

impl ReplicatedLog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            File::create(&path)?;
        }
        let mut file = OpenOptions::new().read(true).append(true).open(&path)?;
        let entries = read_all_entries(&mut file)?;
        let (last_index, last_term) = entries
            .last()
            .map(|e| (e.index, e.term))
            .unwrap_or((0, 0));
        Ok(Self {
            path,
            file,
            last_index,
            last_term,
        })
    }

    pub fn last_index(&self) -> u64 {
        self.last_index
    }

    pub fn last_term(&self) -> u64 {
        self.last_term
    }

    pub fn append(&mut self, term: u64, command: Command) -> Result<LogEntry> {
        let entry = LogEntry {
            index: self.last_index + 1,
            term,
            command,
        };
        self.append_entry(&entry)?;
        Ok(entry)
    }

    pub fn append_entry(&mut self, entry: &LogEntry) -> Result<()> {
        if entry.index != self.last_index + 1 {
            return Err(Error::InvalidData(format!(
                "non-contiguous log index: got {}, expected {}",
                entry.index,
                self.last_index + 1
            )));
        }
        let payload = encode_entry(entry);
        let crc = crc32(&payload);
        self.file.write_all(&LOG_MAGIC.to_le_bytes())?;
        self.file.write_all(&(payload.len() as u32).to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&payload)?;
        self.file.sync_data()?;
        self.last_index = entry.index;
        self.last_term = entry.term;
        Ok(())
    }

    pub fn entries_from(&self, start_index: u64) -> Result<Vec<LogEntry>> {
        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        let mut entries = read_all_entries(&mut file)?;
        entries.retain(|e| e.index >= start_index);
        Ok(entries)
    }

    pub fn truncate_suffix(&mut self, last_kept_index: u64) -> Result<()> {
        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        let entries = read_all_entries(&mut file)?;
        let kept: Vec<_> = entries
            .into_iter()
            .filter(|e| e.index <= last_kept_index)
            .collect();
        rewrite_log_file(&self.path, &kept)?;
        self.file = OpenOptions::new().read(true).append(true).open(&self.path)?;
        if let Some(last) = kept.last() {
            self.last_index = last.index;
            self.last_term = last.term;
        } else {
            self.last_index = 0;
            self.last_term = 0;
        }
        Ok(())
    }

    pub fn build_append_request(&self, from_index: u64, max_entries: usize) -> Result<AppendRequest> {
        let prev_index = from_index.saturating_sub(1);
        let prev_term = self.term_at(prev_index)?.unwrap_or(0);
        let mut entries = self.entries_from(from_index)?;
        if entries.len() > max_entries {
            entries.truncate(max_entries);
        }
        Ok(AppendRequest {
            prev_index,
            prev_term,
            entries,
        })
    }

    pub fn apply_append_request(&mut self, req: &AppendRequest) -> Result<AppendResponse> {
        if req.prev_index > 0 {
            let local_prev_term = self.term_at(req.prev_index)?;
            if local_prev_term != Some(req.prev_term) {
                return Ok(AppendResponse {
                    success: false,
                    last_index: self.last_index,
                });
            }
        }

        for entry in &req.entries {
            if let Some(local_term) = self.term_at(entry.index)? {
                if local_term == entry.term {
                    continue;
                }
                self.truncate_suffix(entry.index.saturating_sub(1))?;
            }
            if entry.index > self.last_index {
                self.append_entry(entry)?;
            }
        }

        Ok(AppendResponse {
            success: true,
            last_index: self.last_index,
        })
    }

    fn term_at(&self, index: u64) -> Result<Option<u64>> {
        if index == 0 {
            return Ok(Some(0));
        }
        let mut file = OpenOptions::new().read(true).open(&self.path)?;
        let entries = read_all_entries(&mut file)?;
        Ok(entries.into_iter().find(|e| e.index == index).map(|e| e.term))
    }
}

pub fn deterministic_state_hash(entries: &[LogEntry]) -> u32 {
    let mut kv = BTreeMap::<Vec<u8>, Vec<u8>>::new();
    for entry in entries {
        match &entry.command {
            Command::Set { key, value } => {
                kv.insert(key.clone(), value.clone());
            }
            Command::Delete { key } => {
                kv.remove(key);
            }
        }
    }
    let mut hasher = Hasher::new();
    for (k, v) in kv {
        hasher.update(&(k.len() as u32).to_le_bytes());
        hasher.update(&(v.len() as u32).to_le_bytes());
        hasher.update(&k);
        hasher.update(&v);
    }
    hasher.finalize()
}

pub struct SnapshotInstall {
    tmp_path: PathBuf,
    target_path: PathBuf,
    expected_size: u64,
    expected_crc: u32,
    written: u64,
    file: File,
}

pub struct SnapshotInstaller;

impl SnapshotInstaller {
    pub fn begin(
        target_path: impl AsRef<Path>,
        expected_size: u64,
        expected_crc: u32,
    ) -> Result<SnapshotInstall> {
        let target_path = target_path.as_ref().to_path_buf();
        let tmp_path = target_path.with_extension("installing");
        let file = File::create(&tmp_path)?;
        Ok(SnapshotInstall {
            tmp_path,
            target_path,
            expected_size,
            expected_crc,
            written: 0,
            file,
        })
    }
}

impl SnapshotInstall {
    pub fn append_chunk(&mut self, offset: u64, chunk: &[u8]) -> Result<()> {
        if offset != self.written {
            return Err(Error::InvalidData(format!(
                "snapshot offset mismatch: got {offset}, expected {}",
                self.written
            )));
        }
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(chunk)?;
        self.written += chunk.len() as u64;
        Ok(())
    }

    pub fn finalize(mut self) -> Result<()> {
        if self.written != self.expected_size {
            return Err(Error::InvalidData(format!(
                "snapshot size mismatch: got {}, expected {}",
                self.written, self.expected_size
            )));
        }
        self.file.sync_data()?;
        drop(self.file);

        let mut file = OpenOptions::new().read(true).open(&self.tmp_path)?;
        let mut hasher = Hasher::new();
        let mut buf = [0u8; 16 * 1024];
        loop {
            let n = file.read(&mut buf)?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        let actual_crc = hasher.finalize();
        if actual_crc != self.expected_crc {
            return Err(Error::Corruption("snapshot crc mismatch"));
        }

        if self.target_path.exists() {
            fs::remove_file(&self.target_path)?;
        }
        fs::rename(&self.tmp_path, &self.target_path)?;
        Ok(())
    }
}

fn encode_entry(entry: &LogEntry) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&entry.index.to_le_bytes());
    out.extend_from_slice(&entry.term.to_le_bytes());
    match &entry.command {
        Command::Set { key, value } => {
            out.push(1);
            out.extend_from_slice(&(key.len() as u32).to_le_bytes());
            out.extend_from_slice(&(value.len() as u32).to_le_bytes());
            out.extend_from_slice(key);
            out.extend_from_slice(value);
        }
        Command::Delete { key } => {
            out.push(2);
            out.extend_from_slice(&(key.len() as u32).to_le_bytes());
            out.extend_from_slice(&0u32.to_le_bytes());
            out.extend_from_slice(key);
        }
    }
    out
}

fn decode_entry(payload: &[u8]) -> Result<LogEntry> {
    if payload.len() < 17 {
        return Err(Error::Corruption("log payload too short"));
    }
    let index = u64::from_le_bytes(
        payload[0..8]
            .try_into()
            .map_err(|_| Error::Corruption("bad log index bytes"))?,
    );
    let term = u64::from_le_bytes(
        payload[8..16]
            .try_into()
            .map_err(|_| Error::Corruption("bad log term bytes"))?,
    );
    let op = payload[16];
    if payload.len() < 25 {
        return Err(Error::Corruption("log payload command header too short"));
    }
    let key_len = u32::from_le_bytes(
        payload[17..21]
            .try_into()
            .map_err(|_| Error::Corruption("bad log key len"))?,
    ) as usize;
    let value_len = u32::from_le_bytes(
        payload[21..25]
            .try_into()
            .map_err(|_| Error::Corruption("bad log value len"))?,
    ) as usize;
    let expected = 25 + key_len + if op == 1 { value_len } else { 0 };
    if payload.len() != expected {
        return Err(Error::Corruption("log payload length mismatch"));
    }
    let key = payload[25..25 + key_len].to_vec();
    let command = match op {
        1 => {
            let value = payload[25 + key_len..expected].to_vec();
            Command::Set { key, value }
        }
        2 => Command::Delete { key },
        _ => return Err(Error::InvalidData(format!("unknown log op code {op}"))),
    };
    Ok(LogEntry {
        index,
        term,
        command,
    })
}

fn read_all_entries(file: &mut File) -> Result<Vec<LogEntry>> {
    file.seek(SeekFrom::Start(0))?;
    let mut out = Vec::new();
    loop {
        let mut header = [0u8; 12];
        match file.read_exact(&mut header) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(Error::Io(e)),
        }
        let magic = u32::from_le_bytes(
            header[0..4]
                .try_into()
                .map_err(|_| Error::Corruption("bad log header magic"))?,
        );
        if magic != LOG_MAGIC {
            return Err(Error::Corruption("replication log magic mismatch"));
        }
        let len = u32::from_le_bytes(
            header[4..8]
                .try_into()
                .map_err(|_| Error::Corruption("bad log len"))?,
        ) as usize;
        let expected_crc = u32::from_le_bytes(
            header[8..12]
                .try_into()
                .map_err(|_| Error::Corruption("bad log crc"))?,
        );
        let mut payload = vec![0u8; len];
        match file.read_exact(&mut payload) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(Error::Io(e)),
        }
        if crc32(&payload) != expected_crc {
            break;
        }
        out.push(decode_entry(&payload)?);
    }
    Ok(out)
}

fn rewrite_log_file(path: &Path, entries: &[LogEntry]) -> Result<()> {
    let tmp = path.with_extension("rewrite");
    let mut file = File::create(&tmp)?;
    for entry in entries {
        let payload = encode_entry(entry);
        file.write_all(&LOG_MAGIC.to_le_bytes())?;
        file.write_all(&(payload.len() as u32).to_le_bytes())?;
        file.write_all(&crc32(&payload).to_le_bytes())?;
        file.write_all(&payload)?;
    }
    file.sync_data()?;
    fs::rename(tmp, path)?;
    Ok(())
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut h = Hasher::new();
    h.update(bytes);
    h.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_file(name: &str) -> PathBuf {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        std::env::temp_dir().join(format!("tinydatabase-{name}-{stamp}.bin"))
    }

    #[test]
    fn log_append_and_reopen() {
        let path = unique_file("replog");
        let mut log = ReplicatedLog::open(&path).expect("open");
        log.append(
            1,
            Command::Set {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            },
        )
        .expect("append1");
        log.append(
            1,
            Command::Delete {
                key: b"a".to_vec(),
            },
        )
        .expect("append2");
        drop(log);

        let log2 = ReplicatedLog::open(&path).expect("reopen");
        assert_eq!(log2.last_index(), 2);
        assert_eq!(log2.last_term(), 1);
    }

    #[test]
    fn deterministic_hash_equal_for_same_final_state() {
        let a = vec![
            LogEntry {
                index: 1,
                term: 1,
                command: Command::Set {
                    key: b"k".to_vec(),
                    value: b"1".to_vec(),
                },
            },
            LogEntry {
                index: 2,
                term: 1,
                command: Command::Set {
                    key: b"k".to_vec(),
                    value: b"2".to_vec(),
                },
            },
        ];
        let b = vec![LogEntry {
            index: 10,
            term: 4,
            command: Command::Set {
                key: b"k".to_vec(),
                value: b"2".to_vec(),
            },
        }];
        assert_eq!(deterministic_state_hash(&a), deterministic_state_hash(&b));
    }

    #[test]
    fn snapshot_install_roundtrip() {
        let path = unique_file("snapshot");
        let bytes = b"hello-snapshot".to_vec();
        let crc = crc32(&bytes);
        let mut install = SnapshotInstaller::begin(&path, bytes.len() as u64, crc).expect("begin");
        install
            .append_chunk(0, &bytes[..5])
            .expect("chunk 1");
        install
            .append_chunk(5, &bytes[5..])
            .expect("chunk 2");
        install.finalize().expect("finalize");
        let got = std::fs::read(path).expect("read");
        assert_eq!(got, bytes);
    }

    #[test]
    fn wal_shipping_apply_success() {
        let leader_path = unique_file("leader");
        let follower_path = unique_file("follower");
        let mut leader = ReplicatedLog::open(&leader_path).expect("leader open");
        let mut follower = ReplicatedLog::open(&follower_path).expect("follower open");

        leader
            .append(
                1,
                Command::Set {
                    key: b"a".to_vec(),
                    value: b"1".to_vec(),
                },
            )
            .expect("append 1");
        leader
            .append(
                1,
                Command::Set {
                    key: b"b".to_vec(),
                    value: b"2".to_vec(),
                },
            )
            .expect("append 2");

        let req = leader.build_append_request(1, 1024).expect("build req");
        let res = follower.apply_append_request(&req).expect("apply");
        assert!(res.success);
        assert_eq!(res.last_index, 2);

        let l_hash = deterministic_state_hash(&leader.entries_from(1).expect("l entries"));
        let f_hash = deterministic_state_hash(&follower.entries_from(1).expect("f entries"));
        assert_eq!(l_hash, f_hash);
    }

    #[test]
    fn wal_shipping_conflict_truncates_and_overwrites() {
        let leader_path = unique_file("leader-conflict");
        let follower_path = unique_file("follower-conflict");
        let mut leader = ReplicatedLog::open(&leader_path).expect("leader open");
        let mut follower = ReplicatedLog::open(&follower_path).expect("follower open");

        leader
            .append(
                1,
                Command::Set {
                    key: b"x".to_vec(),
                    value: b"v1".to_vec(),
                },
            )
            .expect("leader e1");
        leader
            .append(
                2,
                Command::Set {
                    key: b"x".to_vec(),
                    value: b"v2".to_vec(),
                },
            )
            .expect("leader e2");

        follower
            .append(
                1,
                Command::Set {
                    key: b"x".to_vec(),
                    value: b"old".to_vec(),
                },
            )
            .expect("follower e1");
        follower
            .append(
                3,
                Command::Set {
                    key: b"x".to_vec(),
                    value: b"divergent".to_vec(),
                },
            )
            .expect("follower e2");

        let req = leader.build_append_request(2, 1024).expect("build req");
        let res = follower.apply_append_request(&req).expect("apply");
        assert!(res.success);
        assert_eq!(res.last_index, 2);

        let l_hash = deterministic_state_hash(&leader.entries_from(1).expect("l entries"));
        let f_hash = deterministic_state_hash(&follower.entries_from(1).expect("f entries"));
        assert_eq!(l_hash, f_hash);
    }
}
