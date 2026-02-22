use crate::error::{Error, Result};
use std::collections::BTreeMap;
use std::hash::Hasher;
use std::fs::File;
use std::io::{BufReader, Read, Write};
use std::path::Path;

const SST_MAGIC_V1: &[u8; 8] = b"TDBSST01";
const SST_MAGIC_V2: &[u8; 8] = b"TDBSST02";
const IDX_MAGIC_V1: &[u8; 8] = b"TDBIDX01";
const MANIFEST_MAGIC_V1: &[u8; 8] = b"TDBMAN01";
const MANIFEST_MAGIC_V2: &[u8; 8] = b"TDBMAN02";
const BLOOM_BITS: usize = 2048;
const BLOOM_BYTES: usize = BLOOM_BITS / 8;
const BLOOM_HASHES: u64 = 3;

#[derive(Debug, Clone)]
pub struct Manifest {
    pub last_commit_ts: u64,
    pub next_tx_id: u64,
    pub last_flushed_commit_ts: u64,
    pub segments: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SegmentMeta {
    pub min_key: Option<Vec<u8>>,
    pub max_key: Option<Vec<u8>>,
    pub bloom: [u8; BLOOM_BYTES],
}

pub fn write_table(path: &Path, data: &BTreeMap<Vec<u8>, Option<Vec<u8>>>) -> Result<()> {
    let mut file = File::create(path)?;
    file.write_all(SST_MAGIC_V2)?;
    file.write_all(&(data.len() as u64).to_le_bytes())?;
    for (key, value) in data {
        file.write_all(&(key.len() as u32).to_le_bytes())?;
        match value {
            Some(v) => {
                file.write_all(&[0])?;
                file.write_all(&(v.len() as u32).to_le_bytes())?;
                file.write_all(key)?;
                file.write_all(v)?;
            }
            None => {
                file.write_all(&[1])?;
                file.write_all(&0u32.to_le_bytes())?;
                file.write_all(key)?;
            }
        }
    }
    file.sync_data()?;
    Ok(())
}

pub fn read_table(path: &Path) -> Result<BTreeMap<Vec<u8>, Option<Vec<u8>>>> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)?;
    match &magic {
        SST_MAGIC_V2 => read_table_v2(file),
        SST_MAGIC_V1 => read_table_v1(file),
        _ => Err(Error::Corruption("sstable magic mismatch")),
    }
}

pub fn read_value(path: &Path, lookup_key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)?;
    match &magic {
        SST_MAGIC_V2 => read_value_v2(file, lookup_key),
        SST_MAGIC_V1 => read_value_v1(file, lookup_key),
        _ => Err(Error::Corruption("sstable magic mismatch")),
    }
}

pub fn build_segment_meta(data: &BTreeMap<Vec<u8>, Option<Vec<u8>>>) -> SegmentMeta {
    let min_key = data.keys().next().cloned();
    let max_key = data.keys().next_back().cloned();
    let mut bloom = [0u8; BLOOM_BYTES];
    for key in data.keys() {
        for i in 0..BLOOM_HASHES {
            let bit = bloom_hash(key, i) as usize % BLOOM_BITS;
            bloom[bit / 8] |= 1u8 << (bit % 8);
        }
    }
    SegmentMeta {
        min_key,
        max_key,
        bloom,
    }
}

pub fn may_contain_key(meta: &SegmentMeta, lookup_key: &[u8]) -> bool {
    let Some(min_key) = &meta.min_key else {
        return false;
    };
    let Some(max_key) = &meta.max_key else {
        return false;
    };
    if lookup_key < min_key.as_slice() || lookup_key > max_key.as_slice() {
        return false;
    }
    for i in 0..BLOOM_HASHES {
        let bit = bloom_hash(lookup_key, i) as usize % BLOOM_BITS;
        if (meta.bloom[bit / 8] & (1u8 << (bit % 8))) == 0 {
            return false;
        }
    }
    true
}

pub fn write_index(path: &Path, meta: &SegmentMeta) -> Result<()> {
    let mut file = File::create(path)?;
    file.write_all(IDX_MAGIC_V1)?;
    match &meta.min_key {
        Some(min_key) => {
            file.write_all(&[1])?;
            file.write_all(&(min_key.len() as u32).to_le_bytes())?;
            file.write_all(min_key)?;
        }
        None => file.write_all(&[0])?,
    }
    match &meta.max_key {
        Some(max_key) => {
            file.write_all(&[1])?;
            file.write_all(&(max_key.len() as u32).to_le_bytes())?;
            file.write_all(max_key)?;
        }
        None => file.write_all(&[0])?,
    }
    file.write_all(&meta.bloom)?;
    file.sync_data()?;
    Ok(())
}

pub fn read_index(path: &Path) -> Result<SegmentMeta> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)?;
    if &magic != IDX_MAGIC_V1 {
        return Err(Error::Corruption("index magic mismatch"));
    }
    let min_key = read_optional_key(&mut file)?;
    let max_key = read_optional_key(&mut file)?;
    let mut bloom = [0u8; BLOOM_BYTES];
    file.read_exact(&mut bloom)?;
    Ok(SegmentMeta {
        min_key,
        max_key,
        bloom,
    })
}

pub fn write_manifest(path: &Path, manifest: &Manifest) -> Result<()> {
    let mut file = File::create(path)?;
    file.write_all(MANIFEST_MAGIC_V2)?;
    file.write_all(&manifest.last_commit_ts.to_le_bytes())?;
    file.write_all(&manifest.next_tx_id.to_le_bytes())?;
    file.write_all(&manifest.last_flushed_commit_ts.to_le_bytes())?;
    file.write_all(&(manifest.segments.len() as u32).to_le_bytes())?;
    for seg in &manifest.segments {
        let bytes = seg.as_bytes();
        if bytes.len() > u16::MAX as usize {
            return Err(Error::InvalidData("segment name too long".to_string()));
        }
        file.write_all(&(bytes.len() as u16).to_le_bytes())?;
        file.write_all(bytes)?;
    }
    file.sync_data()?;
    Ok(())
}

pub fn read_manifest(path: &Path) -> Result<Manifest> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)?;
    match &magic {
        MANIFEST_MAGIC_V2 => read_manifest_v2(file),
        MANIFEST_MAGIC_V1 => read_manifest_v1(file),
        _ => Err(Error::Corruption("manifest magic mismatch")),
    }
}

fn read_table_v2(mut file: File) -> Result<BTreeMap<Vec<u8>, Option<Vec<u8>>>> {
    let mut count_bytes = [0u8; 8];
    file.read_exact(&mut count_bytes)?;
    let count = u64::from_le_bytes(count_bytes);
    let mut out = BTreeMap::new();
    for _ in 0..count {
        let mut key_len_bytes = [0u8; 4];
        let mut value_len_bytes = [0u8; 4];
        let mut tombstone = [0u8; 1];
        file.read_exact(&mut key_len_bytes)?;
        file.read_exact(&mut tombstone)?;
        file.read_exact(&mut value_len_bytes)?;
        let key_len = u32::from_le_bytes(key_len_bytes) as usize;
        let value_len = u32::from_le_bytes(value_len_bytes) as usize;
        let mut key = vec![0u8; key_len];
        file.read_exact(&mut key)?;
        let value = if tombstone[0] == 1 {
            None
        } else if tombstone[0] == 0 {
            let mut v = vec![0u8; value_len];
            file.read_exact(&mut v)?;
            Some(v)
        } else {
            return Err(Error::Corruption("sstable tombstone flag is invalid"));
        };
        out.insert(key, value);
    }
    Ok(out)
}

fn read_value_v2(file: File, lookup_key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
    let mut reader = BufReader::new(file);
    let mut count_bytes = [0u8; 8];
    reader.read_exact(&mut count_bytes)?;
    let count = u64::from_le_bytes(count_bytes);
    for _ in 0..count {
        let mut key_len_bytes = [0u8; 4];
        let mut value_len_bytes = [0u8; 4];
        let mut tombstone = [0u8; 1];
        reader.read_exact(&mut key_len_bytes)?;
        reader.read_exact(&mut tombstone)?;
        reader.read_exact(&mut value_len_bytes)?;
        let key_len = u32::from_le_bytes(key_len_bytes) as usize;
        let value_len = u32::from_le_bytes(value_len_bytes) as usize;
        let mut key = vec![0u8; key_len];
        reader.read_exact(&mut key)?;

        match key.as_slice().cmp(lookup_key) {
            std::cmp::Ordering::Less => {
                if tombstone[0] == 0 {
                    let mut skip = vec![0u8; value_len];
                    reader.read_exact(&mut skip)?;
                } else if tombstone[0] != 1 {
                    return Err(Error::Corruption("sstable tombstone flag is invalid"));
                }
            }
            std::cmp::Ordering::Equal => {
                if tombstone[0] == 1 {
                    return Ok(Some(None));
                }
                if tombstone[0] != 0 {
                    return Err(Error::Corruption("sstable tombstone flag is invalid"));
                }
                let mut value = vec![0u8; value_len];
                reader.read_exact(&mut value)?;
                return Ok(Some(Some(value)));
            }
            std::cmp::Ordering::Greater => {
                return Ok(None);
            }
        }
    }
    Ok(None)
}

fn read_table_v1(mut file: File) -> Result<BTreeMap<Vec<u8>, Option<Vec<u8>>>> {
    let mut count_bytes = [0u8; 8];
    file.read_exact(&mut count_bytes)?;
    let count = u64::from_le_bytes(count_bytes);
    let mut out = BTreeMap::new();
    for _ in 0..count {
        let (k, v) = read_v1_entry(&mut file)?;
        out.insert(k, Some(v));
    }
    Ok(out)
}

fn read_value_v1(file: File, lookup_key: &[u8]) -> Result<Option<Option<Vec<u8>>>> {
    let mut reader = BufReader::new(file);
    let mut count_bytes = [0u8; 8];
    reader.read_exact(&mut count_bytes)?;
    let count = u64::from_le_bytes(count_bytes);
    for _ in 0..count {
        let (k, v) = read_v1_entry_reader(&mut reader)?;
        match k.as_slice().cmp(lookup_key) {
            std::cmp::Ordering::Less => {}
            std::cmp::Ordering::Equal => return Ok(Some(Some(v))),
            std::cmp::Ordering::Greater => return Ok(None),
        }
    }
    Ok(None)
}

fn read_manifest_v2(mut file: File) -> Result<Manifest> {
    let mut commit_bytes = [0u8; 8];
    let mut tx_bytes = [0u8; 8];
    let mut flushed_bytes = [0u8; 8];
    let mut count_bytes = [0u8; 4];
    file.read_exact(&mut commit_bytes)?;
    file.read_exact(&mut tx_bytes)?;
    file.read_exact(&mut flushed_bytes)?;
    file.read_exact(&mut count_bytes)?;

    let last_commit_ts = u64::from_le_bytes(commit_bytes);
    let next_tx_id = u64::from_le_bytes(tx_bytes);
    let last_flushed_commit_ts = u64::from_le_bytes(flushed_bytes);
    let count = u32::from_le_bytes(count_bytes);
    let mut segments = Vec::with_capacity(count as usize);
    for _ in 0..count {
        segments.push(read_segment_name(&mut file)?);
    }
    Ok(Manifest {
        last_commit_ts,
        next_tx_id,
        last_flushed_commit_ts,
        segments,
    })
}

fn read_manifest_v1(mut file: File) -> Result<Manifest> {
    let mut commit_bytes = [0u8; 8];
    let mut tx_bytes = [0u8; 8];
    file.read_exact(&mut commit_bytes)?;
    file.read_exact(&mut tx_bytes)?;
    let last_commit_ts = u64::from_le_bytes(commit_bytes);
    let next_tx_id = u64::from_le_bytes(tx_bytes);

    let mut flag = [0u8; 1];
    file.read_exact(&mut flag)?;
    let segments = match flag[0] {
        0 => Vec::new(),
        1 => vec![read_segment_name(&mut file)?],
        _ => return Err(Error::Corruption("manifest segment flag is invalid")),
    };
    Ok(Manifest {
        last_commit_ts,
        next_tx_id,
        last_flushed_commit_ts: last_commit_ts,
        segments,
    })
}

fn read_segment_name(file: &mut File) -> Result<String> {
    let mut len_bytes = [0u8; 2];
    file.read_exact(&mut len_bytes)?;
    let len = u16::from_le_bytes(len_bytes) as usize;
    let mut name_bytes = vec![0u8; len];
    file.read_exact(&mut name_bytes)?;
    String::from_utf8(name_bytes)
        .map_err(|_| Error::InvalidData("manifest segment name is not utf-8".to_string()))
}

fn read_optional_key(file: &mut File) -> Result<Option<Vec<u8>>> {
    let mut flag = [0u8; 1];
    file.read_exact(&mut flag)?;
    if flag[0] == 0 {
        return Ok(None);
    }
    if flag[0] != 1 {
        return Err(Error::Corruption("index optional key flag is invalid"));
    }
    let mut len_bytes = [0u8; 4];
    file.read_exact(&mut len_bytes)?;
    let len = u32::from_le_bytes(len_bytes) as usize;
    let mut out = vec![0u8; len];
    file.read_exact(&mut out)?;
    Ok(Some(out))
}

fn read_v1_entry(file: &mut File) -> Result<(Vec<u8>, Vec<u8>)> {
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

fn read_v1_entry_reader<R: Read>(reader: &mut R) -> Result<(Vec<u8>, Vec<u8>)> {
    let mut kb = [0u8; 4];
    let mut vb = [0u8; 4];
    reader.read_exact(&mut kb)?;
    reader.read_exact(&mut vb)?;
    let klen = u32::from_le_bytes(kb) as usize;
    let vlen = u32::from_le_bytes(vb) as usize;
    let mut k = vec![0; klen];
    let mut v = vec![0; vlen];
    reader.read_exact(&mut k)?;
    reader.read_exact(&mut v)?;
    Ok((k, v))
}

fn bloom_hash(key: &[u8], seed: u64) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hasher.write_u64(seed);
    hasher.write(key);
    hasher.finish()
}
