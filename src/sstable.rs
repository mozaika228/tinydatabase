use crate::error::{Error, Result};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

const SST_MAGIC: &[u8; 8] = b"TDBSST01";
const MANIFEST_MAGIC: &[u8; 8] = b"TDBMAN01";

#[derive(Debug, Clone)]
pub struct Manifest {
    pub last_commit_ts: u64,
    pub next_tx_id: u64,
    pub current_segment: Option<String>,
}

pub fn write_table(path: &Path, data: &BTreeMap<Vec<u8>, Vec<u8>>) -> Result<()> {
    let mut file = File::create(path)?;
    file.write_all(SST_MAGIC)?;
    file.write_all(&(data.len() as u64).to_le_bytes())?;
    for (key, value) in data {
        file.write_all(&(key.len() as u32).to_le_bytes())?;
        file.write_all(&(value.len() as u32).to_le_bytes())?;
        file.write_all(key)?;
        file.write_all(value)?;
    }
    file.sync_data()?;
    Ok(())
}

pub fn read_table(path: &Path) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)?;
    if &magic != SST_MAGIC {
        return Err(Error::Corruption("sstable magic mismatch"));
    }

    let mut count_bytes = [0u8; 8];
    file.read_exact(&mut count_bytes)?;
    let count = u64::from_le_bytes(count_bytes);
    let mut out = BTreeMap::new();
    for _ in 0..count {
        let (k, v) = read_entry(&mut file)?;
        out.insert(k, v);
    }
    Ok(out)
}

pub fn write_manifest(path: &Path, manifest: &Manifest) -> Result<()> {
    let mut file = File::create(path)?;
    file.write_all(MANIFEST_MAGIC)?;
    file.write_all(&manifest.last_commit_ts.to_le_bytes())?;
    file.write_all(&manifest.next_tx_id.to_le_bytes())?;
    match &manifest.current_segment {
        Some(name) => {
            file.write_all(&[1])?;
            let bytes = name.as_bytes();
            if bytes.len() > u16::MAX as usize {
                return Err(Error::InvalidData("segment name too long".to_string()));
            }
            file.write_all(&(bytes.len() as u16).to_le_bytes())?;
            file.write_all(bytes)?;
        }
        None => {
            file.write_all(&[0])?;
        }
    }
    file.sync_data()?;
    Ok(())
}

pub fn read_manifest(path: &Path) -> Result<Manifest> {
    let mut file = File::open(path)?;
    let mut magic = [0u8; 8];
    file.read_exact(&mut magic)?;
    if &magic != MANIFEST_MAGIC {
        return Err(Error::Corruption("manifest magic mismatch"));
    }

    let mut commit_bytes = [0u8; 8];
    let mut tx_bytes = [0u8; 8];
    file.read_exact(&mut commit_bytes)?;
    file.read_exact(&mut tx_bytes)?;
    let last_commit_ts = u64::from_le_bytes(commit_bytes);
    let next_tx_id = u64::from_le_bytes(tx_bytes);

    let mut flag = [0u8; 1];
    file.read_exact(&mut flag)?;
    let current_segment = match flag[0] {
        0 => None,
        1 => {
            let mut len_bytes = [0u8; 2];
            file.read_exact(&mut len_bytes)?;
            let len = u16::from_le_bytes(len_bytes) as usize;
            let mut name_bytes = vec![0u8; len];
            file.read_exact(&mut name_bytes)?;
            Some(
                String::from_utf8(name_bytes).map_err(|_| {
                    Error::InvalidData("manifest segment name is not utf-8".to_string())
                })?,
            )
        }
        _ => return Err(Error::Corruption("manifest segment flag is invalid")),
    };

    Ok(Manifest {
        last_commit_ts,
        next_tx_id,
        current_segment,
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
