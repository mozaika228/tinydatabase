use crate::error::{Error, Result};
use crate::format::{Record, MAGIC};
use crc32fast::Hasher;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

pub struct Wal {
    file: File,
}

impl Wal {
    pub fn open(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        Ok(Self { file })
    }

    pub fn append(&mut self, record: &Record) -> Result<()> {
        let payload = record.encode_payload();
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let crc = hasher.finalize();
        let len = payload.len() as u32;

        self.file.write_all(&MAGIC.to_le_bytes())?;
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&payload)?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_data()?;
        Ok(())
    }

    pub fn reset(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.sync_data()?;
        Ok(())
    }

    pub fn read_all(path: &Path) -> Result<Vec<Record>> {
        let mut file = OpenOptions::new().read(true).open(path)?;
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
                    .map_err(|_| Error::Corruption("bad wal header magic"))?,
            );
            if magic != MAGIC {
                return Err(Error::Corruption("wal magic mismatch"));
            }
            let len = u32::from_le_bytes(
                header[4..8]
                    .try_into()
                    .map_err(|_| Error::Corruption("bad wal len"))?,
            ) as usize;
            let expected_crc = u32::from_le_bytes(
                header[8..12]
                    .try_into()
                    .map_err(|_| Error::Corruption("bad wal crc"))?,
            );
            let mut payload = vec![0u8; len];
            match file.read_exact(&mut payload) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(Error::Io(e)),
            }

            let mut hasher = Hasher::new();
            hasher.update(&payload);
            let crc = hasher.finalize();
            if crc != expected_crc {
                break;
            }

            out.push(Record::decode_payload(&payload)?);
        }
        Ok(out)
    }
}
