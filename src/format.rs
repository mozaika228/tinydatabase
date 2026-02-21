use crate::error::{Error, Result};

pub const MAGIC: u32 = 0x5444_4231; // TDB1
pub const OP_SET: u8 = 1;
pub const OP_DELETE: u8 = 2;
pub const OP_BEGIN: u8 = 3;
pub const OP_COMMIT: u8 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    Begin { tx_id: u64 },
    Set { tx_id: u64, key: Vec<u8>, value: Vec<u8> },
    Delete { tx_id: u64, key: Vec<u8> },
    Commit { tx_id: u64 },
}

impl Record {
    pub fn encode_payload(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        match self {
            Record::Begin { tx_id } => {
                buf.push(OP_BEGIN);
                buf.extend_from_slice(&tx_id.to_le_bytes());
            }
            Record::Set { tx_id, key, value } => {
                buf.push(OP_SET);
                buf.extend_from_slice(&tx_id.to_le_bytes());
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
                buf.extend_from_slice(value);
            }
            Record::Delete { tx_id, key } => {
                buf.push(OP_DELETE);
                buf.extend_from_slice(&tx_id.to_le_bytes());
                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
            }
            Record::Commit { tx_id } => {
                buf.push(OP_COMMIT);
                buf.extend_from_slice(&tx_id.to_le_bytes());
            }
        }
        buf
    }

    pub fn decode_payload(payload: &[u8]) -> Result<Self> {
        if payload.len() < 9 {
            return Err(Error::Corruption("payload too short"));
        }
        let op = payload[0];
        let tx_id = u64::from_le_bytes(
            payload[1..9]
                .try_into()
                .map_err(|_| Error::Corruption("bad tx id bytes"))?,
        );
        match op {
            OP_BEGIN => Ok(Record::Begin { tx_id }),
            OP_COMMIT => Ok(Record::Commit { tx_id }),
            OP_SET => {
                if payload.len() < 17 {
                    return Err(Error::Corruption("set payload too short"));
                }
                let key_len = u32::from_le_bytes(
                    payload[9..13]
                        .try_into()
                        .map_err(|_| Error::Corruption("bad key len"))?,
                ) as usize;
                let value_len = u32::from_le_bytes(
                    payload[13..17]
                        .try_into()
                        .map_err(|_| Error::Corruption("bad value len"))?,
                ) as usize;
                let expected = 17 + key_len + value_len;
                if payload.len() != expected {
                    return Err(Error::Corruption("set payload length mismatch"));
                }
                let key = payload[17..17 + key_len].to_vec();
                let value = payload[17 + key_len..expected].to_vec();
                Ok(Record::Set { tx_id, key, value })
            }
            OP_DELETE => {
                if payload.len() < 13 {
                    return Err(Error::Corruption("delete payload too short"));
                }
                let key_len = u32::from_le_bytes(
                    payload[9..13]
                        .try_into()
                        .map_err(|_| Error::Corruption("bad delete key len"))?,
                ) as usize;
                let expected = 13 + key_len;
                if payload.len() != expected {
                    return Err(Error::Corruption("delete payload length mismatch"));
                }
                let key = payload[13..expected].to_vec();
                Ok(Record::Delete { tx_id, key })
            }
            _ => Err(Error::InvalidData(format!("unknown op code {op}"))),
        }
    }
}

