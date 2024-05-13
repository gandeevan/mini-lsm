use crate::error::{Error, Result};
use memoffset::offset_of;
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::FromPrimitive;
use std::array::TryFromSliceError;

// BLOCK_SIZE should not be greater than 65535
// since only 2 bytes are allocated for the `size`
// field in the log record.
pub const DEFAULT_BLOCK_SIZE: usize = 32 * 1024;
pub const LOG_RECORD_HEADER_SIZE: usize = 7;
pub const MIN_RECORD_SIZE: usize = LOG_RECORD_HEADER_SIZE + 1; // CRC (4B) + Size (2B) + Type (1B) + Payload (1B)
pub const BLOCK_PADDING: [u8; LOG_RECORD_HEADER_SIZE] = [0, 0, 0, 0, 0, 0, 0];
pub const DEFAULT_BUFFER_CAPACITY: usize = 128 * 1024; // TODO: move this to a constants file

#[derive(Clone, Copy, FromPrimitive, ToPrimitive)]
pub enum RecordType {
    None = 0,
    First = 1,
    Middle = 2,
    Last = 3,
    Full = 4,
}

impl RecordType {
    pub fn value(&self) -> u8 {
        *self as u8
    }
}

// Record Format:
//
// https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format
//
// +---------+-----------+-----------+--- ... ---+
// |CRC (4B) | Size (2B) | Type (1B) | Payload   |
// +---------+-----------+-----------+--- ... ---+
//
// CRC = 32bit hash computed over the payload using CRC
// Size = Length of the payload data
// Type = Type of record
//      (kZeroType, kFullType, kFirstType, kLastType, kMiddleType )
// The type is used to group a bunch of records together to represent
// blocks that are larger than kBlockSize
// Payload = Byte stream as long as specified by the payload size
pub struct LogRecord<'a> {
    pub crc: u32,
    pub size: u16,
    pub rtype: RecordType,
    pub payload: &'a [u8],
}

fn bytes_to_type<'a, T: TryFrom<&'a [u8], Error = TryFromSliceError>>(
    bytes: &'a [u8],
) -> Result<T> {
    bytes.try_into().map_err(Error::TryFromSlice)
}

// TODO: add unit tests
impl<'a> LogRecord<'a> {
    pub fn validate_crc(&self) -> Result<()> {
        let actual_crc = crc32c::crc32c(self.payload);
        if self.crc == actual_crc {
            return Ok(());
        }
        Err(Error::InvalidCrc(self.crc, actual_crc))
    }

    #[allow(dead_code)]
    pub fn from_serialized_bytes(bytes: &[u8]) -> Result<LogRecord> {
        if bytes.len() < MIN_RECORD_SIZE {
            return Err(Error::WalRecordTooSmall(bytes.len(), MIN_RECORD_SIZE));
        }

        let crc_offset = offset_of!(LogRecord, crc);
        let size_offset = offset_of!(LogRecord, size);
        let rtype_offset = offset_of!(LogRecord, rtype);
        let payload_offset = offset_of!(LogRecord, payload);
        let payload_size = u16::from_be_bytes(bytes_to_type(&bytes[size_offset..size_offset + 2])?);

        let record_type = u8::from_be_bytes(bytes_to_type(&bytes[rtype_offset..rtype_offset + 1])?);
        Ok(LogRecord {
            crc: u32::from_be_bytes(bytes_to_type(&bytes[crc_offset..crc_offset + 4])?),
            size: payload_size,
            rtype: RecordType::from_u8(record_type).ok_or(Error::InvalidRecordType(record_type))?,
            payload: &bytes[payload_offset
                ..(payload_offset + (TryInto::<usize>::try_into(payload_size).unwrap()))],
        })
    }

    pub fn new(rtype: RecordType, payload: &[u8]) -> LogRecord {
        LogRecord {
            crc: crc32c::crc32c(payload),
            rtype,
            size: payload.len().try_into().unwrap(),
            payload,
        }
    }

    pub fn len(&self) -> usize {
        // Header (7B) = CRC (4B) + Size (2B) + Type (1B)
        LOG_RECORD_HEADER_SIZE + self.payload.len()
    }
}
