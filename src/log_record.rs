use crate::error::{Error, Result};
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::FromPrimitive;
use std::{array::TryFromSliceError, mem};

// BLOCK_SIZE should not be greater than 65535
// since only 2 bytes are allocated for the `size`
// field in the log record.
pub const DEFAULT_BLOCK_SIZE: usize = 32 * 1024;
pub const LOG_RECORD_HEADER_SIZE: usize = 7; // CRC (4B) + Size (2B) + Type (1B)
pub const MIN_RECORD_SIZE: usize = LOG_RECORD_HEADER_SIZE + 1; // CRC (4B) + Size (2B) + Type (1B) + Payload (1B)
pub const BLOCK_PADDING: [u8; LOG_RECORD_HEADER_SIZE] = [0, 0, 0, 0, 0, 0, 0];
pub const DEFAULT_BUFFER_CAPACITY: usize = 128 * 1024; // TODO: move this to a constants file

const CRC_OFFSET: usize = 0;
const SIZE_OFFSET: usize = 4;
const TYPE_OFFSET: usize = 6;
const PAYLOAD_OFFSET: usize = 7;

#[derive(Clone, Copy, FromPrimitive, ToPrimitive, PartialEq, Debug)]
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

/// Represents a log record.
///
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
#[derive(Debug, PartialEq)]
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

impl<'a> LogRecord<'a> {
    /// Validates the CRC (Cyclic Redundancy Check) of the log record.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the CRC is valid.
    ///
    /// Returns `Err(Error::InvalidCrc)` if the CRC is invalid.
    pub fn validate_crc(&self) -> Result<()> {
        let actual_crc = crc32c::crc32c(self.payload);
        if self.crc == actual_crc {
            return Ok(());
        }
        Err(Error::InvalidCrc(self.crc, actual_crc))
    }

    /// Creates a `LogRecord` from serialized bytes.
    /// # Arguments
    ///
    /// * `bytes` - The serialized bytes representing the log record.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the deserialized `LogRecord` if successful.
    ///
    /// Returns `Err(Error::WalRecordTooSmall)` if the serialized bytes are too small to form a valid log record.
    pub fn from_serialized_bytes(bytes: &[u8]) -> Result<LogRecord> {
        let phantom_record = LogRecord::new(RecordType::Full, bytes);
        if bytes.len() < MIN_RECORD_SIZE {
            return Err(Error::WalRecordTooSmall(bytes.len(), MIN_RECORD_SIZE));
        }
        let payload_size = u16::from_be_bytes(bytes_to_type(
            &bytes[SIZE_OFFSET..SIZE_OFFSET + mem::size_of_val(&phantom_record.size)],
        )?);

        let record_type = u8::from_be_bytes(bytes_to_type(
            &bytes[TYPE_OFFSET..TYPE_OFFSET + mem::size_of_val(&phantom_record.rtype)],
        )?);
        Ok(LogRecord {
            crc: u32::from_be_bytes(bytes_to_type(
                &bytes[CRC_OFFSET..CRC_OFFSET + mem::size_of_val(&phantom_record.crc)],
            )?),
            size: payload_size,
            rtype: RecordType::from_u8(record_type).ok_or(Error::InvalidRecordType(record_type))?,
            payload: &bytes[PAYLOAD_OFFSET
                ..(PAYLOAD_OFFSET + (TryInto::<usize>::try_into(payload_size).unwrap()))],
        })
    }

    /// Creates a new `LogRecord`.
    ///
    /// # Arguments
    ///
    /// * `rtype` - The type of the log record.
    /// * `payload` - The payload of the log record.
    ///
    /// # Returns
    ///
    /// Returns the newly created `LogRecord`.
    pub fn new(rtype: RecordType, payload: &[u8]) -> LogRecord {
        LogRecord {
            crc: crc32c::crc32c(payload),
            rtype,
            size: payload.len().try_into().unwrap(),
            payload,
        }
    }

    /// Returns the length of the log record.
    ///
    /// # Returns
    ///
    /// Returns the length of the log record in bytes.
    pub fn len(&self) -> usize {
        // Header (7B) = CRC (4B) + Size (2B) + Type (1B)
        LOG_RECORD_HEADER_SIZE + self.payload.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_crc_valid() {
        let payload = b"test payload";
        let crc = crc32c::crc32c(payload);
        let record = LogRecord {
            crc,
            size: payload.len() as u16,
            rtype: RecordType::Full,
            payload,
        };
        assert_eq!(record.validate_crc().is_ok(), true);
    }

    #[test]
    fn test_validate_crc_invalid() {
        let payload = b"test payload";
        let crc = crc32c::crc32c(b"invalid payload");
        let record = LogRecord {
            crc,
            size: payload.len() as u16,
            rtype: RecordType::Full,
            payload,
        };
        record.validate_crc().expect_err("Expected an error");
    }

    #[test]
    fn test_from_serialized_bytes_valid() {
        let payload = b"test payload";
        let crc = crc32c::crc32c(payload);
        let serialized_bytes: Vec<u8> = [
            &[(crc >> 24) as u8] as &[u8],
            &[(crc >> 16) as u8],
            &[(crc >> 8) as u8],
            &[crc as u8],
            &[(payload.len() >> 8) as u8],
            &[payload.len() as u8],
            &[RecordType::Full as u8],
            payload,
        ]
        .concat();
        let record = LogRecord::from_serialized_bytes(&serialized_bytes).unwrap();
        assert_eq!(record.crc, crc);
        assert_eq!(record.size, payload.len() as u16);
        assert_eq!(record.rtype, RecordType::Full);
        assert_eq!(record.payload, payload);
    }

    #[test]
    fn test_from_serialized_bytes_invalid() {
        let serialized_bytes = [0u8; MIN_RECORD_SIZE - 1];
        let result = LogRecord::from_serialized_bytes(&serialized_bytes);
        match result {
            Err(Error::WalRecordTooSmall(_, _)) => {}
            _ => panic!("Expected WalRecordTooSmall error"),
        }
    }

    #[test]
    fn test_new() {
        let payload = b"test payload";
        let record = LogRecord::new(RecordType::Full, payload);
        let crc = crc32c::crc32c(payload);
        assert_eq!(record.crc, crc);
        assert_eq!(record.size, payload.len() as u16);
        assert_eq!(record.rtype, RecordType::Full);
        assert_eq!(record.payload, payload);
    }

    #[test]
    fn test_len() {
        let payload = b"test payload";
        let record = LogRecord::new(RecordType::Full, payload);
        let expected_len = LOG_RECORD_HEADER_SIZE + payload.len();
        assert_eq!(record.len(), expected_len);
    }
}
