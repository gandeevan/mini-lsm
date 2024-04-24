use crate::file_writer::FileWriter;
use std::array::TryFromSliceError;
use std::fmt;
use std::{cmp::min};
use crate::buffer_consumer::BufferConsumer;
use crate::error::{Result, Error};
use memoffset::offset_of;
use num_traits::{FromPrimitive};
use num_derive::{FromPrimitive, ToPrimitive};


// BLOCK_SIZE should not be greater than 65535 
// since only 2 bytes are allocated for the `size`
// field in the log record.  
const DEFAULT_BLOCK_SIZE: usize = 32 * 1024; 
const LOG_RECORD_HEADER_SIZE: usize = 7;
const MIN_RECORD_SIZE: usize = LOG_RECORD_HEADER_SIZE + 1;  // CRC (4B) + Size (2B) + Type (1B) + Payload (1B)
const BLOCK_PADDING: [u8; LOG_RECORD_HEADER_SIZE] = [0,0,0,0,0,0,0];

#[derive(Clone, Copy, FromPrimitive, ToPrimitive)]
enum RecordType {
    None = 0,
    First = 1, 
    Middle = 2, 
    Last = 3,
    Full = 4, 
}

impl RecordType {
    fn value(&self) -> u8 {
        *self as u8
    }
}

struct LogRecord<'a> {
    crc: u32,
    size: u16,
    rtype: RecordType,
    payload: &'a [u8],
}

fn bytes_to_type<'a, T: TryFrom<&'a[u8], Error = TryFromSliceError>>(bytes: &'a[u8]) -> Result<T> {
    bytes.try_into().map_err(Error::TryFromSliceError)
}

impl<'a> LogRecord<'a> {
    pub fn from_serialized_bytes(bytes: &[u8]) -> Result<LogRecord> {
        if bytes.len() < MIN_RECORD_SIZE {
            return Err(Error::WalRecordTooSmall(bytes.len(), MIN_RECORD_SIZE));
        }

        let crc_offset = offset_of!(LogRecord, crc);
        let size_offset = offset_of!(LogRecord, size);
        let rtype_offset =  offset_of!(LogRecord, rtype);
        let payload_offset = offset_of!(LogRecord, payload);

        let record_type = u8::from_le_bytes(bytes_to_type(&bytes[rtype_offset..rtype_offset+1])?);
        Ok(LogRecord {
            crc: u32::from_le_bytes(bytes_to_type(&bytes[crc_offset..crc_offset+4])?), 
            size: u16::from_le_bytes(bytes_to_type(&bytes[size_offset..size_offset+2])?),
            rtype: RecordType::from_u8(record_type).ok_or(Error::InvalidRecordType(record_type))?,
            payload: &bytes[payload_offset..], 
        })
    }

    pub fn new(rtype: RecordType, payload: &[u8]) -> LogRecord {
        LogRecord {
            crc: crc32c::crc32c(payload), 
            rtype: rtype, 
            size: payload.len().try_into().unwrap(), 
            payload: payload,
        }
    }

    pub fn len(&self) -> usize {
        // Header (7B) = CRC (4B) + Size (2B) + Type (1B) 
        return LOG_RECORD_HEADER_SIZE + self.payload.len();
    }
}


pub struct LogWriter {
    fw: FileWriter,
    block_pos: usize,
}

impl LogWriter {
    pub fn new(filepath: &str, truncate: bool) -> Result<LogWriter> {
        let file_writer = FileWriter::new(filepath, truncate)?;
        Ok(LogWriter { fw: file_writer, block_pos: 0 })
    }

    fn remaining_block_capacity(&self) -> usize {
        DEFAULT_BLOCK_SIZE - self.block_pos
    }

    fn add_block_padding(&mut self) -> Result<()> {
        let remaining_block_size = DEFAULT_BLOCK_SIZE-self.block_pos;
        if remaining_block_size < MIN_RECORD_SIZE {
            self.fw.append(&BLOCK_PADDING[0..remaining_block_size])?;
        }
        self.block_pos = 0;
        return Ok(());
    }

    fn append_record(&mut self, record: &LogRecord) -> Result<()> {
        self.fw.append(&record.crc.to_le_bytes())?;
        self.fw.append(&record.size.to_le_bytes())?;
        self.fw.append(&record.rtype.value().to_le_bytes())?;
        self.fw.append(record.payload)
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
    pub fn append(&mut self, payload: &[u8]) -> Result<()>{
        let mut record_count = 0;
        let pconsumer = BufferConsumer::new(payload);
        while !pconsumer.done() {
            self.add_block_padding()?;

            let consume_count = min(pconsumer.remaining(), self.remaining_block_capacity()-LOG_RECORD_HEADER_SIZE); 
            let payload = pconsumer.consume(consume_count);
            let rtype = {
                if pconsumer.done() {
                    if record_count == 0 {
                        RecordType::Full
                    } else {
                        RecordType::Last
                    } 
                } else {
                    if record_count == 0 {
                        RecordType::First
                    } else {
                        RecordType::Middle
                    }
                }
            };

            let record = LogRecord::new(rtype, payload);
            record_count+=1;
            self.append_record(&record)?;
            self.block_pos += record.len();
        }
        self.fw.flush()
    }
}


#[cfg(test)]
mod tests {
    use rand::{Rng, RngCore};

    use super::LogWriter;

    #[test]
    fn test_write_small_payload() {
        let log_filepath = "/tmp/test.txt";
        let mut payload: Vec<u8> = vec![0; 256];
        rand::thread_rng().fill_bytes(&mut payload);
        let mut writer = LogWriter::new(log_filepath, true).expect("Failed creating a log writer");
        writer.append(&payload).expect("Failed writing the payload");
    }

    #[test]
    fn test_write_large_payload() {
        let log_filepath = "/tmp/test.txt";
        let mut payload: Vec<u8> = vec![0; 4 * 1024 * 1024];
        rand::thread_rng().fill_bytes(&mut payload);
        let mut writer = LogWriter::new(log_filepath, true).expect("Failed to create a log writer");
        writer.append(&payload).expect("Failed writing the payload");
    }
}