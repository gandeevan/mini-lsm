use crate::buffer_consumer::BufferConsumer;
use crate::error::{Error, Result};
use crate::file_writer::FileWriter;
use crate::log_record::{
    LogRecord, RecordType, BLOCK_PADDING, DEFAULT_BLOCK_SIZE, LOG_RECORD_HEADER_SIZE,
    MIN_RECORD_SIZE,
};
use std::cmp::min;

pub struct LogWriter {
    fw: FileWriter,
    block_pos: usize,
}

impl LogWriter {
    pub fn new(file_path: &str, truncate: bool) -> Result<LogWriter> {
        let file_writer = FileWriter::new(file_path, truncate)?;
        Ok(LogWriter {
            fw: file_writer,
            block_pos: 0,
        })
    }

    fn remaining_block_capacity(&self) -> usize {
        crate::log_record::DEFAULT_BLOCK_SIZE - self.block_pos
    }

    fn add_block_padding(&mut self) -> Result<()> {
        let remaining_block_size = DEFAULT_BLOCK_SIZE - self.block_pos;
        if remaining_block_size < MIN_RECORD_SIZE {
            self.fw.append(&BLOCK_PADDING[0..remaining_block_size])?;
        }
        self.block_pos = 0;
        Ok(())
    }

    fn append_record(&mut self, record: &LogRecord) -> Result<()> {
        self.fw.append(&record.crc.to_be_bytes())?;
        self.fw.append(&record.size.to_be_bytes())?;
        self.fw.append(&record.rtype.value().to_be_bytes())?;
        self.fw.append(record.payload)
    }

    pub fn append(&mut self, payload: &[u8]) -> Result<()> {
        if payload.is_empty() {
            return Err(Error::ValueError("Payload is empty".to_string()));
        }

        let mut record_count = 0;
        let pconsumer = BufferConsumer::new(payload);
        while !pconsumer.done() {
            self.add_block_padding()?;

            let consume_count = min(
                pconsumer.remaining(),
                self.remaining_block_capacity() - LOG_RECORD_HEADER_SIZE,
            );
            let payload = pconsumer.consume(consume_count);
            let rtype = {
                if pconsumer.done() {
                    if record_count == 0 {
                        RecordType::Full
                    } else {
                        RecordType::Last
                    }
                } else if record_count == 0 {
                    RecordType::First
                } else {
                    RecordType::Middle
                }
            };

            let record = LogRecord::new(rtype, payload);
            record_count += 1;
            self.append_record(&record)?;
            self.block_pos += record.len();
        }
        self.fw.flush()
    }
}

#[cfg(test)]
mod tests {
    use rand::RngCore;

    use crate::log_record::{DEFAULT_BLOCK_SIZE, LOG_RECORD_HEADER_SIZE};

    use super::LogWriter;

    #[test]
    fn test_append_small_payload() {
        let log_filepath = "/tmp/test.txt";
        let mut payload: Vec<u8> = vec![0; 256];
        rand::thread_rng().fill_bytes(&mut payload);
        let mut writer = LogWriter::new(log_filepath, true).expect("Failed creating a log writer");
        writer.append(&payload).expect("Failed writing the payload");
    }

    #[test]
    fn test_append_empty_payload() {
        let log_filepath = "/tmp/test.txt";
        let payload: Vec<u8> = vec![];
        let mut writer = LogWriter::new(log_filepath, true).expect("Failed creating a log writer");
        writer
            .append(&payload)
            .expect_err("Expected an error when appending an empty payload");
    }

    #[test]
    fn test_append_multiple_payloads() {
        let log_filepath = "/tmp/test.txt";
        let mut payload1: Vec<u8> = vec![1, 2, 3];
        let mut payload2: Vec<u8> = vec![4, 5, 6];
        let mut writer = LogWriter::new(log_filepath, true).expect("Failed creating a log writer");
        writer
            .append(&payload1)
            .expect("Failed writing the first payload");
        writer
            .append(&payload2)
            .expect("Failed writing the second payload");
    }

    #[test]
    fn test_append_payload_exceeding_block_capacity() {
        let log_filepath = "/tmp/test.txt";
        let mut payload: Vec<u8> = vec![0; 2 * DEFAULT_BLOCK_SIZE];
        rand::thread_rng().fill_bytes(&mut payload);
        let mut writer = LogWriter::new(log_filepath, true).expect("Failed to create a log writer");
        writer.append(&payload).expect("Failed writing the payload");
        // The payload should spill over to the third block
        // The first block should contain DEFAULT_BLOCK_SIZE - LOG_RECORD_HEADER_SIZE bytes of the payload
        // The second block should contain DEFAULT_BLOCK_SIZE - LOG_RECORD_HEADER_SIZE bytes of the payload
        // The third block should contain 2 * LOG_RECORD_HEADER_SIZE bytes of the payload + LOG_RECORD_HEADER_SIZE bytes of the header
        assert_eq!(writer.block_pos, 3 * LOG_RECORD_HEADER_SIZE);
    }

    #[test]
    fn test_append_large_payloads_with_padding() {
        let log_filepath = "/tmp/test.txt";

        let payload_size = DEFAULT_BLOCK_SIZE - LOG_RECORD_HEADER_SIZE - 1;
        let mut payload: Vec<u8> = vec![0; payload_size];
        rand::thread_rng().fill_bytes(&mut payload);
        let mut writer = LogWriter::new(log_filepath, true).expect("Failed creating a log writer");
        writer.append(&payload).expect("Failed writing the payload");
        assert_eq!(writer.block_pos, payload_size + LOG_RECORD_HEADER_SIZE);

        let payload_size = 1;
        let mut payload: Vec<u8> = vec![0; payload_size];
        rand::thread_rng().fill_bytes(&mut payload);
        writer.append(&payload).expect("Failed writing the payload");
        // This payload should be written to the next block
        assert_eq!(writer.block_pos, payload_size + LOG_RECORD_HEADER_SIZE);
    }
}
