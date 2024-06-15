use crate::buffer_consumer::BufferConsumer;
use crate::error::{Error, Result};
use crate::file_writer::FileWriter;
use crate::log_record::{
    LogRecord, RecordType, BLOCK_PADDING, DEFAULT_BLOCK_SIZE, LOG_RECORD_HEADER_SIZE,
    MIN_RECORD_SIZE,
};
use std::cmp::min;

pub struct Stats {
    record_count: usize,
}

impl Stats {
    fn new() -> Stats {
        Stats { record_count: 0 }
    }

    fn consume_record(&mut self, _record: &LogRecord) {
        self.record_count += 1;
    }
}

/// The `LogWriter` struct represents a log writer that appends log records to a file.
pub struct LogWriter {
    fw: FileWriter,
    block_pos: usize,
    stats: Stats,
}

impl LogWriter {
    /// Creates a new `LogWriter` instance.
    ///
    /// # Arguments
    ///
    /// * `file_path` - The path to the file where the log records will be written.
    /// * `truncate` - A flag indicating whether to truncate the file if it already exists.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the `LogWriter` instance if successful, or an error if the file cannot be opened.
    pub fn new(file_path: &str, truncate: bool) -> Result<LogWriter> {
        let file_writer = FileWriter::new(file_path, truncate)?;
        Ok(LogWriter {
            fw: file_writer,
            block_pos: 0,
            stats: Stats::new(),
        })
    }

    /// Returns the remaining capacity in the current log block.
    fn remaining_block_capacity(&self) -> usize {
        crate::log_record::DEFAULT_BLOCK_SIZE - self.block_pos
    }

    /// Adds padding to the current log block if necessary.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if successful, or an error if the padding cannot be added.
    fn add_block_padding(&mut self) -> Result<()> {
        let remaining_block_size = DEFAULT_BLOCK_SIZE - self.block_pos;
        if remaining_block_size < MIN_RECORD_SIZE {
            self.fw.append(&BLOCK_PADDING[0..remaining_block_size])?;
        }
        self.block_pos = 0;
        Ok(())
    }

    /// Appends a log record to the log file.
    ///
    /// # Arguments
    ///
    /// * `record` - The log record to append.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if successful, or an error if the record cannot be appended.
    fn append_record(&mut self, record: &LogRecord) -> Result<()> {
        self.fw.append(&record.crc.to_be_bytes())?;
        self.fw.append(&record.size.to_be_bytes())?;
        self.fw.append(&record.rtype.value().to_be_bytes())?;
        self.fw.append(record.payload)
    }

    /// Appends a payload to the log file.
    ///
    /// # Arguments
    ///
    /// * `payload` - The payload to append.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if successful, or an error if the payload is empty or cannot be appended.
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
            self.stats.consume_record(&record);
            self.block_pos += record.len();
        }
        self.fw.flush()
    }
}

#[cfg(test)]
mod tests {
    use rand::RngCore;
    use tempfile::NamedTempFile;

    use crate::log_record::{LogRecord, DEFAULT_BLOCK_SIZE, LOG_RECORD_HEADER_SIZE};

    use super::LogWriter;

    #[test]
    fn test_append_small_payload() {
        let temp_file = NamedTempFile::new().unwrap();
        let log_file_path = temp_file.path().to_str().unwrap();

        let mut payload: Vec<u8> = vec![0; 256];
        rand::thread_rng().fill_bytes(&mut payload);
        let mut writer = LogWriter::new(log_file_path, true).expect("Failed creating a log writer");
        writer.append(&payload).expect("Failed writing the payload");

        // validate the contents of the file
        let reader = std::fs::read(log_file_path).unwrap();
        let record = LogRecord::from_serialized_bytes(&reader).unwrap();
        assert_eq!(record.payload, payload);
    }

    #[test]
    fn test_append_empty_payload() {
        let temp_file = NamedTempFile::new().unwrap();
        let log_file_path = temp_file.path().to_str().unwrap();

        let payload: Vec<u8> = vec![];
        let mut writer = LogWriter::new(log_file_path, true).expect("Failed creating a log writer");
        writer
            .append(&payload)
            .expect_err("Expected an error when appending an empty payload");
    }

    #[test]
    fn test_append_multiple_payloads() {
        let temp_file = NamedTempFile::new().unwrap();
        let log_file_path = temp_file.path().to_str().unwrap();

        let payload1: Vec<u8> = vec![1, 2, 3];
        let payload2: Vec<u8> = vec![4, 5, 6];
        let mut writer = LogWriter::new(log_file_path, true).expect("Failed creating a log writer");
        writer
            .append(&payload1)
            .expect("Failed writing the first payload");
        writer
            .append(&payload2)
            .expect("Failed writing the second payload");
    }

    #[test]
    fn test_append_payload_exceeding_block_capacity() {
        let temp_file = NamedTempFile::new().unwrap();
        let log_file_path = temp_file.path().to_str().unwrap();

        let mut payload: Vec<u8> = vec![0; 2 * DEFAULT_BLOCK_SIZE];
        rand::thread_rng().fill_bytes(&mut payload);
        let mut writer =
            LogWriter::new(log_file_path, true).expect("Failed to create a log writer");
        writer.append(&payload).expect("Failed writing the payload");
        // The payload should spill over to the third block
        // The first block should contain DEFAULT_BLOCK_SIZE - LOG_RECORD_HEADER_SIZE bytes of the payload
        // The second block should contain DEFAULT_BLOCK_SIZE - LOG_RECORD_HEADER_SIZE bytes of the payload
        // The third block should contain 2 * LOG_RECORD_HEADER_SIZE bytes of the payload + LOG_RECORD_HEADER_SIZE bytes of the header
        assert_eq!(writer.block_pos, 3 * LOG_RECORD_HEADER_SIZE);
    }

    #[test]
    fn test_append_large_payloads_with_padding() {
        let temp_file = NamedTempFile::new().unwrap();
        let log_file_path = temp_file.path().to_str().unwrap();

        let payload_size = DEFAULT_BLOCK_SIZE - LOG_RECORD_HEADER_SIZE - 1;
        let mut payload: Vec<u8> = vec![0; payload_size];
        rand::thread_rng().fill_bytes(&mut payload);
        let mut writer = LogWriter::new(log_file_path, true).expect("Failed creating a log writer");
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
