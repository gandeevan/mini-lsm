use std::io::{BufRead, BufReader};

use crate::{
    error::{self},
    lending_iterator::LendingIterator,
    log_record::{LogRecord, DEFAULT_BLOCK_SIZE, MIN_RECORD_SIZE},
};

pub struct LogReader {
    file_path: String,
}

/// LogReader struct represents a reader for a log file.
/// It provides methods for creating a new LogReader instance and iterating over the log entries.
impl LogReader {
    /// Creates a new LogReader instance with the given file path.
    ///
    /// # Arguments
    ///
    /// * `file_path` - A string slice that holds the path to the log file.
    ///
    /// # Returns
    ///
    /// Returns a Result containing the LogReader instance if successful, or an error if the file cannot be opened.
    pub fn new(file_path: &str) -> error::Result<LogReader> {
        // TODO: check if the file exists and if it is a valid WAL file
        Ok(LogReader {
            file_path: file_path.to_string(),
        })
    }

    /// Returns an iterator over the log entries in the log file.
    ///
    /// # Returns
    ///
    /// Returns a Result containing the Iter instance if successful, or an error if the file cannot be opened or the buffer capacity is invalid.
    pub fn to_iter(&self) -> error::Result<Iter> {
        // TODO: store and read the block size from the header of the WAL file
        let buffer_capacity = DEFAULT_BLOCK_SIZE * 4;
        let f = std::fs::File::open(&self.file_path)?;
        Ok(Iter {
            reader: BufReader::with_capacity(buffer_capacity, f),
            curr_idx: 0,
            bytes_remaining: 0,
            bytes_read: 0,
        })
    }
}

pub struct Iter {
    reader: std::io::BufReader<std::fs::File>,
    bytes_remaining: usize,
    bytes_read: usize,
    curr_idx: usize,
}

impl Iter {
    fn min_record_size_bytes_remaining(&self) -> bool {
        self.bytes_remaining >= MIN_RECORD_SIZE
    }

    fn consume_remaining_bytes(&mut self) {
        assert_eq!(self.bytes_read, self.bytes_remaining + self.curr_idx);
        self.reader.consume(self.bytes_remaining + self.curr_idx);
        self.curr_idx += self.bytes_remaining;
        self.bytes_remaining = 0;
    }

    fn fill_buffer(&mut self) -> error::Result<()> {
        assert_eq!(self.bytes_read, self.bytes_remaining + self.curr_idx);
        self.curr_idx = 0;
        match self.reader.fill_buf() {
            Result::Err(err) => return Err(error::Error::Io(err)),
            Result::Ok(bytes_read) => {
                self.bytes_read = bytes_read.len();
                self.bytes_remaining = bytes_read.len();
            }
        }
        Ok(())
    }

    fn read_record(&mut self) -> error::Result<LogRecord> {
        assert_eq!(self.bytes_read, self.bytes_remaining + self.curr_idx);
        let buffer = &self.reader.buffer()[self.curr_idx..];
        let record = LogRecord::from_serialized_bytes(buffer)?;
        self.curr_idx += record.len();
        self.bytes_remaining -= record.len();
        Ok(record)
    }
}

/// Implementation of the `LendingIterator` trait for the `Iter` struct.
impl LendingIterator for Iter {
    type Item<'b> = error::Result<LogRecord<'b>>;

    /// Advances the iterator and returns the next item.
    ///
    /// # Returns
    ///
    /// - `Some(result)`: If there is a next item, returns `Some` with the result.
    /// - `None`: If there are no more items, returns `None`.
    fn next<'b>(&'b mut self) -> Option<Self::Item<'b>> {
        // Check if the remaining bytes in the buffer are less than the minimum record size
        // If so, read more data from the file.
        if !self.min_record_size_bytes_remaining() {
            self.consume_remaining_bytes();
            match self.fill_buffer() {
                Err(err) => return Some(Err(err)),
                _ => (),
            }
        }

        // If we still don't have enough bytes to read a record,
        // that means we have reached the end of the file.
        if !self.min_record_size_bytes_remaining() {
            return None;
        }
        Some(self.read_record())
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use crate::{
        log_writer::LogWriter,
        write_batch::{WriteBatch, WriteBatchBuilder},
    };

    use super::*;

    #[test]
    fn test_new_log_reader() {
        let file_path = "/tmp/file.log";
        let log_reader = LogReader::new(file_path).unwrap();
        assert_eq!(log_reader.file_path, file_path);
    }

    #[test]
    fn test_log_reader_to_iter() {
        let file_path = "/tmp/file.log";
        File::create(file_path).unwrap();
        let log_reader = LogReader::new(file_path).unwrap();
        let mut iter = log_reader.to_iter().unwrap();
        assert_eq!(iter.curr_idx, 0);
        assert_eq!(iter.bytes_remaining, 0);
        assert_eq!(iter.next().is_none(), true);
    }

    #[test]
    fn test_iter_next() {
        // Append some log records to the log file
        let file_path = "/tmp/file.log";
        let _ = std::fs::remove_file(file_path);

        // Write a bunch of key-value pairs to the log file
        let mut data = Vec::new();
        let mut wb = WriteBatch::new();
        let count: i32 = 100000;
        for i in 1..count {
            wb.insert_or_update(i.to_be_bytes().as_ref(), i.to_be_bytes().as_ref());
            data.push((i.to_be_bytes(), i.to_be_bytes()));
        }
        let mut log_writer = LogWriter::new(file_path, true).unwrap();
        log_writer.append(wb.as_bytes()).unwrap();

        // Read the log file and construct a write batch
        let mut builder = WriteBatchBuilder::new();
        let log_reader = LogReader::new(file_path).unwrap();
        let mut log_iter = log_reader.to_iter().unwrap();
        while let Some(record) = log_iter.next() {
            let record = record.unwrap();
            builder.accumulate_record(&record).unwrap();
        }
        assert!(builder.is_ready());
        let wb = builder.get_write_batch();

        // Verify the data in the WriteBatch read from the log file.
        assert_eq!(wb.count(), data.len().try_into().unwrap());
        for (idx, (key, value)) in wb.iter().enumerate() {
            assert_eq!(data[idx].0, key);
            assert_eq!(data[idx].1, value.unwrap());
        }
        builder.consume();
    }
}
