use std::io::{BufRead, BufReader};

use crate::{
    error,
    lending_iterator::LendingIterator,
    log_record::{LogRecord, DEFAULT_BLOCK_SIZE, MIN_RECORD_SIZE},
};

pub struct LogReader {
    file_path: String,
}

impl LogReader {
    pub fn new(file_path: &str) -> error::Result<LogReader> {
        Ok(LogReader {
            file_path: file_path.to_string(),
        })
    }

    pub fn to_iter(&self) -> error::Result<Iter> {
        // TODO: store and read the block size from the header of the WAL file
        let buffer_capacity = DEFAULT_BLOCK_SIZE * 4;
        let f = std::fs::File::open(&self.file_path)?;
        Ok(Iter {
            reader: BufReader::with_capacity(buffer_capacity, f),
            curr_idx: 0,
            max_idx: 0,
        })
    }
}

pub struct Iter {
    reader: std::io::BufReader<std::fs::File>,
    max_idx: usize,
    curr_idx: usize,
}

impl LendingIterator for Iter {
    type Item<'b> = error::Result<LogRecord<'b>>;

    fn next<'b>(&'b mut self) -> Option<Self::Item<'b>> {
        if self.max_idx == 0 || self.max_idx - self.curr_idx < MIN_RECORD_SIZE {
            let bytes_remaining = self.max_idx - self.curr_idx + 1;

            if bytes_remaining > 0 {
                self.reader.consume(bytes_remaining);
            }

            match self.reader.fill_buf() {
                Result::Err(err) => {
                    return Some(Err(error::Error::Io(err)));
                }
                Result::Ok(bytes_read) => {
                    self.max_idx = bytes_read.len() - 1;
                }
            }

            self.curr_idx = 0;
        }

        let buffer = &self.reader.buffer()[self.curr_idx..];
        let maybe_record = LogRecord::from_serialized_bytes(buffer);
        if let Ok(ref record) = maybe_record {
            self.curr_idx += record.len();
        }
        Some(maybe_record)
    }
}
