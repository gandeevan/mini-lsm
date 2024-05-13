use std::{fs::File, io::BufReader};

use crate::{error::Result, log_record::DEFAULT_BUFFER_CAPACITY};

pub struct FileReader {
    buf_reader: std::io::BufReader<std::fs::File>,
}

impl FileReader {
    pub fn new(file_path: &str) -> Result<FileReader> {
        let f = File::open(file_path)?;
        Ok(FileReader {
            buf_reader: BufReader::with_capacity(DEFAULT_BUFFER_CAPACITY, f),
        })
    }

    pub fn read(&mut self, _bytes: usize, _offset: usize) -> Result<&[u8]> {
        unimplemented!();
    }
}
