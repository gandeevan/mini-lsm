use crate::error::{Error, Result};
use crate::log_record::DEFAULT_BUFFER_CAPACITY;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};

pub struct FileWriter {
    writer: BufWriter<File>,
}

impl FileWriter {
    pub fn new(file_path: &str, truncate: bool) -> Result<FileWriter> {
        let mut options = OpenOptions::new();
        options.create(true);

        if truncate {
            options.write(true).truncate(true);
        } else {
            options.append(true);
        }

        let file = options.open(file_path).map_err(Error::Io)?;
        Ok(FileWriter {
            writer: BufWriter::with_capacity(DEFAULT_BUFFER_CAPACITY, file),
        })
    }

    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.writer.write_all(data).map_err(Error::Io)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().map_err(Error::Io)
    }

    #[allow(dead_code)]
    pub fn sync(&mut self) -> Result<()> {
        self.flush()
            .and_then(|_| self.writer.get_mut().sync_all().map_err(Error::Io))
    }
}

#[cfg(test)]
mod tests {
    use rand::{Rng, RngCore};
    use std::fs;

    use super::*;

    #[test]
    fn append() {
        let file_path = "/tmp/test.txt";
        let mut options = OpenOptions::new();
        options.create(true).write(true).truncate(true);
        let mut fw = FileWriter::new(file_path, true).expect("failed opening a file handle");

        let mut random_bytes: Vec<u8> = vec![0; 10 * DEFAULT_BUFFER_CAPACITY];
        rand::thread_rng().fill_bytes(&mut random_bytes);

        let mut pos = 0;
        let mut remaining = random_bytes.len();
        while remaining > 0 {
            let write_count: usize = rand::thread_rng().gen_range(0..remaining + 1);
            fw.append(&random_bytes[pos..pos + write_count]).unwrap();
            pos += write_count;
            remaining -= write_count;
        }
        fw.flush().unwrap();
        fw.sync().unwrap();

        // read file and validate the contents
        let actual = fs::read(file_path).unwrap();
        assert_eq!(actual, random_bytes);
    }
}
