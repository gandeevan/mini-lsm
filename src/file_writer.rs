use crate::error::{Error, Result};
use crate::log_record::DEFAULT_BUFFER_CAPACITY;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};

/// A struct representing a file writer.
pub struct FileWriter {
    writer: BufWriter<File>,
}

impl FileWriter {
    /// Creates a new `FileWriter` instance.
    ///
    /// # Arguments
    ///
    /// * `file_path` - The path to the file.
    /// * `truncate` - A flag indicating whether to truncate the file or append to it.
    ///
    /// # Returns
    ///
    /// Returns a `Result` containing the `FileWriter` instance if successful, or an `Error` if an error occurs.
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

    /// Appends data to the file.
    ///
    /// # Arguments
    ///
    /// * `data` - The data to append to the file.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating success or an `Error` if an error occurs.
    pub fn append(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.writer.write_all(data).map_err(Error::Io)
    }

    /// Flushes any buffered data to the file.
    /// This only flushes the write to the page cache and does not guarantee
    /// that the data is written to disk.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating success or an `Error` if an error occurs.
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush().map_err(Error::Io)
    }

    /// Flushes any buffered data to the OS and fsyncs the file to disk.
    ///
    /// # Returns
    ///
    /// Returns a `Result` indicating success or an `Error` if an error occurs.
    #[allow(dead_code)]
    pub fn sync(&mut self) -> Result<()> {
        self.flush()
            .and_then(|_| self.writer.get_mut().sync_all().map_err(Error::Io))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, RngCore};
    use std::fs;
    use tempfile::NamedTempFile;

    #[test]
    fn append() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();

        let mut options = OpenOptions::new();
        options.create(true).write(true).truncate(true);
        let mut fw = FileWriter::new(file_path, true).expect("failed opening a file handle");

        let mut random_bytes: Vec<u8> = vec![0; DEFAULT_BUFFER_CAPACITY];
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
    #[test]
    fn append_empty_data() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();

        let mut fw = FileWriter::new(file_path, true).expect("failed opening a file handle");

        // Append empty data
        fw.append(&[]).unwrap();
        fw.flush().unwrap();
        fw.sync().unwrap();

        let actual = fs::read(file_path).unwrap();
        assert_eq!(actual, []);
    }

    #[test]
    fn append_large_data() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();

        let mut fw = FileWriter::new(file_path, true).expect("failed opening a file handle");

        let mut random_bytes: Vec<u8> = vec![0; 100 * DEFAULT_BUFFER_CAPACITY];
        rand::thread_rng().fill_bytes(&mut random_bytes);

        // Append large data
        fw.append(&random_bytes).unwrap();
        fw.flush().unwrap();
        fw.sync().unwrap();

        // Read file and validate the contents
        let actual = fs::read(file_path).unwrap();
        assert_eq!(actual, random_bytes);
    }

    #[test]
    fn append_multiple_times() {
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();

        let mut fw = FileWriter::new(file_path, true).expect("failed opening a file handle");

        let mut random_bytes: Vec<u8> = vec![0; 10 * DEFAULT_BUFFER_CAPACITY];
        rand::thread_rng().fill_bytes(&mut random_bytes);

        // Append data multiple times
        for _ in 0..5 {
            fw.append(&random_bytes).unwrap();
        }
        fw.flush().unwrap();
        fw.sync().unwrap();

        // Read file and validate the contents
        let actual = fs::read(file_path).unwrap();
        let expected = random_bytes.repeat(5);
        assert_eq!(actual, expected);
    }
}
