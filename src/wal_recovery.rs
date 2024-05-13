/// This module provides functionality for WAL (Write-Ahead Log) recovery.
///
/// WAL recovery is responsible for loading the WAL file into the memtable.
///
use crate::{
    error, lending_iterator::LendingIterator, log_reader::LogReader, log_record::RecordType,
    memtable::Memtable, write_batch::WriteBatchIterator,
};

fn handle_payload(memtable: &mut Memtable, payload: &[u8]) {
    let wb_iter = WriteBatchIterator::from_payload(payload);
    for (key, value) in wb_iter {
        match value {
            Some(value) => memtable.insert_or_update(key, value),
            None => {
                memtable.delete(key);
            }
        }
    }
}

/// Load the WAL (Write-Ahead Log) file into the memtable.
///
/// This function reads the WAL file specified by `log_file` and loads its contents
/// into the `memtable`. It iterates over the records in the WAL file, validates them,
/// and handles the payload based on the record type. The payload is processed by the
/// `handle_payload` function, which inserts or updates key-value pairs in the memtable.
/// Partial records are buffered until a complete record is received.
///
/// # Arguments
///
/// * `log_file` - The path to the WAL file.
/// * `memtable` - A mutable reference to the memtable.
///
/// # Errors
///
/// This function returns an error if there is an issue reading the WAL file or if the
/// records in the WAL file are invalid.
///
/// # Example
///
/// ```rust
/// use crate::wal_recovery::load;
/// use crate::memtable::Memtable;
///
/// let mut memtable = Memtable::new();
/// let log_file = "/path/to/wal.log";
///
/// if let Err(err) = load(log_file, &mut memtable) {
///     println!("Failed to load WAL file: {}", err);
/// }
/// ```
///
pub fn load(log_file: &str, memtable: &mut Memtable) -> error::Result<()> {
    let mut buffer: Vec<u8> = Vec::new();
    let log_reader = LogReader::new(log_file)?;
    let mut iter = log_reader.to_iter()?;
    while let Some(record_or_error) = iter.next() {
        let record = record_or_error?;
        record.validate_crc()?;
        match record.rtype {
            RecordType::Full => {
                handle_payload(memtable, record.payload);
            }
            RecordType::First | RecordType::Middle => {
                buffer.extend_from_slice(record.payload);
            }
            RecordType::Last => {
                buffer.extend_from_slice(record.payload);
                handle_payload(memtable, &buffer);
                buffer.clear();
            }
            RecordType::None => {
                assert!(false, "unexpected record type")
            }
        }
    }
    Ok(())
}
