/// This module provides functionality for WAL (Write-Ahead Log) recovery.
///
/// WAL recovery is responsible for loading the WAL file into the memtable.
///
use crate::{
    error,
    lending_iterator::LendingIterator,
    log_reader::LogReader,
    memtable::Memtable,
    write_batch::{WriteBatch, WriteBatchBuilder},
};

pub fn consume_write_batch(memtable: &mut Memtable, wb: &WriteBatch) {
    for (key, value) in wb.iter() {
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
/// ```ignore
/// use mini_lsm::wal_recovery::load;
/// use mini_lsm::memtable::Memtable;
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
    let log_reader = LogReader::new(log_file)?;
    let mut wb_builder = WriteBatchBuilder::new();

    let mut iter = log_reader.to_iter()?;
    while let Some(record_or_error) = iter.next() {
        let record = record_or_error?;
        record.validate_crc()?;
        wb_builder.accumulate_record(&record)?;
        if wb_builder.is_ready() {
            consume_write_batch(memtable, wb_builder.get_write_batch());
            wb_builder.consume();
        }
    }
    Ok(())
}
