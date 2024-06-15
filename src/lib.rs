mod buffer_consumer;
mod error;
mod file_writer;
mod lending_iterator;
mod log_reader;
mod log_record;
mod log_writer;
mod memtable;
mod wal_recovery;
pub mod write_batch;
use std::{fs, os::unix::fs::MetadataExt, path::Path};

use log_writer::LogWriter;
use memtable::Memtable;

pub struct DB {
    memtable: Memtable,
    log_writer: LogWriter,
}

pub struct Iter<'a> {
    it: memtable::Iter<'a>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        self.it.next()
    }
}

impl DB {
    pub fn new(log_file: &str) -> error::Result<DB> {
        let mut memtable = Memtable::new();

        if Path::new(log_file).exists() {
            let metadata = fs::metadata(log_file)?;
            if metadata.size() > 0 {
                wal_recovery::load(log_file, &mut memtable)?;
            }
        }

        let log_writer = LogWriter::new(log_file, false)?;
        Ok(DB {
            memtable,
            log_writer,
        })
    }

    pub fn insert_or_update(&mut self, key: &[u8], value: &[u8]) -> error::Result<()> {
        let mut wb = write_batch::WriteBatch::new();
        wb.insert_or_update(key, value);
        self.write(&wb)
    }

    pub fn write(&mut self, wb: &write_batch::WriteBatch) -> error::Result<()> {
        self.log_writer.append(wb.as_bytes())?;
        wal_recovery::consume_write_batch(&mut self.memtable, wb);
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> error::Result<Option<&[u8]>> {
        Ok(self.memtable.get(key))
    }

    /// Deletes a key from the KVStore.
    /// Performs a logical delete by inserting an empty value for the key.
    pub fn delete(&mut self, key: &[u8]) -> error::Result<()> {
        let mut wb = write_batch::WriteBatch::new();
        wb.delete(key);
        self.write(&wb)
    }

    pub fn scan(&self, start: &[u8], end: &[u8]) -> error::Result<Iter> {
        let iter = self.memtable.scan(start, end);
        Ok(Iter { it: iter })
    }
}

#[cfg(test)]
mod test_utils {
    use std::collections::HashSet;

    use num_traits::ToBytes;

    use super::*;

    pub fn populate(count: i32, kvstore: &mut DB) -> Vec<(i32, i32)> {
        let mut data: Vec<(i32, i32)> = vec![];
        for i in 0..count {
            data.push((i, i));
        }

        for (key, value) in data.iter() {
            kvstore
                .insert_or_update(&key.to_be_bytes(), &value.to_be_bytes())
                .expect("Insert failed");
        }

        data
    }

    pub fn update(data: &mut Vec<(i32, i32)>, kvstore: &mut DB) {
        for (_, value) in data.iter_mut() {
            *value = 2 * (*value);
        }

        for (key, value) in data.iter() {
            kvstore
                .insert_or_update(&key.to_be_bytes(), &value.to_be_bytes())
                .expect("Update failed");
        }
    }

    pub fn validate_key_values(
        data: &Vec<(i32, i32)>,
        deleted_keys: Option<&HashSet<i32>>,
        kvstore: &DB,
    ) {
        for (key, value) in data {
            if deleted_keys.is_some() && deleted_keys.unwrap().get(key).is_some() {
                assert!(
                    kvstore
                        .get(key.to_be_bytes().as_ref())
                        .expect("Get failed")
                        .is_none(),
                    "Expected key {} to be deleted",
                    key
                );
            } else {
                assert_eq!(
                    kvstore
                        .get(key.to_be_bytes().as_ref())
                        .expect("Get failed")
                        .expect(&format!("Expected a non-empty value for key {}", key)),
                    value.to_be_bytes()
                );
            }
        }
    }

    pub fn delete_keys(keys: &HashSet<i32>, kvstore: &mut DB) {
        for key in keys {
            kvstore
                .delete(key.to_be_bytes().as_ref())
                .expect("Delete failed");
        }
    }
}

#[cfg(test)]
mod test_basic_operations {
    use tempfile::NamedTempFile;

    use self::test_utils::{delete_keys, validate_key_values};

    use super::*;

    #[test]
    fn insert_or_update() {
        let temp_file: NamedTempFile = NamedTempFile::new().unwrap();
        let log_file_path = temp_file.path().to_str().unwrap();

        let mut kvstore = DB::new(log_file_path).expect("Failed to create a new DB");
        let count = 1000;

        // Test inserts
        let mut data: Vec<(i32, i32)> = test_utils::populate(count, &mut kvstore);

        // Test updates
        test_utils::update(&mut data, &mut kvstore)
    }

    #[test]
    fn get() {
        let temp_file: NamedTempFile = NamedTempFile::new().unwrap();
        let log_file_path = temp_file.path().to_str().unwrap();

        let mut kvstore = DB::new(&log_file_path).expect("Failed to create a new DB");
        let count = 1000;

        // Check that a non-exisitent key returns an empty value
        assert!(kvstore
            .get((1 as i32).to_be_bytes().as_ref())
            .expect("Get failed")
            .is_none());

        // Populate the KVStore and validate the data
        let mut data = test_utils::populate(count, &mut kvstore);
        test_utils::validate_key_values(&data, None, &mut kvstore);

        // Update all the values and validate the data
        test_utils::update(&mut data, &mut kvstore);
        test_utils::validate_key_values(&data, None, &mut kvstore);
    }

    #[test]
    fn delete() {
        let temp_file: NamedTempFile = NamedTempFile::new().unwrap();
        let log_file_path = temp_file.path().to_str().unwrap();

        let mut kvstore = DB::new(&log_file_path).expect("Failed to create a new DB");
        let count = 1000;

        // Populate the KVStore and validate the data
        let data = test_utils::populate(count, &mut kvstore);
        test_utils::validate_key_values(&data, None, &mut kvstore);

        // Delete half the keys and validate that they are deleted
        let mut keys_to_delete = std::collections::HashSet::new();
        for (idx, (key, _)) in data.iter().enumerate() {
            if idx % 2 == 0 {
                keys_to_delete.insert(*key);
            }
        }
        delete_keys(&keys_to_delete, &mut kvstore);
        validate_key_values(&data, Some(&keys_to_delete), &kvstore);
    }

    #[test]
    fn scan() {
        let temp_file: NamedTempFile = NamedTempFile::new().unwrap();
        let log_file_path = temp_file.path().to_str().unwrap();

        let mut kvstore = DB::new(&log_file_path).expect("Failed to create a new DB");
        let count = 1000;

        let mut data = test_utils::populate(count, &mut kvstore);
        data.sort();

        let start_idx = data.len() / 2;
        let end_idx = data.len() - 1;

        let mut result = Vec::new();
        for (key, value) in kvstore
            .scan(
                data[start_idx].0.to_be_bytes().as_ref(),
                data[end_idx].0.to_be_bytes().as_ref(),
            )
            .expect("range query returned an error")
        {
            let key = i32::from_be_bytes(key.try_into().unwrap());
            let value = i32::from_be_bytes(value.try_into().unwrap());
            result.push((key, value));
        }
        assert_eq!(result, &data[start_idx..end_idx]);
    }
}

#[cfg(test)]
/// Module for testing recovery functionality.
mod test_recovery {
    use tempfile::NamedTempFile;

    use super::*;
    use crate::test_utils::{delete_keys, validate_key_values};

    #[test]
    fn recovery() {
        let temp_file: NamedTempFile = NamedTempFile::new().unwrap();
        let log_file_path = temp_file.path().to_str().unwrap();

        let mut kvstore = DB::new(&log_file_path).expect("Failed to create a new DB");
        let count = 1000;

        /**********************************/
        /*           STAGE 1              */
        /* Basic CRUD Operations Testing  */
        /**********************************/

        // Insert initial set of values into the database
        let mut data = test_utils::populate(count, &mut kvstore);
        validate_key_values(&data, None, &kvstore);

        // Update certain values in the database
        test_utils::update(&mut data, &mut kvstore);
        validate_key_values(&data, None, &kvstore);

        // Delete every second key from the database
        let mut keys_to_delete = std::collections::HashSet::new();
        for (idx, (key, _)) in data.iter().enumerate() {
            if idx % 2 == 0 {
                keys_to_delete.insert(*key);
            }
        }
        delete_keys(&keys_to_delete, &mut kvstore);
        validate_key_values(&data, Some(&keys_to_delete), &kvstore);

        /**********************************/
        /*           STAGE 2              */
        /*       Recovery Testing         */
        /**********************************/

        // Re-instantiate the database to simulate recovery and validate the integrity of data post-recovery
        let kvstore = DB::new(log_file_path).expect("Failed to create a new DB");
        validate_key_values(&data, Some(&keys_to_delete), &kvstore);
    }
}
