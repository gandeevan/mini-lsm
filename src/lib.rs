// TODO: Explore a cleaner way to incorporate all files into the compilation list,
// potentially eliminating the need to explicitly enumerate each module here.
mod buffer_consumer;
mod error;
mod file_writer;
mod log_writer;
mod memtable;
mod write_batch;

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
        let log_writer = LogWriter::new(log_file, false)?;
        Ok(DB {
            memtable: Memtable::new(),
            log_writer,
        })
    }

    pub fn insert_or_update(&mut self, key: &[u8], value: &[u8]) -> Result<(), error::Error> {
        let mut wb = write_batch::WriteBatch::new();
        wb.append(key, value);
        self.insert_or_update_batch(&wb)
    }

    pub fn insert_or_update_batch(
        &mut self,
        batch: &write_batch::WriteBatch,
    ) -> Result<(), error::Error> {
        for (key, value) in batch.iter() {
            self.memtable.insert_or_update(key, value);
        }
        self.log_writer.append(batch.as_bytes())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<&[u8]>, String> {
        Ok(self.memtable.get(key))
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<bool, String> {
        Ok(self.memtable.delete(key))
    }

    pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<Iter, String> {
        let iter = self.memtable.scan(start, end);
        Ok(Iter { it: iter })
    }
}

#[cfg(test)]
mod test_utils {
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

    pub fn validate(expected: &Vec<(i32, i32)>, kvstore: &DB) {
        for (key, value) in expected {
            assert_eq!(
                kvstore
                    .get(key.to_be_bytes().as_ref())
                    .expect("Get failed")
                    .expect("Expected a non-empty value"),
                value.to_be_bytes()
            );
        }
    }
}

#[cfg(test)]
mod test_db {

    use num_traits::ToBytes;

    use super::*;

    #[test]
    fn insert_or_update() {
        let mut kvstore = DB::new("/tmp/log.txt").expect("Failed to create a new DB");
        let count = 1000;

        // Test inserts
        let mut data = test_utils::populate(count, &mut kvstore);

        // Test updates
        test_utils::update(&mut data, &mut kvstore)
    }

    #[test]
    fn get() {
        let mut kvstore = DB::new("/tmp/log.txt").expect("Failed to create a new DB");
        let count = 1000;

        // Check that a non-exisitent key returns an empty value
        assert!(kvstore
            .get((1 as i32).to_be_bytes().as_ref())
            .expect("Get failed")
            .is_none());

        // Populate the KVStore and validate the data
        let mut data = test_utils::populate(count, &mut kvstore);
        test_utils::validate(&data, &mut kvstore);

        // Update all the values and validate the data
        test_utils::update(&mut data, &mut kvstore);
        test_utils::validate(&data, &mut kvstore);
    }

    #[test]
    fn delete() {
        let mut kvstore = DB::new("/tmp/log.txt").expect("Failed to create a new DB");
        let count = 1000;

        // Populate the KVStore and validate the data
        let data = test_utils::populate(count, &mut kvstore);
        test_utils::validate(&data, &mut kvstore);

        // Delete all values and validate that delete returns true
        for (key, _) in data.iter() {
            assert!(kvstore
                .delete(key.to_be_bytes().as_ref())
                .expect("Delete failed"));
        }

        // Try deleting all the keys again and validate that delete returns false
        for (key, _) in data.iter() {
            assert!(!kvstore
                .delete(key.to_be_bytes().as_ref())
                .expect("Delete failed"));
        }
    }

    #[test]
    fn scan() {
        let mut kvstore = DB::new("/tmp/log.txt").expect("Failed to create a new DB");
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
