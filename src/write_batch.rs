const HEADER_SIZE: usize = 16;
const COUNT_OFFSET: usize = 0;

pub struct WriteBatch {
    entries: Vec<u8>,
}

pub struct WriteBatchIterator<'a> {
    payload: &'a [u8],
    pos: usize,
}

impl<'a> WriteBatchIterator<'a> {
    pub fn from_payload(bytes: &'a [u8]) -> WriteBatchIterator<'a> {
        WriteBatchIterator {
            payload: bytes,
            pos: HEADER_SIZE,
        }
    }
}

/// An iterator over the entries in a `WriteBatch`.
///
/// This iterator yields key-value pairs, where the key is a byte slice and the value is an optional byte slice.
/// If the value is `None`, it indicates a deletion entry.
///
/// # Example
///
/// ```
/// use mini_lsm::WriteBatch;
///
/// let mut write_batch = WriteBatch::new();
/// write_batch.insert_or_update(b"key1", Some(b"value1"));
/// write_batch.insert_or_update(b"key2", Some(b"value2"));
/// write_batch.delete(b"key3");
///
/// let mut iter = write_batch.iter();
/// assert_eq!(iter.next(), Some((&b"key1"[..], Some(&b"value1"[..]))));
/// assert_eq!(iter.next(), Some((&b"key2"[..], Some(&b"value2"[..]))));
/// assert_eq!(iter.next(), Some((&b"key3"[..], None)));
/// assert_eq!(iter.next(), None);
/// ```
impl<'a> Iterator for WriteBatchIterator<'a> {
    type Item = (&'a [u8], Option<&'a [u8]>);

    /// Advances the iterator and returns the next key-value pair.
    ///
    /// Returns `Some((key, value))` if there is a next entry, where `key` is a byte slice representing the key
    /// and `value` is an optional byte slice representing the value. If the value is `None`, it indicates a deletion entry.
    ///
    /// Returns `None` if there are no more entries in the `WriteBatch`.
    fn next(&mut self) -> Option<Self::Item> {
        if self.pos == self.payload.len() {
            return None;
        }

        let key_len =
            u32::from_be_bytes(self.payload[self.pos..self.pos + 4].try_into().unwrap()) as usize;
        self.pos += 4;

        let key = &self.payload[self.pos..self.pos + key_len];
        self.pos += key_len;

        let value_len =
            u32::from_be_bytes(self.payload[self.pos..self.pos + 4].try_into().unwrap()) as usize;
        self.pos += 4;

        if value_len == 0 {
            return Some((key, None));
        } else {
            let value = &self.payload[self.pos..self.pos + value_len];
            self.pos += value_len;
            return Some((key, Some(value)));
        }
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// Represents a write batch, which is a collection of write operations to be applied atomically.
/// Represents a batch of write operations.
///
/// A `WriteBatch` is used to group multiple write operations together, such as inserts and deletes,
/// in order to perform them atomically. It provides methods to add, count, and iterate over the
/// write operations in the batch.
impl WriteBatch {
    /// Creates a new empty write batch.
    pub fn new() -> WriteBatch {
        WriteBatch {
            entries: vec![0; HEADER_SIZE],
        }
    }

    /// Returns the number of write operations in the batch.
    pub fn count(&self) -> u32 {
        u32::from_be_bytes(
            self.entries[COUNT_OFFSET..COUNT_OFFSET + 4]
                .try_into()
                .unwrap(),
        )
    }

    /// Increments the count of write operations in the batch.
    fn increment_count(&mut self) {
        let count = self.count() + 1;
        self.entries[COUNT_OFFSET..COUNT_OFFSET + 4].copy_from_slice(&count.to_be_bytes());
    }

    /// Adds a delete operation to the batch for the given key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete.
    pub fn delete(&mut self, key: &[u8]) {
        self.entries
            .extend_from_slice(&u32::try_from(key.len()).unwrap().to_be_bytes());
        self.entries.extend_from_slice(key);
        self.entries
            .extend_from_slice(&u32::try_from(0).unwrap().to_be_bytes());
    }

    /// Adds an insert or update operation to the batch for the given key-value pair.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert or update.
    /// * `value` - The value to associate with the key.
    pub fn insert_or_update(&mut self, key: &[u8], value: &[u8]) {
        self.entries
            .extend_from_slice(&u32::try_from(key.len()).unwrap().to_be_bytes());
        self.entries.extend_from_slice(key);
        self.entries
            .extend_from_slice(&u32::try_from(value.len()).unwrap().to_be_bytes());
        self.entries.extend_from_slice(value);
        self.increment_count();
    }

    /// Returns the total length of the write batch in bytes.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the write batch is empty, false otherwise.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Clears all write operations from the batch.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// Returns the write batch as a byte slice.
    pub fn as_bytes(&self) -> &[u8] {
        &self.entries
    }

    /// Returns an iterator over the write operations in the batch.
    pub fn iter(&self) -> WriteBatchIterator {
        WriteBatchIterator {
            payload: &self.entries,
            pos: HEADER_SIZE,
        }
    }
}

mod tests {

    #[test]
    fn write_and_read() {
        let mut wb = super::WriteBatch::new();
        let batch_size = 10;
        for i in 0..batch_size {
            wb.insert_or_update(&(i as i32).to_be_bytes(), &(i as i32).to_be_bytes());
        }
        assert_eq!(wb.count(), batch_size);

        let mut items_read = 0;
        for (i, (key, value)) in wb.iter().enumerate() {
            assert_eq!(i as i32, i32::from_be_bytes(key.try_into().unwrap()));
            assert_eq!(
                i as i32,
                i32::from_be_bytes(value.unwrap().try_into().unwrap())
            );
            items_read += 1;
        }
        assert_eq!(items_read, batch_size);
    }
}
