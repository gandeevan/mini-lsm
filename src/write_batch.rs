const HEADER_SIZE: usize = 16;
const COUNT_OFFSET: usize = 0;
const DEFAULT_BUFFER_CAPACITY: usize = 256;

pub struct WriteBatch {
    entries: Vec<u8>,
}

pub struct WriteBatchIterator<'a> {
    wb: &'a WriteBatch,
    pos: usize,
}

impl<'a> Iterator for WriteBatchIterator<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos == self.wb.len() {
            return None;
        }

        let key_len =
            u32::from_le_bytes(self.wb.entries[self.pos..self.pos + 4].try_into().unwrap())
                as usize;
        self.pos += 4;

        let key = &self.wb.entries[self.pos..self.pos + key_len];
        self.pos += key_len;

        let value_len =
            u32::from_le_bytes(self.wb.entries[self.pos..self.pos + 4].try_into().unwrap())
                as usize;
        self.pos += 4;

        let value = &self.wb.entries[self.pos..self.pos + value_len];
        self.pos += value_len;

        return Some((key, value));
    }
}

impl WriteBatch {
    pub fn new() -> WriteBatch {
        WriteBatch {
            entries: vec![0; HEADER_SIZE],
        }
    }

    pub fn count(&self) -> u32 {
        u32::from_le_bytes(
            self.entries[COUNT_OFFSET..COUNT_OFFSET + 4]
                .try_into()
                .unwrap(),
        )
    }

    fn increment_count(&mut self) {
        let count = self.count() + 1;
        self.entries[COUNT_OFFSET..COUNT_OFFSET + 4].copy_from_slice(&count.to_le_bytes());
    }

    pub fn append(&mut self, key: &[u8], value: &[u8]) {
        self.entries
            .extend_from_slice(&u32::try_from(key.len()).unwrap().to_le_bytes());
        self.entries.extend_from_slice(key);
        self.entries
            .extend_from_slice(&u32::try_from(value.len()).unwrap().to_le_bytes());
        self.entries.extend_from_slice(value);
        self.increment_count();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.entries
    }

    pub fn iter(&self) -> WriteBatchIterator {
        WriteBatchIterator {
            wb: &self,
            pos: HEADER_SIZE,
        }
    }
}

mod tests {
    use super::WriteBatch;

    #[test]
    fn write_and_read() {
        let mut wb = WriteBatch::new();
        let batch_size = 10;
        for i in 0..batch_size {
            wb.append(&(i as i32).to_le_bytes(), &(i as i32).to_le_bytes());
        }
        assert_eq!(wb.count(), batch_size);

        let mut items_read = 0;
        for (i, (key, value)) in wb.iter().enumerate() {
            assert_eq!(i as i32, i32::from_le_bytes(key.try_into().unwrap()));
            assert_eq!(i as i32, i32::from_le_bytes(value.try_into().unwrap()));
            items_read += 1;
        }
        assert_eq!(items_read, batch_size);
    }
}
