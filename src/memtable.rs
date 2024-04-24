use std::collections::BTreeMap;

use tinyvec::TinyVec;
pub struct Iter<'a> {
    it: std::collections::btree_map::Range<'a, TinyVec<[u8; 16]>, TinyVec<[u8; 16]>>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        self.it.next().map(|(k, v)| (k.as_slice(), v.as_slice()))
    }
}

// Single Threaded BTree Memtable
pub struct Memtable {
    table: BTreeMap<TinyVec<[u8; 16]>, TinyVec<[u8; 16]>>,
}

impl Memtable {
    pub fn new() -> Memtable {
        Memtable {
            table: BTreeMap::new(),
        }
    }

    pub fn insert_or_update(&mut self, key: &[u8], value: &[u8]) -> () {
        self.table
            .insert(tinyvec::TinyVec::from(key), tinyvec::TinyVec::from(value));
    }

    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        // TODO: avoid copying the key to construct the TinyVec
        return self
            .table
            .get(&tinyvec::TinyVec::from(key))
            .map(|v| v.as_slice());
    }

    pub fn delete(&mut self, key: &[u8]) -> bool {
        // TODO: avoid copying the key to construct the TinyVec
        return self.table.remove(key).is_some();
    }

    pub fn scan(&self, start: &[u8], end: &[u8]) -> Iter {
        // TODO: avoid copying the key to construct the TinyVec
        return Iter {
            it: self
                .table
                .range(tinyvec::TinyVec::from(start)..tinyvec::TinyVec::from(end)),
        };
    }
}
