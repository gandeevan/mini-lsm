
use std::collections::BTreeMap;
use crate::{Key, Value};
pub struct Iter<'a, K: Key, V: Value> {
    it: std::collections::btree_map::Range<'a, K, V>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: Key,
    V: Value,
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        self.it.next()
    }
}


// Single Threaded BTree Memtable
pub struct Memtable<K: Key, V: Value> {
    table: BTreeMap<K, V>,
}

impl<K, V> Memtable<K, V>
where K: Key, V: Value {
    pub fn new() -> Memtable<K, V> {
        Memtable {
            table: BTreeMap::<K, V>::new(),
        }
    }

    pub fn insert_or_update(&mut self, key: K, value: V) -> Result<(), String> {
        self.table.insert(key, value);
        return Ok(());
    }

    pub fn get(&self, key: &K) -> Result<Option<&V>, String> {
        return Ok(self.table.get(key));
    }

    pub fn delete(&mut self, key: &K) -> Result<bool, String> {
        return Ok(self.table.remove(key).is_some());
    }


    pub fn scan(&self, start: &K, end: &K) -> Result<Iter<K, V>, String> {
        return Ok(Iter {
            it: self.table.range(start..end),
        });
    }
}


