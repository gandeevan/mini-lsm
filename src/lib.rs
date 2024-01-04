use serde::{de::DeserializeOwned, Serialize};
use std::{collections::BTreeMap, marker::PhantomData};
use std::ops::Bound::{Included, Excluded};

pub trait Key: Ord + Clone + Serialize + DeserializeOwned {}
impl<T> Key for T where T: Ord + Clone + Serialize + DeserializeOwned {}

pub trait Value: Clone + Serialize + DeserializeOwned {}
impl<T> Value for T where T: Ord + Clone + Serialize + DeserializeOwned {}

pub struct DB<K: Key, V: Value> {
    memtable: BTreeMap<K, V>,
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
}

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

impl<K, V> DB<K, V>
where
    K: Key,
    V: Value,
{
    pub fn new() -> DB<K, V> {
        DB {
            memtable: BTreeMap::<K, V>::new(),
            phantom_key: PhantomData::<K> {},
            phantom_value: PhantomData::<V> {},
        }
    }

    pub fn insert_or_update(&mut self, key: K, value: V) -> Result<(), String> {
        self.memtable.insert(key, value);
        return Ok(());
    }

    pub fn get(&self, key: &K) -> Result<Option<&V>, String> {
        return Ok(self.memtable.get(key));
    }

    pub fn delete(&mut self, key: &K) -> Result<bool, String> {
        return Ok(self.memtable.remove(key).is_some());
    }

    pub fn scan(&self, start: &K, end: &K) -> Result<Iter<K, V>, String> {
        return Ok(Iter {
            it: self.memtable.range(start..end),
        });
    }
}



#[cfg(test)]
mod test_utils {
    use super::*;

    pub fn populate(count: i32, kvstore: &mut DB<i32, i32>) -> Vec<(i32, i32)> {
        let mut data: Vec<(i32, i32)> = vec![];
        for i in 0..count {
            data.push((i, i));
        }

        for (key, value) in data.iter() {
            kvstore
                .insert_or_update(key.clone(), value.clone())
                .expect("Insert failed");
        }

        data
    }

    pub fn update(data: &mut Vec<(i32, i32)>, kvstore: &mut DB<i32, i32>) {
        for (_, value) in data.iter_mut() {
            *value = 2 * (*value);
        }

        for (key, value) in data.iter() {
            kvstore
                .insert_or_update(key.clone(), value.clone())
                .expect("Update failed");
        }
    }

    pub fn validate(expected: &Vec<(i32, i32)>, kvstore: &DB<i32, i32>) {
        for (key, value) in expected {
            assert_eq!(
                kvstore
                    .get(key)
                    .expect("Get failed")
                    .expect("Expected a non-empty value"),
                value
            );
        }
    }
}

#[cfg(test)]
mod test_checkpoint_1 {

   use super::*;

    #[test]
    fn insert_or_update() {
        let mut kvstore = DB::<i32, i32>::new();
        let count = 1000;

        // Test inserts
        let mut data = test_utils::populate(count, &mut kvstore);

        // Test updates
        test_utils::update(&mut data, &mut kvstore)
    }

    #[test]
    fn get() {
        let mut kvstore = DB::<i32, i32>::new();
        let count = 1000;

        // Check that a non-exisitent key returns an empty value
        assert!(kvstore.get(&1).expect("Get failed").is_none());

        // Populate the KVStore and validate the data
        let mut data = test_utils::populate(count, &mut kvstore);
        test_utils::validate(&data, &mut kvstore);

        // Update all the values and validate the data
        test_utils::update(&mut data, &mut kvstore);
        test_utils::validate(&data, &mut kvstore);
    }

    #[test]
    fn delete() {
        let mut kvstore = DB::<i32, i32>::new();
        let count = 1000;

        // Populate the KVStore and validate the data
        let mut data = test_utils::populate(count, &mut kvstore);
        test_utils::validate(&data, &mut kvstore);

        // Delete all values and validate that delete returns true
        for (key, _) in data.iter() {
            assert!(kvstore.delete(key).expect("Delete failed"));
        }

        // Try deleting all the keys again and validate that delete returns false
        for (key, _) in data.iter() {
            assert!(!kvstore.delete(&key).expect("Delete failed"));
        }
    }
}

#[cfg(test)]
mod test_checkpoint_2 {
    use super::*;

    #[test]
    fn scan() {
        let mut kvstore = DB::<i32, i32>::new();
        let count = 1000;

        let mut data = test_utils::populate(count, &mut kvstore);
        data.sort();

        let start_idx = data.len()/2;
        let end_idx = data.len()-1;

        let mut result = Vec::new();
        for (key, value) in kvstore.scan(&data[start_idx].0, &data[end_idx].0).expect("range query returned an error") {
            result.push((*key, *value));
        }
        assert_eq!(result, &data[start_idx..end_idx]);
    }
}
