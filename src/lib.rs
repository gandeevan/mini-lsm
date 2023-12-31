use serde::{de::DeserializeOwned, Serialize};
use std::marker::PhantomData;

pub trait Key: Ord + Clone + Serialize + DeserializeOwned {}
impl<T> Key for T where T: Ord + Clone + Serialize + DeserializeOwned {}

pub trait Value: Clone + Serialize + DeserializeOwned {}
impl<T> Value for T where T: Ord + Clone + Serialize + DeserializeOwned {}

pub struct DB<K: Key, V: Value> {
    phantom_key: PhantomData<K>,
    phantom_value: PhantomData<V>,
}

pub struct DBIterator<'a, K: Key, V: Value> {
    db: &'a DB<K, V>,
}

impl<'a, K, V> Iterator for DBIterator<'a, K, V>
where
    K: Key,
    V: Value,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        return None;
    }
}

impl<K, V> DB<K, V>
where
    K: Key,
    V: Value,
{
    pub fn new() -> DB<K, V> {
        DB {
            phantom_key: PhantomData::<K> {},
            phantom_value: PhantomData::<V> {},
        }
    }

    pub fn insert_or_update(&mut self, key: K, value: V) -> Result<(), String> {
        return Err(String::from("Not Implemented"));
    }

    pub fn delete(&mut self, key: &K) -> Result<bool, String> {
        return Err(String::from("Not Implemented"));
    }

    pub fn get(&self, key: &K) -> Result<Option<V>, String> {
        return Err(String::from("Not Implemented"));
    }

    pub fn scan(&self, start: &K, end: &K) -> Result<DBIterator<K, V>, String> {
        return Err(String::from("Not Implemented"));
    }
}

#[cfg(test)]
mod test_checkpoint_1 {

    use super::*;

    fn populate(count: i32, kvstore: &mut DB<i32, i32>) -> Vec<(i32, i32)> {
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

    fn update(data: &mut Vec<(i32, i32)>,  kvstore: &mut DB<i32, i32>) {
        for (_, value) in data.iter_mut() {
            *value = 2*(*value);
        }

        for (key, value) in data.iter() {
            kvstore
                .insert_or_update(key.clone(), value.clone())
                .expect("Update failed");
        }
    }

    fn validate(expected: &Vec<(i32, i32)>,  kvstore: &DB<i32, i32>) {
        for (key, value) in expected {
            assert_eq!(kvstore.get(key).expect("Get failed").expect("Expected a non-empty value"), *value);
        }
    }

    #[test]
    fn insert_or_update() {
        let mut kvstore = DB::<i32, i32>::new();
        let count = 1000;
        
        // Test inserts
        let mut data = populate(count, &mut kvstore);
        
        // Test updates
        update(&mut data, &mut kvstore)
    }

    #[test]
    fn get() {
        let mut kvstore = DB::<i32, i32>::new();
        let count = 1000;

        // Check that a non-exisitent key returns an empty value
        assert!(kvstore.get(&1).expect("Get failed").is_none());    

        // Populate the KVStore and validate the data
        let mut data = populate(count, &mut kvstore);
        validate(&data, &mut kvstore);

        // Update all the values and validate the data    
        update(&mut data, &mut kvstore);
        validate(&data, &mut kvstore);        
    }

    #[test]
    fn delete() {
        let mut kvstore = DB::<i32, i32>::new();
        let count = 1000;

        // Populate the KVStore and validate the data
        let mut data = populate(count, &mut kvstore);
        validate(&data, &mut kvstore);

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
    #[test]
    fn scan() {}
}