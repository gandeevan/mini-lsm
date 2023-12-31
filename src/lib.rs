use std::marker::PhantomData;
use serde::{Serialize, de::DeserializeOwned};

pub trait Key: Ord + Clone + Serialize + DeserializeOwned {}

pub trait Value: Clone + Serialize + DeserializeOwned {}

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
    pub fn insert_or_update(&mut self, key: K, value: V) -> Result<(), String> {
        return Err(String::from("Not Implemented"));
    }

    pub fn delete(&mut self, key: &K) -> Result<bool, String> {
        return Err(String::from("Not Implemented"));
    }

    fn get(&self, key: &K) -> Result<Option<V>, String> {
        return Err(String::from("Not Implemented"));
    }

    fn scan(&self, start: &K, end: &K) -> Result<DBIterator<K, V>, String> {
        return Err(String::from("Not Implemented"));
    }
}
