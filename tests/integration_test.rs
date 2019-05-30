use std::collections::hash_map::RandomState;

use concurrent_hash_map::{ConcurrentHashMap, Entry};

#[test]
fn get_test() {
    let map = ConcurrentHashMap::new();

    let key = 1;
    let mut value = "value";

    assert!(map.get(&key).is_none());

    map.insert(key, value);
    assert_eq!(value, *map.get(&key).unwrap());

    value = "value2";
    map.insert(key, value);
    assert_eq!(value, *map.get(&key).unwrap());
}

#[test]
fn get_mut_test() {
    let map = ConcurrentHashMap::new();

    let key = 1;
    let mut value = "value";

    assert!(map.get_mut(&key).is_none());

    map.insert(key, value);
    assert_eq!(value, *map.get_mut(&key).unwrap());

    value = "value2";
    *map.get_mut(&key).unwrap() = value;
    assert_eq!(value, *map.get_mut(&key).unwrap());
}

#[test]
fn contains_key_test() {
    let map = ConcurrentHashMap::new();

    let key = 1;
    let mut value = "value";

    assert!(!map.contains_key(&key));

    map.insert(key, value);
    assert!(map.contains_key(&key));

    value = "value2";
    map.insert(key, value);
    assert!(map.contains_key(&key));
}

#[test]
fn insert_test() {
    let map = ConcurrentHashMap::new();

    let key = 1;
    let value = "value";

    assert_eq!(0, map.len());
    assert_eq!(None, map.insert(key, value));

    assert_eq!(Some(value), map.insert(key, value));
    assert_eq!(Some(value), map.insert(key, value));
    assert_eq!(1, map.len());
}

#[test]
fn remove_test() {
    let map = ConcurrentHashMap::new();

    let key = 1;
    let value = "value";

    assert_eq!(0, map.len());
    assert_eq!(None, map.remove(&key));

    assert_eq!(None, map.insert(key, value));
    assert_eq!(1, map.len());

    assert_eq!(Some(value), map.remove(&key));
    assert_eq!(0, map.len());
}

#[test]
fn clear_test() {
    let map = ConcurrentHashMap::new();

    let key1 = 1;
    let value1 = "value1";

    let key2 = 2;
    let value2 = "value2";

    assert_eq!(0, map.len());

    assert_eq!(None, map.insert(key1, value1));
    assert_eq!(None, map.insert(key2, value2));
    assert_eq!(2, map.len());

    map.clear();

    assert_eq!(0, map.len());
}

#[test]
fn len_test() {
    let map = ConcurrentHashMap::new();

    let key1 = 1;
    let value1 = "value1";

    let key2 = 2;
    let value2 = "value2";

    assert_eq!(0, map.len());

    assert_eq!(None, map.insert(key1, value1));
    assert_eq!(1, map.len());

    assert_eq!(None, map.insert(key2, value2));
    assert_eq!(Some(value2), map.insert(key2, value2));
    assert_eq!(2, map.len());

    assert_eq!(Some(value1), map.remove(&key1));
    assert_eq!(1, map.len());

    assert_eq!(Some(value2), map.remove(&key2));
    assert_eq!(0, map.len());
}

#[test]
fn capacity_test() {
    let initial_capacity = 32;

    let map = ConcurrentHashMap::with_capacity(initial_capacity);

    let capacity = map.capacity();

    assert!(capacity >= initial_capacity);

    for i in 0..capacity {
        map.insert(i, i);
    }

    assert_eq!(capacity, map.capacity());

    for i in capacity..capacity * 2 {
        map.insert(i, i);
    }

    assert!(map.capacity() > capacity);
}

#[test]
fn into_iterator_test() {
    let map = ConcurrentHashMap::new();

    let entry1 = Entry::new(1, "value1");
    let entry2 = Entry::new(2, "value2");

    map.insert(*entry1.key(), *entry1.value());
    map.insert(*entry2.key(), *entry2.value());

    let entries: Vec<Entry<i32, &str>> = map.into_iter().collect();

    assert_eq!(2, entries.len());
    assert!(entries.contains(&entry1));
    assert!(entries.contains(&entry2));
}

#[test]
fn clone_test() {
    let origin_map = ConcurrentHashMap::new();

    let key = 1;

    origin_map.insert(key, "value1");

    let cloned_map = origin_map.clone();

    assert_eq!(origin_map.len(), cloned_map.len());
    assert_eq!(origin_map.capacity(), cloned_map.capacity());

    assert_eq!(
        *origin_map.get(&key).unwrap(),
        *cloned_map.get(&key).unwrap()
    );

    cloned_map.insert(key, "value2");

    assert_ne!(
        *origin_map.get(&key).unwrap(),
        *cloned_map.get(&key).unwrap()
    );
}

#[test]
fn with_capacity_and_hasher_test() {
    let capacity = 32;

    let map: ConcurrentHashMap<i32, i32, RandomState> =
        ConcurrentHashMap::with_capacity_and_hasher(capacity, RandomState::new());

    assert_eq!(0, map.len());
    assert!(map.capacity() >= capacity);
}

#[test]
fn with_capacity_test() {
    let capacity = 32;

    let map: ConcurrentHashMap<i32, i32, RandomState> = ConcurrentHashMap::with_capacity(capacity);

    assert_eq!(0, map.len());
    assert!(map.capacity() >= capacity);
}

#[test]
fn with_hasher_test() {
    let map: ConcurrentHashMap<i32, i32, RandomState> =
        ConcurrentHashMap::with_hasher(RandomState::new());

    assert_eq!(0, map.len());
    assert!(map.capacity() > 0);
}

#[test]
fn new_test() {
    let map: ConcurrentHashMap<i32, i32, RandomState> = ConcurrentHashMap::new();
    assert_eq!(0, map.len());
    assert!(map.capacity() > 0);
}

#[test]
fn default_test() {
    let map: ConcurrentHashMap<i32, &str, RandomState> = Default::default();
    assert_eq!(0, map.len());
    assert!(map.capacity() > 0);
}
