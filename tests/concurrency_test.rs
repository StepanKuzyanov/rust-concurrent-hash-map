use std::sync::Arc;
use std::thread;

use concurrent_hash_map::ConcurrentHashMap;
use core::borrow::Borrow;

/// Each test repeats count
const TEST_COUNT: usize = 32;

/// Test concurrent get value
///
/// Two threads perform insert and get operations on different keys.
///
/// Assert that if insert occurs before get, than get should view changes performing that insert.
/// Assert that map length after inserts equals inserts count.
#[test]
fn get_test() {
    for _ in 0..TEST_COUNT {
        let map = Arc::new(ConcurrentHashMap::new());

        let key1 = 1;
        let value1 = "value1";

        let key2 = 2;
        let value2 = "value2";

        let map_ref = Arc::clone(&map);
        let handle1 = thread::spawn(move || {
            map_ref.insert(key1, value1);
            *map_ref.get(&key1).unwrap()
        });

        let map_ref = Arc::clone(&map);
        let handle2 = thread::spawn(move || {
            map_ref.insert(key2, value2);
            *map_ref.get(&key2).unwrap()
        });

        let r1 = handle1.join().unwrap();
        let r2 = handle2.join().unwrap();

        assert_contains((r1, r2), &[(value1, value2)]);
        assert_eq!(2, map.len());
    }
}

/// Test concurrent get and modify value
///
/// Two threads perform get and modify values on the same key.
///
/// Assert that if one thread t1 get and modify value before another thread t2, than thread t2 must
/// view value written by t1.
/// Assert that map length after modify value equals inserts count.
#[test]
fn get_mut_test() {
    for _ in 0..TEST_COUNT {
        let map = Arc::new(ConcurrentHashMap::new());

        let key = 1;
        let value = true;
        let new_value = false;

        map.insert(key, value);

        let map_ref = Arc::clone(&map);
        let get_mut_handle1 = thread::spawn(move || {
            let mut value = map_ref.get_mut(&key).unwrap();
            let prev_value = *value;
            *value = new_value;
            (prev_value, new_value)
        });

        let map_ref = Arc::clone(&map);
        let get_mut_handle2 = thread::spawn(move || {
            let mut value = map_ref.get_mut(&key).unwrap();
            let prev_value = *value;
            *value = new_value;
            (prev_value, new_value)
        });

        let r1 = get_mut_handle1.join().unwrap();
        let r2 = get_mut_handle2.join().unwrap();

        assert_contains(
            (r1, r2),
            &[
                ((value, new_value), (new_value, new_value)),
                ((new_value, new_value), (value, new_value)),
            ],
        );
        assert_eq!(1, map.len());
    }
}

/// Test concurrent contains key check
///
/// Two threads perform insert and contains key checking on different keys.
///
/// Assert that if insert occurs before contains, than contains method should return true.
/// Assert that map length after inserts equals inserts count.
#[test]
fn contains_test() {
    for _ in 0..TEST_COUNT {
        let map = Arc::new(ConcurrentHashMap::new());

        let key1 = 1;
        let value1 = "value1";

        let key2 = 2;
        let value2 = "value2";

        let map_ref = Arc::clone(&map);
        let handle1 = thread::spawn(move || {
            map_ref.insert(key1, value1);
            map_ref.contains_key(&key1)
        });

        let map_ref = Arc::clone(&map);
        let handle2 = thread::spawn(move || {
            map_ref.insert(key2, value2);
            map_ref.contains_key(&key2)
        });

        let r1 = handle1.join().unwrap();
        let r2 = handle2.join().unwrap();

        assert_contains((r1, r2), &[(true, true)]);
        assert_eq!(2, map.len());
    }
}

/// Test concurrent insert
///
/// Two threads perform insert on the same keys.
///
/// Assert that if insert i1 occurs before insert i2, than i2 must view changes by i1
/// Assert that map length after inserts equals inserts count.
#[test]
fn insert_test() {
    for _ in 0..TEST_COUNT {
        let map = Arc::new(ConcurrentHashMap::new());

        let key = 1;
        let value = "value";

        let map_ref = Arc::clone(&map);
        let handle1 = thread::spawn(move || map_ref.insert(key, value));

        let map_ref = Arc::clone(&map);
        let handle2 = thread::spawn(move || map_ref.insert(key, value));

        let r1 = handle1.join().unwrap();
        let r2 = handle2.join().unwrap();

        assert_contains((r1, r2), &[(None, Some(value)), (Some(value), None)]);
        assert_eq!(1, map.len());
    }
}

/// Test concurrent resize
///
/// Two threads perform multiple inserts, the number of which is twice initial capacity.
///
/// Assert that capacity grows no more than double.
/// Assert that map length after inserts equals inserts count.
#[test]
fn resize_test() {
    for _ in 0..TEST_COUNT {
        let map = Arc::new(ConcurrentHashMap::new());

        let capacity = map.capacity();

        let threads_count = capacity * 2;

        let mut handles = Vec::with_capacity(threads_count);

        for i in 0..threads_count {
            let map = Arc::clone(&map);
            handles.push(thread::spawn(move || assert_eq!(None, map.insert(i, i))));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(threads_count, map.len());
        assert!(map.capacity() > capacity && map.capacity() / capacity <= 2);
    }
}

/// Test concurrent remove
///
/// Two threads perform remove on the same keys.
///
/// Assert that if remove r1 occurs before remove r2, than r2 must view changes by r1
/// Assert that map length after removes equals to zero.
#[test]
fn remove_test() {
    for _ in 0..TEST_COUNT {
        let map = Arc::new(ConcurrentHashMap::new());

        let key = 1;
        let value = "value";

        map.insert(key, value);

        let map_ref = Arc::clone(&map);
        let handle1 = thread::spawn(move || map_ref.remove(&key));

        let map_ref = Arc::clone(&map);
        let handle2 = thread::spawn(move || map_ref.remove(&key));

        let r1 = handle1.join().unwrap();
        let r2 = handle2.join().unwrap();

        assert_contains((r1, r2), &[(Some(value), None), (None, Some(value))]);
        assert_eq!(0, map.len());
    }
}

/// Test concurrent clone
///
/// Two threads perform insert, clone map and than get value from cloned map.
///
/// Assert that if insert occurs before clone, and clone occurs before get, than get method called
/// on cloned map must return inserted value to original map.
/// Assert that map length after inserts equals inserts count.
#[test]
fn clone_test() {
    for _ in 0..TEST_COUNT {
        let map = Arc::new(ConcurrentHashMap::new());

        let key1 = 1;
        let value1 = "value1";

        let key2 = 2;
        let value2 = "value2";

        let map_ref = Arc::clone(&map);
        let handle1 = thread::spawn(move || {
            map_ref.insert(key1, value1);
            let map_clone = ConcurrentHashMap::clone(map_ref.borrow());
            let value = *map_clone.get(&key1).unwrap();
            value
        });

        let map_ref = Arc::clone(&map);
        let handle2 = thread::spawn(move || {
            map_ref.insert(key2, value2);
            let map_clone = ConcurrentHashMap::clone(map_ref.borrow());
            let value = *map_clone.get(&key2).unwrap();
            value
        });

        let r1 = handle1.join().unwrap();
        let r2 = handle2.join().unwrap();

        assert_contains((r1, r2), &[(value1, value2)]);
        assert_eq!(2, map.len());
    }
}

/// Test concurrent clear
///
/// Two threads perform insert, clear map and than get.
///
/// Assert that if insert occurs before clear, and clear occurs before get, than get method must
/// return empty value.
/// Assert that map length after clears equals to zero.
#[test]
fn clear_test() {
    for _ in 0..TEST_COUNT {
        let map = Arc::new(ConcurrentHashMap::new());

        let key1 = 1;
        let value1 = "value1";

        let key2 = 2;
        let value2 = "value2";

        let map_ref = Arc::clone(&map);
        let handle1 = thread::spawn(move || {
            map_ref.insert(key1, value1);
            map_ref.clear();
            map_ref.get(&key1).is_none()
        });

        let map_ref = Arc::clone(&map);
        let handle2 = thread::spawn(move || {
            map_ref.insert(key2, value2);
            map_ref.clear();
            map_ref.get(&key2).is_none()
        });

        let r1 = handle1.join().unwrap();
        let r2 = handle2.join().unwrap();

        assert_contains((r1, r2), &[(true, true)]);
        assert_eq!(0, map.len());
    }
}

/// Assert that actual value contains in expected values array
fn assert_contains<T: PartialEq>(actual: T, expected: &[T]) {
    assert!(expected.contains(&actual));
}
