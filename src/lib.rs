//! Concurrent hash map, based on open addressing algorithm with linear probing
//! for collision resolving. Map supports dynamic resizing with rehashing.
//!
//! Map used vector for storing entries (key value pairs).
//!
//! Thread safety is achieved by one RwLock on whole vector and separate RwLocks on each bucket.
//! All operations on map (except clear and resize) acquires read lock on vector.
//!
//! When map performs resizing or clear method called, map acquire write lock on vector for
//! exclusive access. With a well-chosen map capacity, resizing occurs very rarely (or even never).
//!
//! Methods that read or mutate entries acquires read or write lock on that entry respectively.
//! Thereby, map allows multiple readers or at most one writer at the same entry at any point in
//! time.

use std::collections::hash_map::RandomState;
use std::fmt::{Debug, Formatter};
use std::hash::{BuildHasher, Hash, Hasher};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{cmp, fmt, mem};

use owning_ref::{OwningHandle, OwningRef};

/// Default initial capacity used by map when constructing new instance
const DEFAULT_INITIAL_CAPACITY: usize = 16;

/// When map filled, number of baskets is multiplied by this number
const MULTIPLIER: usize = 2;

/// Load factor divisor, used 100 for specify load factor in percentage.
const LOAD_FACTOR_DIVISOR: usize = 100;

/// Load factor. When reached, map performs resizing.
const LOAD_FACTOR: usize = 70;

/// Ordering, that used for atomic operations.
/// Relaxed, because synchronization order achieved by locks.
const ATOMIC_ORDERING: Ordering = Ordering::Relaxed;

/// Map entry that hold key value pair.
#[derive(PartialEq, Clone, Debug)]
pub struct Entry<K, V> {
    key: K,
    value: V,
}

impl<K, V> Entry<K, V> {
    /// Creates new entry by specified key and value
    pub fn new(key: K, value: V) -> Entry<K, V> {
        Entry { key, value }
    }

    /// Returns the key reference
    pub fn key(&self) -> &K {
        &self.key
    }

    /// Returns the value reference
    pub fn value(&self) -> &V {
        &self.value
    }
}

/// Map bucket.
/// Can hold an entry, be removed or empty.
/// We should know that bucket was removed instead of empty: search operation must skip removed
/// buckets until it achieved empty bucket or find occupied bucket with the same key.
#[derive(Clone)]
enum Bucket<K, V> {
    Occupied(Entry<K, V>),
    Removed,
    Empty,
}

impl<K, V> Bucket<K, V> {
    /// Returns an Option with entry value
    fn value(self) -> Option<V> {
        if let Bucket::Occupied(entry) = self {
            Some(entry.value)
        } else {
            None
        }
    }

    /// Returns Result with reference to the value
    fn value_ref(&self) -> Result<&V, ()> {
        if let Bucket::Occupied(ref entry) = self {
            Ok(&entry.value)
        } else {
            Err(())
        }
    }

    /// Returns Result with mutable reference to the value
    fn value_ref_mut(&mut self) -> Result<&mut V, ()> {
        if let Bucket::Occupied(ref mut entry) = self {
            Ok(&mut entry.value)
        } else {
            Err(())
        }
    }
}

/// Internal map struct.
/// Stores vector of buckets, where each bucket guarded by RwLock and hash builder
struct Map<K, V, S> {
    buckets: Vec<RwLock<Bucket<K, V>>>,
    hash_builder: S,
}

impl<K, V, S: BuildHasher> Map<K, V, S> {
    fn with_capacity_and_hasher(capacity: usize, hash_builder: S) -> Map<K, V, S> {
        Map {
            buckets: create_buckets(capacity),
            hash_builder,
        }
    }
}

impl<K: Eq + Hash, V, S: BuildHasher> Map<K, V, S> {
    /// Returns a bucket, guarded by read lock, for specified key.
    ///
    /// Parameter search must be set to true for search bucket operations such as get(), and false
    /// for insert operations.
    fn get_bucket_read_guard(&self, key: &K, search: bool) -> RwLockReadGuard<Bucket<K, V>> {
        self.get_bucket(key, search, |index| self.buckets[index].read().unwrap())
    }

    /// Returns a bucket, guarded by write lock, for specified key.
    ///
    /// Parameter search must be set to true for search bucket operations such as get(), and false
    /// for insert operations.
    fn get_bucket_write_guard(&self, key: &K, search: bool) -> RwLockWriteGuard<Bucket<K, V>> {
        self.get_bucket(key, search, |index| self.buckets[index].write().unwrap())
    }

    /// Returns a bucket, by specified key, extracted from vector by specified closure.
    ///
    /// Parameter search used by scanning algorithm - if search = true than removed buckets will be
    /// skipped, instead removed bucket can be returns. Parameter search must be set to true for
    /// search bucket operations such as get(), and false for insert operations.
    fn get_bucket<'a, F, B>(&self, key: &K, search: bool, bucket_extractor: F) -> B
    where
        F: Fn(usize) -> B,
        B: Deref<Target = Bucket<K, V>>,
    {
        let hash = self.hash(key);
        let mut probe = 0;
        let len = self.buckets.len();

        let mut bucket = bucket_extractor(index(hash, probe, len));

        while match *bucket {
            Bucket::Occupied(ref entry) if entry.key != *key => true,
            Bucket::Removed if search => true,
            _ => false,
        } {
            probe += 1;
            bucket = bucket_extractor(index(hash, probe, len));
        }

        bucket
    }

    /// Inserts entry to underlined vector, without acquiring a lock.
    fn insert_entry_without_lock(&mut self, entry: Entry<K, V>) {
        let hash = self.hash(&entry.key);
        let mut probe = 0;
        let len = self.buckets.len();

        let mut bucket = self.buckets[index(hash, probe, len)].get_mut().unwrap();

        while let Bucket::Occupied(ref old_entry) = *bucket {
            if &old_entry.key == &entry.key {
                mem::replace(&mut *bucket, Bucket::Occupied(entry));
                return;
            }

            probe += 1;
            bucket = self.buckets[index(hash, probe, len)].get_mut().unwrap();
        }

        mem::replace(&mut *bucket, Bucket::Occupied(entry));
    }

    /// Return hash for provided key
    fn hash(&self, key: &K) -> usize {
        let mut hasher = self.hash_builder.build_hasher();
        key.hash(&mut hasher);
        hasher.finish() as usize
    }
}

impl<K: Clone, V: Clone, S: Clone> Clone for Map<K, V, S> {
    fn clone(&self) -> Self {
        Map {
            buckets: self
                .buckets
                .iter()
                .map(|bucket| RwLock::new(bucket.read().unwrap().clone()))
                .collect(),
            hash_builder: self.hash_builder.clone(),
        }
    }
}

// OwningHandle that holds locks, because we can't simple returns value that guarded by lock
type GuardOwningHandle<'a, K, V, S, BG> = OwningHandle<RwLockReadGuard<'a, Map<K, V, S>>, BG>;

/// Guard for reading an entry. Value can be accessed by dereference.
///
/// Guard holds read lock on this map and read lock on entry. Guard will be unlocked on drop
pub struct EntryRwReadGuard<'a, K: 'a, V: 'a, S: 'a> {
    owner: OwningRef<GuardOwningHandle<'a, K, V, S, RwLockReadGuard<'a, Bucket<K, V>>>, V>,
}

impl<'a, K, V, S> Deref for EntryRwReadGuard<'a, K, V, S> {
    type Target = V;

    fn deref(&self) -> &V {
        &self.owner
    }
}

impl<'a, K: Debug, V: Debug, S> Debug for EntryRwReadGuard<'a, K, V, S> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "EntryReadGuard({:?})", &**self)
    }
}

/// Guard for writing an entry. Value can be accessed by dereference.
///
/// Guard holds read lock on this map and write lock on entry. Guard will be unlocked on drop
pub struct EntryRwWriteGuard<'a, K: 'a, V: 'a, S: 'a> {
    owner:
        OwningHandle<GuardOwningHandle<'a, K, V, S, RwLockWriteGuard<'a, Bucket<K, V>>>, &'a mut V>,
}

impl<'a, K, V, S> Deref for EntryRwWriteGuard<'a, K, V, S> {
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.owner
    }
}

impl<'a, K, V, S> DerefMut for EntryRwWriteGuard<'a, K, V, S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.owner
    }
}

impl<'a, K: Debug, V: Debug, S> Debug for EntryRwWriteGuard<'a, K, V, S> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "EntryWriteGuard({:?})", &**self)
    }
}

/// Concurrent hash map, based on open addressing algorithm with linear probing
/// for collision resolving. Map supports dynamic resizing with rehashing. Used locks for thread
/// safety.
///
/// # Examples
///
/// ```
/// use concurrent_hash_map::ConcurrentHashMap;
///
/// // create map
/// let map = ConcurrentHashMap::new();
///
/// let key = 1;
/// let value = "value1";
///
/// // insert value by key
/// assert_eq!(None, map.insert(key, value));
///
/// // read value by key
/// assert_eq!(value, *map.get(&key).unwrap());
///
/// // modify value
/// let value2 = "value2";
/// *map.get_mut(&key).unwrap() = value2;
/// assert_eq!(value2, *map.get(&key).unwrap());
///
/// // check, that map contains some key
/// assert!(map.contains_key(&key));
///
/// // get map length
/// assert_eq!(1, map.len());
///
/// // get map capacity
/// assert!(map.capacity() > 0);
///
/// // remove entry from map
/// let key2 = 2;
/// assert_eq!(None, map.insert(key2, value2));
/// assert_eq!(Some(value2), map.remove(&key2));
///
/// // clone map and print it content
/// let cloned_map = map.clone();
/// println!("cloned map: {:?}", cloned_map);
///
/// // clear map
/// cloned_map.clear();
/// assert_eq!(0, cloned_map.len());
///
/// // iterate over entries
/// for entry in map {
///   println!("{:?}", entry);
/// }
/// ```
pub struct ConcurrentHashMap<K, V, S = RandomState> {
    map: RwLock<Map<K, V, S>>,
    len: AtomicUsize,
    capacity: AtomicUsize,
}

impl<K, V, S> ConcurrentHashMap<K, V, S> {
    /// Clears the map, removing all key-value pairs. Keeps the allocated memory for reuse.
    ///
    /// # Examples
    ///
    /// ```
    /// use concurrent_hash_map::ConcurrentHashMap;
    ///
    /// let map = ConcurrentHashMap::new();
    /// map.insert(1, "a");
    /// map.clear();
    /// assert_eq!(0, map.len());
    /// ```
    pub fn clear(&self) {
        let mut map = self.map.write().unwrap();

        for bucket_lock in &mut map.buckets {
            let bucket = bucket_lock.get_mut().unwrap();
            match bucket {
                Bucket::Occupied(_) | Bucket::Removed => {
                    mem::replace(bucket, Bucket::Empty);
                }
                _ => {}
            }
        }

        self.len.store(0, ATOMIC_ORDERING);
    }

    /// Returns the number of elements in the map.
    ///
    /// # Examples
    ///
    /// ```
    /// use concurrent_hash_map::ConcurrentHashMap;
    ///
    /// let map = ConcurrentHashMap::new();
    /// map.insert(1, "a");
    /// assert_eq!(1, map.len());
    /// ```
    pub fn len(&self) -> usize {
        self.len.load(ATOMIC_ORDERING)
    }

    /// Returns the number of entries that map can hold without resizing.
    ///
    /// # Examples
    ///
    /// ```
    /// use concurrent_hash_map::ConcurrentHashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let capacity = 32;
    /// let map:ConcurrentHashMap<i32, i32, RandomState> = ConcurrentHashMap::with_capacity(capacity);
    /// assert!(map.capacity() >= capacity);
    /// ```
    pub fn capacity(&self) -> usize {
        self.capacity.load(ATOMIC_ORDERING)
    }
}

impl<K, V> ConcurrentHashMap<K, V, RandomState> {
    /// Creates new ConcurrentHashMap with default initial capacity and default hasher
    ///
    /// # Examples
    ///
    /// ```
    /// use concurrent_hash_map::ConcurrentHashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let map:ConcurrentHashMap<i32, i32, RandomState> = ConcurrentHashMap::new();
    /// ```
    pub fn new() -> ConcurrentHashMap<K, V, RandomState> {
        ConcurrentHashMap::with_capacity_and_hasher(DEFAULT_INITIAL_CAPACITY, RandomState::new())
    }

    /// Creates new ConcurrentHashMap with specified initial capacity and default hasher
    ///
    /// # Examples
    ///
    /// ```
    /// use concurrent_hash_map::ConcurrentHashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let map:ConcurrentHashMap<i32, i32, RandomState> = ConcurrentHashMap::with_capacity(32);
    /// ```
    pub fn with_capacity(capacity: usize) -> ConcurrentHashMap<K, V, RandomState> {
        ConcurrentHashMap::with_capacity_and_hasher(capacity, RandomState::new())
    }
}

impl<K, V, S: BuildHasher> ConcurrentHashMap<K, V, S> {
    /// Creates new ConcurrentHashMap with default initial capacity and specified hasher
    ///
    /// # Examples
    ///
    /// ```
    /// use concurrent_hash_map::ConcurrentHashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let map:ConcurrentHashMap<i32, i32, RandomState> = ConcurrentHashMap::with_hasher(RandomState::new());
    /// ```
    pub fn with_hasher(hash_builder: S) -> ConcurrentHashMap<K, V, S> {
        ConcurrentHashMap::with_capacity_and_hasher(DEFAULT_INITIAL_CAPACITY, hash_builder)
    }

    /// Creates new ConcurrentHashMap with specified initial capacity and hasher
    ///
    /// # Examples
    ///
    /// ```
    /// use concurrent_hash_map::ConcurrentHashMap;
    /// use std::collections::hash_map::RandomState;
    ///
    /// let map:ConcurrentHashMap<i32, i32, RandomState> = ConcurrentHashMap::with_capacity_and_hasher(32, RandomState::new());
    /// ```
    pub fn with_capacity_and_hasher(
        capacity: usize,
        hash_builder: S,
    ) -> ConcurrentHashMap<K, V, S> {
        let capacity = cmp::max(DEFAULT_INITIAL_CAPACITY, capacity) * MULTIPLIER;

        ConcurrentHashMap {
            map: RwLock::new(Map::with_capacity_and_hasher(capacity, hash_builder)),
            len: AtomicUsize::new(0),
            capacity: AtomicUsize::new(self::capacity(capacity)),
        }
    }
}

impl<K: Eq + Hash, V, S: BuildHasher> ConcurrentHashMap<K, V, S> {
    /// Returns a entry guard for value reading, corresponding to the key.
    ///
    /// Value can be accessed by dereference. Returned guard will be unlocked on drop.
    ///
    /// # Examples
    ///
    /// ```
    /// use concurrent_hash_map::ConcurrentHashMap;
    ///
    /// let map = ConcurrentHashMap::new();
    ///
    /// let key = 1;
    /// let value = "value1";
    ///
    /// assert_eq!(None, map.insert(key, value));
    /// assert_eq!(value, *map.get(&key).unwrap());
    /// ```
    pub fn get(&self, key: &K) -> Option<EntryRwReadGuard<K, V, S>> {
        let owning_handle = OwningHandle::new_with_fn(self.map.read().unwrap(), |map| {
            unsafe { &*map }.get_bucket_read_guard(key, true)
        });

        let ref_result = OwningRef::new(owning_handle).try_map(|bucket| bucket.value_ref());

        if let Ok(owner) = ref_result {
            Some(EntryRwReadGuard { owner })
        } else {
            None
        }
    }

    /// Returns a entry guard for value reading and writing, corresponding to the key.
    ///
    /// Value can be accessed by dereference. Returned guard will be unlocked on drop.
    ///
    /// # Examples
    ///
    /// ```
    /// use concurrent_hash_map::ConcurrentHashMap;
    ///
    /// let map = ConcurrentHashMap::new();
    ///
    /// let key = 1;
    /// let value = "value1";
    ///
    /// assert_eq!(None, map.insert(key, value));
    /// assert_eq!(value, *map.get(&key).unwrap());
    ///
    /// let value2 = "value2";
    /// *map.get_mut(&key).unwrap() = value2;
    /// assert_eq!(value2, *map.get(&key).unwrap());
    /// ```
    pub fn get_mut(&self, key: &K) -> Option<EntryRwWriteGuard<K, V, S>> {
        let owning_handle = OwningHandle::new_with_fn(self.map.read().unwrap(), |map| {
            unsafe { &*map }.get_bucket_write_guard(key, true)
        });

        let ref_result = OwningHandle::try_new(owning_handle, |bucket| {
            unsafe { &mut *(bucket as *mut Bucket<K, V>) }.value_ref_mut()
        });

        if let Ok(owner) = ref_result {
            Some(EntryRwWriteGuard { owner })
        } else {
            None
        }
    }

    /// Returns `true` if the map contains a value for the specified key, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```
    /// use concurrent_hash_map::ConcurrentHashMap;
    ///
    /// let map = ConcurrentHashMap::new();
    ///
    /// map.insert(1, "a");
    ///
    /// assert!(map.contains_key(&1));
    /// assert!(!map.contains_key(&2));
    /// ```
    pub fn contains_key(&self, key: &K) -> bool {
        let map = self.map.read().unwrap();
        let bucket = map.get_bucket_read_guard(key, true);

        if let Bucket::Occupied(_) = *bucket {
            true
        } else {
            false
        }
    }

    /// Inserts a key-value pair into the map.
    ///
    /// If the map did not have this key present, [`None`] is returned.
    /// If the map have this key present, value is updated and previous [`Some(value)`] is returned.
    ///
    /// # Examples
    ///
    /// ```
    /// use concurrent_hash_map::ConcurrentHashMap;
    ///
    /// let map = ConcurrentHashMap::new();
    ///
    /// let key = 1;
    /// let value1 = "a";
    ///
    /// let value2 = "b";
    ///
    /// assert_eq!(None, map.insert(key, value1));
    /// assert_eq!(Some(value1), map.insert(key, value2));
    /// ```
    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let map = self.map.read().unwrap();
        {
            let mut bucket = map.get_bucket_write_guard(&key, false);

            let new_bucket = Bucket::Occupied(Entry { key, value });

            if let Bucket::Occupied(_) = *bucket {
                return mem::replace(&mut *bucket, new_bucket).value();
            }

            mem::replace(&mut *bucket, new_bucket);
        }

        self.ensure_capacity(map);

        None
    }

    /// Removes a key from the map, returning the value if the key was previously exists in the map.
    ///
    /// # Examples
    ///
    /// ```
    /// use concurrent_hash_map::ConcurrentHashMap;
    ///
    /// let map = ConcurrentHashMap::new();
    ///
    /// let key = 1;
    /// let value = "a";
    ///
    /// map.insert(key, value);
    /// assert_eq!(Some(value), map.remove(&key));
    /// assert_eq!(None, map.remove(&key));
    /// ```
    pub fn remove(&self, key: &K) -> Option<V> {
        let map = self.map.read().unwrap();
        let mut bucket = map.get_bucket_write_guard(key, true);

        if let Bucket::Occupied(_) = *bucket {
            self.len.fetch_sub(1, ATOMIC_ORDERING);
            return mem::replace(&mut *bucket, Bucket::Removed).value();
        }

        None
    }

    /// Increase the map size and grow map, if needed.
    fn ensure_capacity(&self, map_read_guard: RwLockReadGuard<Map<K, V, S>>) {
        let size = self.len.fetch_add(1, ATOMIC_ORDERING) + 1;
        let capacity = self.capacity.load(ATOMIC_ORDERING);

        if size > capacity {
            // drop read lock and acquire write lock on whole map
            drop(map_read_guard);

            let mut map = self.map.write().unwrap();

            if capacity != self.capacity.load(ATOMIC_ORDERING) {
                // another thread already acquire write lock and expand map
                return;
            }

            let new_len = map.buckets.len() * MULTIPLIER;
            let new_capacity = self::capacity(new_len);

            self.capacity.store(new_capacity, ATOMIC_ORDERING);

            let old_buckets = mem::replace(&mut map.buckets, create_buckets(new_len));

            // move entries from old buckets to new without locks
            for old_bucket in old_buckets {
                if let Bucket::Occupied(entry) = old_bucket.into_inner().unwrap() {
                    map.insert_entry_without_lock(entry);
                }
            }
        }
    }
}

impl<K, V> Default for ConcurrentHashMap<K, V, RandomState> {
    fn default() -> ConcurrentHashMap<K, V, RandomState> {
        ConcurrentHashMap::new()
    }
}

impl<K: Clone, V: Clone, S: Clone> Clone for ConcurrentHashMap<K, V, S> {
    fn clone(&self) -> Self {
        let map = self.map.read().unwrap();

        ConcurrentHashMap {
            map: RwLock::new(map.clone()),
            len: AtomicUsize::new(self.len.load(ATOMIC_ORDERING)),
            capacity: AtomicUsize::new(self.capacity.load(ATOMIC_ORDERING)),
        }
    }
}

/// An iterator over the entries of map
pub struct IntoIter<K, V, S> {
    map: Map<K, V, S>,
}

impl<K, V, S> Iterator for IntoIter<K, V, S> {
    type Item = Entry<K, V>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(bucket) = self.map.buckets.pop() {
            if let Bucket::Occupied(entry) = bucket.into_inner().unwrap() {
                return Some(entry);
            }
        }

        None
    }
}

impl<K, V, S> IntoIterator for ConcurrentHashMap<K, V, S> {
    type Item = Entry<K, V>;
    type IntoIter = IntoIter<K, V, S>;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            map: self.map.into_inner().unwrap(),
        }
    }
}

impl<K: Debug, V: Debug, S> Debug for ConcurrentHashMap<K, V, S> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut debug_map = f.debug_map();
        for bucket_lock in &self.map.read().unwrap().buckets {
            let bucket = bucket_lock.read().unwrap();
            if let Bucket::Occupied(ref entry) = *bucket {
                debug_map.entry(&entry.key, &entry.value);
            }
        }
        debug_map.finish()
    }
}

/// Returns created vector of buckets, where each bucket guarded by own RwLock
fn create_buckets<K, V>(capacity: usize) -> Vec<RwLock<Bucket<K, V>>> {
    let mut buckets = Vec::with_capacity(capacity);

    for _ in 0..capacity {
        buckets.push(RwLock::new(Bucket::Empty));
    }

    buckets
}

/// Returns entries count, that can be stored in map without resizing, evaluated on buckets length
fn capacity(buckets_len: usize) -> usize {
    buckets_len * LOAD_FACTOR / LOAD_FACTOR_DIVISOR
}

/// Returns an index in buckets vector from key hash, probe number, and buckets length
fn index(hash: usize, probe: usize, len: usize) -> usize {
    (hash + probe) % len
}
