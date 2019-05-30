# rust-concurrent-hash-map

Concurrent hash map, based on open addressing algorithm with linear probing
for collision resolving. Map supports dynamic resizing with rehashing.

Map used vector for storing entries (key value pairs).

Thread safety is achieved by one RwLock on whole vector and separate RwLocks on each bucket.
All operations on map (except clear and resize) acquires read lock on vector.

When map performs resizing or clear method called, map acquire write lock on vector for
exclusive access. With a well-chosen map capacity, resizing occurs very rarely (or even never).

Methods that read or mutate entries acquires read or write lock on that entry respectively.
Thereby, map allows multiple readers or at most one writer at the same entry at any point in
time.

# Run tests

To run the tests run the following command
```sh
cargo test
```

# View doc

To view doc run the following command
```sh
cargo doc --open
```