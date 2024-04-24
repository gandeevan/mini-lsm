# Mini-LSM

Mini-LSM is a disk-based key-value store library built in Rust for educational purposes. It aims to provide a simplified implementation of the Log-Structured Merge-Tree (LSM-Tree) architecture, which is commonly used in modern database systems. While Mini-LSM is not intended for production use, it serves as a valuable learning resource for understanding the core concepts and components of an LSM-based storage engine.

## Features

- [x] Write-Ahead Logging (WAL): Mini-LSM ensures durability by recording all write operations to a write-ahead log before applying them to the main storage.
- [ ] Recovery from WAL: In case of a system failure, Mini-LSM can recover the database state by replaying the operations stored in the write-ahead log.
- [ ] Log Spilling: When the in-memory buffer reaches a certain threshold, Mini-LSM spills the data to disk by creating immutable log files.
- [ ] Log Compaction: Mini-LSM performs periodic compaction of log files to optimize storage space and improve read performance.
- [ ] Asynchronous I/O: Mini-LSM leverages Rust's asynchronous I/O capabilities to enable efficient non-blocking I/O operations, enhancing performance and scalability.
- [ ] Thread-per-Core Architecture: Mini-LSM utilizes a thread-per-core architecture, where each thread is pinned to a specific CPU core. This architecture maximizes CPU utilization and minimizes context switching overhead, resulting in improved performance and scalability.

## Getting Started

To get started with Mini-LSM, follow these steps:

1. Add Mini-LSM as a dependency in your Rust project's `Cargo.toml`:
   ```toml
   [dependencies]
   mini-lsm = "0.1.0"
   ```

2. Import the Mini-LSM library in your Rust code:
   ```rust
   use mini_lsm::DB;
   ```

3. Create an instance of the Mini-LSM database and perform read/write operations:
   ```rust
   let db = DB::new("path/to/db").unwrap();
   db.insert_or_update(b"key", b"value").unwrap();
   let value = db.get(b"key").unwrap();
   ```

## Contributing

Contributions to Mini-LSM are welcome! If you find any bugs, have feature requests, or want to contribute improvements, please open an issue or submit a pull request on the [GitHub repository](https://github.com/gandeevan/mini-lsm).

## License

Mini-LSM is released under the [MIT License](https://opensource.org/licenses/MIT).

## Acknowledgements

Mini-LSM is inspired by the design and concepts of popular LSM-based database systems, such as LevelDB, and RocksDB.

## Contact

For any questions or inquiries, please contact the project maintainer at [gandeevan8@gmail.com](mailto:gandeevan8@gmail.com).

Happy coding with Mini-LSM in Rust!
