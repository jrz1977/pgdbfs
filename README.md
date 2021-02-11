PostgreSQL Database as File System

**pgdbfs** is a basic file system implemented in Rust using libfuse and PostgreSQL as backend storage.

## Why

- Small non trivial project to learn Rust
- There may be a small subset of valid use cases to store files in DB
  - Replication and HA is taken care of by DB replication
  - Lots of small file operations
 

## Requirements
- postgresql 10+
- fuse 2.5 or later
- Rust toolchain 1.49
- Clone repository 

## Setup

### Setup mount point
```
$ mkdir -p /tmp/my_storage
```

### Setup DB Schema

- Create database to use for pgdbfs
- Update pgdbfs.sql change line to point to the mount point using full path
```
\set mntpt /tmp/my_storage
```
- Run pgdbfs.sql to initialize schema

### Setup configuration file

- Create config file in $HOME/.pgdbfs/pgdbfs.toml

```
db_host = 'localhost'
db_user = 'pgdbfs'
db_pass = 'pgdbfs'
db_segment_len = 1048576
```
## Running the Filesystem
```
$ RUST_LOG=info cargo run /tmp/my_storage
```
## Troubleshooting

### Enable debug
```
export RUST_LOG="pgdbfs::fsys=debug,pgdbfs::db=debug,pgdbfs::fcache=debug"
cargo run /tmp/my_storage
```
