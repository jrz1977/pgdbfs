PostgreSQL Database as File System

**pgdbfs** is a toy file system implemented in Rust using libfuse and PostgreSQL as backend storage.

## Requirements
- postgresql 10+
- fuse 2.5 or later
- Rust toolchain 1.49
- Clone repository 

## Setup

### Setup DB Schema

- Create database to use for pgdbfs
- Run pgdbfs.sql to initialize schema

### Setup configuration file

- Create config file in $HOME/.pgdbfs/pgdbfs.toml

```db_host = 'localhost'
db_user = 'pgdbfs'
db_pass = 'pgdbfs'
db_segment_len = 1048576
```
### Setup mount point
```
$ mkdir -p $HOME/pgdbfs/my_storage
```
## Running the Filesystem
```
$ RUST_LOG=info cargo run $HOME/pgdbfs/my_storage
```
