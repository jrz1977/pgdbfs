PostgreSQL Database as File System

**pgdbfs** is a basic file system implemented in Rust using libfuse and PostgreSQL as backend storage. Manage files in databas using familiar Linux directory and file manipulation commands and utilities.

## Why

Please read through PostgreSQL wiki to understand the benefits and tradeoffs of storing files in Database. [https://wiki.postgresql.org/wiki/BinaryFilesInDB](https://wiki.postgresql.org/wiki/BinaryFilesInDB)

There is a small subset of valid use cases to store files in DB

  - Simple central and distributed file based configuration for services
  - Replication and HA is taken care of by DB replication
  - Lots of small file operations
  - Files are infrequently read or accessed
  - DB Backups automatically include files

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
$ RUST_LOG=info cargo run -- -m /tmp/my_storage -f ~/.pgdbfs/pgdbfs.toml
```
## Troubleshooting

### Enable debug
```
$ export RUST_LOG="pgdbfs::fsys=debug,pgdbfs::db=debug,pgdbfs::fcache=debug"
$ cargo run -- -m /tmp/my_storage -f ~/.pgdbfs/pgdbfs.toml
```

### Fails to start

If it fails to start with the following error:

```
fuse: failed to open mountpoint for reading: Transport endpoint is not connected
thread 'main' panicked at 'Box<Any>', src/fsys/mod.rs:556:13
```
Execute
```
$ cd $HOME
$ fusermount -u /tmp/my_storage
```

Discuss on [Discord](https://discord.gg/KbmpmvVETF)
