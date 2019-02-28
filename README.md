Pearl
=====
[![Build Status](https://travis-ci.org/qoollo/pearl.svg?branch=master)](https://travis-ci.org/qoollo/pearl)

Append only key-value blob storage on disk

Table of Contents
=================

* [Storage scheme](#storage-scheme)
* [Blob](#blob)
  * [Header](#header)
* [Record](#record)
  * [Header](#header)
* [Index](#index)
  * [Header](#header)

# Storage scheme
![pearl storage scheme](pearl_storage_scheme.svg)

# Blob
## Header

Structure:
```rust
struct Header {
    magic_byte: u64,
    version: u32,
    key_size: u32,
    flags: u64,
}
```
Description

| Field       | Size, B | Description |
| ----------- | :----:  | :----------- |
|magic_byte   | 8       | marks `pearl` blob
|version      |any size | used to check compatibility
|key_size     |8        | size of key in record
|flags        |1        | additional file props

# Record
## Header

Structure:
```rust
struct Header<T>
where
    T: Sized,
{
    magic_byte: u64,
    size: u64,
    flags: u8,
    blob_offset: u64,
    created: u64,
    key: T,
    data_checksum: u32,
    header_checksum: u32,
}
```
Description

| Field          | Size, B | Description |
| -------------- | :----:  | :----------- |
|magic_byte      | 8       | separates records in blob
|size            | 8       | data length (without header)
|flags           | 1       | additional record metadata
|blob_offset     | 8       | record offset from blob start
|created         | 8       | created timestamp
|key             | (any)   | key for record location and searching
|data_checksum   | 4       | data crc32 checksum (without header)
|header_checksum | 4       | header crc32 checksum (only record header)


# Index
## Header

Structure:
```rust
struct Header {
    magic_byte: u64,
    version: u32,
    key_size: u32,
    flags: u64,
}
```
Description

| Field       | Size, B | Description |
| ----------- | :----:  | :----------- |
|magic_byte   | 8       | marks pearl index file
|version      | 4       | used to check compatibility
|key_size     | 4       | size of key in record
|flags        | 8       | additional index props