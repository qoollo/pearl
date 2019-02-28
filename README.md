Pearl
=====
[![Build Status](https://travis-ci.org/qoollo/pearl.svg?branch=master)](https://travis-ci.org/qoollo/pearl)

Append only key-value blob storage on disk

Table of Contents
=================
* [Blob structure](#blob-structure)
  * [Header](#header)

# Blob structure
## Header

Structure:
```Rust
struct Header<T>
where
    T: Sized,
{
    magic_byte: u64,
    key: T,
    flags: u8,
    size: u64,
    blob_offset: u64,
    created: u64,
    modified: u64,
    checksum: u32,
}
```
Description

| Field       | Size, B | Description |
| ----------- | :----:  | :----------- |
|magic_byte   | 8       | separates files in blob
|key          |any size | used to determined file location and searching
|flags        |1        | additional file props, e.g. `DELETED`
|size         |8        | size of data in record, without header
|blob_offset  |8        | record offset in blob
|created      |8        | created timestamp
|modified     |8        | modified timestamp
|checksum     |4        | checksum CRC32
