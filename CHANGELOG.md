# Changelog
Pearl changelog

## [Unreleased]
#### Added

#### Changed
- Add key size check for blob from file ([#99](https://github.com/qoollo/pearl/pull/99))
- `ahash` version fixed on `=v0.7.4` ([#112](https://github.com/qoollo/pearl/pull/112))

#### Fixed
- Fix warnings ([#100](https://github.com/qoollo/pearl/pull/100)) 

#### Updated


## [0.5.14] - 2021-04-14
#### Fixed
- Fix possible infinite readlock (#97)
  - Change all readlocks on writelocks
  - add more traces, increase check timer
  - disable old blobs dump
  - fix benchmark and lock issue


## [0.5.13] - 2021-04-13
#### Fixed
- Fix observer worker leak and simplify logic
- Fix 'core dumped' bug


## [0.5.12] - 2021-03-30
#### Added
- Add check before fsync index file in dump (#90)
- Add an option to ignore corrupted blobs (#86)

#### Changed
- Switch to RwLock for storage inner safe (instead of Mutex) (#91)

#### Updated
- update libs


## [0.5.11] - 2021-03-24
#### Added
- Add synchronization for closing blobs to keep them in stable state in case of future disk faults (#85)
- Add an option to ignore corrupted blobs (#86)

#### Updated
- tokio update v1.3


## [0.5.0] - 2020-08-21
#### Changed
- reduced disk IO calls when index used
- remove Entries
- Replace Error::RecordNotFound with Option
- use anyhow crate for result/error handling

#### Fixed
- wrong offset of index file read


## [0.4.0] - 2020-07-30
#### Added
- use of the io_uring, linux asynchronous I/O API.


## [0.3.1] - 2020-05-15
#### Added
- storage records count methods all/active/detailed


## [0.3.0] - 2020-03-02
#### Added
- CHANGELOG.md
