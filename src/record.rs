#![allow(dead_code)]
use bincode::deserialize;
use serde::de::Deserialize;
use std::mem::size_of;

/// # Description
/// Record is the elementary part of storage.
///
/// # Examples
#[derive(Deserialize, Default)]
pub struct Record<T>
where
    T: Default,
{
    header: Header<T>,
    data: Vec<u8>,
}

impl<'de, T> Record<T>
where
    T: Deserialize<'de> + Default,
{
    pub fn from_raw(buf: &'de [u8]) -> Option<Self> {
        Some(Self {
            header: Header::from_vec(&buf)?,
            data: Vec::from(&buf[size_of::<Header<T>>() - 7..]),
        })
    }
}

/// # Description
/// # Examples
#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
#[repr(C)]
struct Header<T>
where
    T: Sized + Default,
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

impl<'de, T> Header<T>
where
    T: Deserialize<'de> + Default,
{
    fn from_vec(buf: &'de [u8]) -> Option<Self> {
        deserialize(buf).ok()
    }
}

#[test]
fn test_record_new() {
    let rec: Record<u64> = Record::default();
    assert_eq!(
        rec.header,
        Header {
            magic_byte: 0,
            size: 0,
            flags: 0,
            blob_offset: 0,
            created: 0,
            key: 0u64,
            data_checksum: 0,
            header_checksum: 0,
        }
    );
    assert!(rec.data.is_empty());
}

#[test]
fn test_record_from_raw() {
    use bincode::serialize;

    let raw_data: Vec<u8> = (0..16).map(|i| i).collect();
    let mut rec: Record<u64> = Record::default();
    rec.header.size = raw_data.len() as u64;
    let raw_header = serialize(&rec.header).unwrap();
    assert_eq!(raw_header.len(), size_of::<Header<u64>>() - 7);
    let mut raw_record = raw_header.clone();
    raw_record.extend_from_slice(&raw_data);
    assert_eq!(
        raw_record.len(),
        size_of::<Header<u64>>() - 7 + raw_data.len()
    );

    let new_rec = Record::from_raw(&raw_record).unwrap();
    assert_eq!(new_rec.header, rec.header);
    assert_eq!(new_rec.data.len(), raw_data.len());
}

#[test]
fn test_alignment() {
    #[repr(C, packed)]
    struct TestHeader<T>
    where
        T: Sized + Default,
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

    assert_ne!(size_of::<Header<u64>>(), size_of::<TestHeader<u64>>());
}
