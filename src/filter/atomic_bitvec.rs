use std::sync::atomic::*;

pub(crate) struct AtomicBitVec {
    data: Vec<AtomicU64>,
    bits_count: usize
}

#[derive(Debug)]
pub(crate) enum AtomicBitVecError {
    BitsCountMismatch,
    DataLengthLessThanRequired
}


impl AtomicBitVec {
    const ITEM_BYTES_SIZE: usize = 8;
    const ITEM_BITS_SIZE: usize = 64;

    const fn offset_and_mask(bit_index: usize) -> (usize, u64) {
        let mask = 1u64 << (bit_index & (64 - 1));
        let offset = bit_index >> 6;
        (offset, mask)
    }

    const fn items_count(bits_count: usize) -> usize {
        if bits_count > 0 { 
            ((bits_count - 1) / Self::ITEM_BITS_SIZE) + 1 
        } else {
            0
        }
    }

    pub(crate) fn new(bits_count: usize) -> Self {
        let items_count = Self::items_count(bits_count);
        let mut data = Vec::with_capacity(items_count);
        for _ in 0..items_count {
            data.push(AtomicU64::new(0));
        }

        Self {
            data,
            bits_count
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.bits_count
    }
    pub(crate) fn size_in_mem(&self) -> usize {
        self.data.capacity() * Self::ITEM_BYTES_SIZE
    }


    #[inline]
    pub(crate) fn get(&self, index: usize, ordering: Ordering) -> bool {
        debug_assert!(index < self.bits_count);

        let (offset, mask) = Self::offset_and_mask(index);
        let item = &self.data[offset];
        item.load(ordering) & mask != 0
    }

    #[inline]
    pub(crate) fn set(&self, index: usize, value: bool, ordering: Ordering) -> bool {
        debug_assert!(index < self.bits_count);

        let (offset, mask) = Self::offset_and_mask(index);
        let item = &self.data[offset];
        let prev = if value {
            item.fetch_or(mask, ordering)
        } else {
            item.fetch_and(!mask, ordering)
        };

        prev & mask != 0
    }

    /// Merge bits from 'other' into self. Other is mut to prevent data modification during merge
    pub(crate) fn merge_with(&mut self, other: &mut Self, ordering: Ordering) -> Result<(), AtomicBitVecError> {
        if self.bits_count != other.bits_count {
            return Err(AtomicBitVecError::BitsCountMismatch);
        }

        let load_ordering = match ordering {
            Ordering::Release => Ordering::Relaxed,
            Ordering::AcqRel => Ordering::Acquire,
            v => v
        };

        for i in 0..self.data.len() {
            let other_val = other.data[i].load(load_ordering);
            (&self.data[i]).fetch_or(other_val, ordering);
        }

        Ok(())
    }


    /// Copy inner raw data to vector. mut on self is used to prevent modification during the operation
    pub(crate) fn to_raw_vec(&mut self, mut ordering: Ordering) -> Vec<u64> {
        if self.bits_count == 0 {
            return Vec::new();
        }

        if ordering == Ordering::AcqRel {
            ordering = Ordering::Acquire;
        }

        let mut result = vec![0; self.data.len()];
        for i in 0..self.data.len() {
            result[i] = (&self.data[i]).load(ordering);
        }

        result
    }

    pub(crate) fn from_raw_slize(raw_data: &[u64], bits_count: usize) -> Result<Self, AtomicBitVecError> {
        let items_count = Self::items_count(bits_count);
        if items_count > raw_data.len() {
            return Err(AtomicBitVecError::DataLengthLessThanRequired);
        }

        let mut data = Vec::with_capacity(items_count);
        for i in 0..items_count {
            data.push(AtomicU64::new(raw_data[i]));
        }

        Ok(Self {
            data,
            bits_count
        })
    }
}


impl Default for AtomicBitVec {
    fn default() -> Self {
        Self {
            data: Vec::new(),
            bits_count: 0
        }
    }
}


#[cfg(test)]
mod tests {
    use super::AtomicBitVec;
    use std::sync::atomic::*;

    #[test]
    fn test_set_get_works() {
        let bitvec = AtomicBitVec::new(87);
        assert_eq!(87, bitvec.len());

        for i in 0..bitvec.len() {
            assert_eq!(false, bitvec.get(i, Ordering::Acquire));
        }

        bitvec.set(1, true, Ordering::Release);
        bitvec.set(5, true, Ordering::Release);

        assert_eq!(false, bitvec.get(0, Ordering::Acquire));
        assert_eq!(true, bitvec.get(1, Ordering::Acquire));
        assert_eq!(false, bitvec.get(4, Ordering::Acquire));
        assert_eq!(true, bitvec.get(5, Ordering::Acquire));

        bitvec.set(86, true, Ordering::Release);
        bitvec.set(1, false, Ordering::Release);

        assert_eq!(false, bitvec.get(1, Ordering::Acquire));
        assert_eq!(true, bitvec.get(86, Ordering::Acquire));
    }

    #[test]
    fn test_len_and_size_in_mem() {
        let bitvec = AtomicBitVec::new(0);
        assert_eq!(0, bitvec.len());
        assert_eq!(0, bitvec.size_in_mem());

        let bitvec = AtomicBitVec::new(16);
        assert_eq!(16, bitvec.len());
        assert_eq!(8, bitvec.size_in_mem());

        let bitvec = AtomicBitVec::new(64);
        assert_eq!(64, bitvec.len());
        assert_eq!(8, bitvec.size_in_mem());

        let bitvec = AtomicBitVec::new(65);
        assert_eq!(65, bitvec.len());
        assert_eq!(16, bitvec.size_in_mem());
    }
}