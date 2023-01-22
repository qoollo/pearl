use std::sync::atomic::*;

/// Bit vector with atomic operations on its bits
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


    /// Reads bit value at 'index' offset with specific ordering
    /// Ordering possible values are [`Ordering::SeqCst`], [`Ordering::Acquire`] and [`Ordering::Relaxed`]
    #[inline(always)]
    pub(crate) fn get_ord(&self, index: usize, ordering: Ordering) -> bool {
        debug_assert!(index < self.bits_count);

        let (offset, mask) = Self::offset_and_mask(index);
        let item = &self.data[offset];
        item.load(ordering) & mask != 0
    }

    /// Reads bit value at 'index' offset with [`Ordering::Acquire`] ordering
    #[inline(always)]
    pub(crate) fn get(&self, index: usize) -> bool {
        self.get_ord(index, Ordering::Acquire)
    }

    /// Writes bit value at 'index' offset with specific ordering
    /// 
    /// `set_ord` takes an [`Ordering`] argument which describes the memory ordering
    /// of this operation. All ordering modes are possible. Note that using
    /// [`Ordering::Acquire`] makes the store part of this operation [`Ordering::Relaxed`], and
    /// using [`Ordering::Release`] makes the load part [`Ordering::Relaxed`].
    #[inline]
    pub(crate) fn set_ord(&self, index: usize, value: bool, ordering: Ordering) -> bool {
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

    /// Writes bit value at `index` offset with [`Ordering::AcqRel`] ordering
    #[inline(always)]
    pub(crate) fn set(&self, index: usize, value: bool) -> bool {
        self.set_ord(index, value, Ordering::AcqRel)
    }

    /// Sets bit value to true at `index` offset with [`Ordering::AcqRel`] ordering
    #[inline(always)]
    pub(crate) fn set_true(&self, index: usize) -> bool {
        self.set_ord(index, true, Ordering::AcqRel)
    }

    /// Merge bits from 'other' into self. Other is mut to prevent data modification during merge
    pub(crate) fn merge_with(&mut self, other: &mut Self) -> Result<(), AtomicBitVecError> {
        if self.bits_count != other.bits_count {
            return Err(AtomicBitVecError::BitsCountMismatch);
        }

        for i in 0..self.data.len() {
            let other_val = other.data[i].load(Ordering::Acquire);
            (&self.data[i]).fetch_or(other_val, Ordering::AcqRel);
        }

        Ok(())
    }


    /// Copy inner raw data to vector. mut on self is used to prevent modification during the operation
    pub(crate) fn to_raw_vec(&mut self) -> Vec<u64> {
        if self.bits_count == 0 {
            return Vec::new();
        }

        let mut result = vec![0; self.data.len()];
        for i in 0..self.data.len() {
            result[i] = (&self.data[i]).load(Ordering::Acquire);
        }

        result
    }

    /// Creates `AtomicBitVec` from raw data
    pub(crate) fn from_raw_slice(raw_data: &[u64], bits_count: usize) -> Result<Self, AtomicBitVecError> {
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

    #[test]
    fn test_set_get_works() {
        let bitvec = AtomicBitVec::new(87);
        assert_eq!(87, bitvec.len());

        for i in 0..bitvec.len() {
            assert_eq!(false, bitvec.get(i));
        }

        bitvec.set(1, true);
        bitvec.set(5, true);

        assert_eq!(false, bitvec.get(0));
        assert_eq!(true, bitvec.get(1));
        assert_eq!(false, bitvec.get(4));
        assert_eq!(true, bitvec.get(5));

        bitvec.set(86, true);
        bitvec.set(1, false);

        assert_eq!(false, bitvec.get(1));
        assert_eq!(true, bitvec.get(86));
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

    #[test]
    fn test_merge_with() {
        let mut bitvec_a = AtomicBitVec::new(111);
        for i in 0..bitvec_a.len() {
            bitvec_a.set(i, i % 5 == 0);
        }

        let mut bitvec_b = AtomicBitVec::new(111);
        for i in 0..bitvec_b.len() {
            bitvec_b.set(i, i % 3 == 0);
        }

        bitvec_a.merge_with(&mut bitvec_b).expect("merge success");

        for i in 0..bitvec_a.len() {
            assert_eq!((i % 3 == 0) || (i % 5) == 0, bitvec_a.get(i));
        }
    }

    #[test]
    fn test_merge_with_mismatch_size() {
        let mut bitvec_a = AtomicBitVec::new(111);
        let mut bitvec_b = AtomicBitVec::new(112);

        bitvec_a.merge_with(&mut bitvec_b).expect_err("merge error");
    }

    #[test]
    fn test_to_raw_from_raw() {
        let mut bitvec_a = AtomicBitVec::new(1111);
        for i in 0..bitvec_a.len() {
            bitvec_a.set(i, i % 5 == 0);
        }
        
        let raw_data = bitvec_a.to_raw_vec();
        let bitvec_b = AtomicBitVec::from_raw_slice(&raw_data, bitvec_a.len()).expect("from_raw_slice success");

        assert_eq!(bitvec_a.len(), bitvec_b.len());

        for i in 0..bitvec_a.len() {
            assert_eq!(bitvec_a.get(i), bitvec_b.get(i));
        }
    }

    #[test]
    fn test_backward_compat() {
        let bits_count = 1111;
        let raw_vec: Vec<u64> = vec![1190112520884487201, 2380365779257329730, 4760450083537948804, 9520900168149639432, 595056260442243600, 1190112520884495393, 3533146546375821378, 4760450083537948804, 9520900167075897608, 595056260442243600, 1190112520951596065, 2380225041768974402, 4760450083537949316, 9592957761113825544, 595056260442243600, 1190113070640301089, 2380225041768974402, 4329604];

        let bitvec_a = AtomicBitVec::from_raw_slice(&raw_vec, bits_count).expect("success");
        assert_eq!(bits_count, bitvec_a.len());
        for i in 0..bitvec_a.len() {
            assert_eq!(((i % 5) == 0) || ((i % 111) == 0), bitvec_a.get(i));
        }

        let mut bitvec_b = AtomicBitVec::new(bits_count);
        for i in 0..bitvec_b.len() {
            bitvec_b.set(i, ((i % 5) == 0) || ((i % 111) == 0));
        }

        let bitvec_b_raw = bitvec_b.to_raw_vec();
        assert_eq!(raw_vec, bitvec_b_raw);
    }
}