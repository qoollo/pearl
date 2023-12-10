use std::ops::DerefMut;

use crate::{
    filter::wyhash3::{make_secret, WyHash},
    Bloom, FilterResult,
};

use super::{ahash::AHasher, SeededHash};

const SEED1: u128 = 2832714830;
const SEED2: u128 = 6911161266;
const SEED3: u128 = 8589794635;
const SEED4: u128 = 4438787122;

#[test]
fn bloom_ahash_simple() {
    let mut bloom = Bloom::<AHasher>::new_with_hashers(
        super::Config {
            elements: (500_000. * 2_f64.ln()) as usize, // 1_000_000 bits
            hashers_count: 2,
            max_buf_bits_count: 1_000_000,
            buf_increase_step: 1,
            preferred_false_positive_rate: 8.,
        },
        vec![AHasher::new_with_keys(1, 2), AHasher::new_with_keys(2, 3)],
    );
    println!("Simple AHash");
    test_collision(bloom);
}
#[test]
fn bloom_ahash_seeded() {
    let mut bloom = Bloom::<AHasher>::new_with_hashers(
        super::Config {
            elements: (500_000. * 2_f64.ln()) as usize, // 1_000_000 bits
            hashers_count: 2,
            max_buf_bits_count: 1_000_000,
            buf_increase_step: 1,
            preferred_false_positive_rate: 8.,
        },
        vec![
            AHasher::new_with_keys(SEED1, SEED2),
            AHasher::new_with_keys(SEED3, SEED4),
        ],
    );
    println!("Seeded AHash");
    test_collision(bloom);
}

#[test]
fn bloom_wyhash_simple() {
    let mut bloom = Bloom::<WyHash>::new_with_hashers(
        super::Config {
            elements: (500_000. * 2_f64.ln()) as usize, // 1_000_000 bits
            hashers_count: 2,
            max_buf_bits_count: 1_000_000,
            ..Default::default()
        },
        vec![
            WyHash::new(1, make_secret(2)),
            WyHash::new(2, make_secret(3)),
        ],
    );
    println!("Simple WyHash");
    test_collision(bloom);
}
#[test]
fn bloom_wyhash_seeded() {
    let mut bloom = Bloom::<WyHash>::new_with_hashers(
        super::Config {
            elements: (500_000. * 2_f64.ln()) as usize, // 1_000_000 bits
            hashers_count: 2,
            max_buf_bits_count: 1_000_000,
            buf_increase_step: 1,
            preferred_false_positive_rate: 8.,
        },
        vec![
            WyHash::new(SEED1 as u64, make_secret(SEED2 as u64)),
            WyHash::new(SEED3 as u64, make_secret(SEED4 as u64)),
        ],
    );
    println!("Seeded WyHash");
    test_collision(bloom);
}

#[derive(Clone)]
pub struct BoxedHasher(pub Box<dyn SeededHash>);

impl SeededHash for BoxedHasher {
    fn new(seed: u128) -> Self
    where
        Self: Sized,
    {
        todo!()
    }

    fn box_clone(&self) -> Box<dyn SeededHash> {
        todo!()
    }
}

impl Clone for Box<dyn SeededHash> {
    fn clone(&self) -> Self {
        self.box_clone()
    }
}

impl std::hash::Hasher for BoxedHasher {
    fn finish(&self) -> u64 {
        self.0.finish()
    }

    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes);
    }
}

#[test]
fn bloom_boxhash_seeded() {
    let hashers: Vec<BoxedHasher> = vec![
        BoxedHasher(Box::new(AHasher::new_with_keys(SEED3, SEED4))),
        BoxedHasher(Box::new(WyHash::new(
            SEED1 as u64,
            make_secret(SEED2 as u64),
        ))),
    ];
    let mut bloom = Bloom::<BoxedHasher>::new_with_hashers(
        super::Config {
            elements: (500_000. * 2_f64.ln()) as usize, // 1_000_000 bits
            hashers_count: 2,
            max_buf_bits_count: 1_000_000,
            buf_increase_step: 1,
            preferred_false_positive_rate: 8.,
        },
        hashers,
    );
    println!("Ahash + WyHash");
    test_collision(bloom);
}

fn test_collision<T: SeededHash + Clone>(mut bloom: Bloom<T>) {
    let mut res = vec![];
    (0..10).into_iter().for_each(|iter| {
        bloom.clear();
        (1..=100_000).into_iter().for_each(|key| {
            bloom.add(((100_000 * iter + key) as u64).to_le_bytes());
        });
        res.push(
            (1..=1_000_000u64)
                .into_iter()
                .filter(|&key| {
                    matches!(
                        bloom.contains_in_memory(key.to_le_bytes()).unwrap(),
                        FilterResult::NeedAdditionalCheck
                    ) && (key <= 100_000 * iter || key > 100_000 * (iter + 1))
                })
                .collect::<Vec<_>>()
                .len(),
        );
        println!(
            "[{}]-[{}] Collisions: {}",
            100_000 * iter + 1,
            100_000 * (iter + 1),
            res.last().unwrap()
        );
    });
    println!(
        "mean: {}",
        res.iter().sum::<usize>() as f64 / res.len() as f64
    );
}
