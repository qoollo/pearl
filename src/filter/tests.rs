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
            WyHash::new(1, make_secret(2)),
            WyHash::new(2, make_secret(3)),
        ],
    );
    println!("Seeded WyHash");
    test_collision(bloom);
}

fn test_collision<T: SeededHash>(mut bloom: Bloom<T>) {
    let mut res = vec![];
    (0..10).into_iter().for_each(|iter| {
        bloom.clear();
        (1..=100_000).into_iter().for_each(|key| {
            bloom.add(((100_000 * iter + key) as u64).to_be_bytes());
        });
        res.push(
            (1..=1_000_000u64)
                .into_iter()
                .filter(|&key| {
                    matches!(
                        bloom.contains_in_memory(key.to_be_bytes()).unwrap(),
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
