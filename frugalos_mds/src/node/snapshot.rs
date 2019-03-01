use rand::{Rng, SeedableRng, StdRng};
use std::fmt;
use std::iter;
use std::ops::Range;
use trackable::error::ErrorKindExt;

use {ErrorKind, Result};

/// The length of a seed.
const SEED_LENGTH: usize = 32;

/// This struct represents a threshold of taking a snapshot.
#[derive(Debug)]
pub struct SnapshotThreshold {
    /// The random number generator, which is initialized by a seed.
    rng: StdRng,
    /// The value range which this threshold may take.
    range: Range<usize>,
    /// The current value of this threshold.
    current: usize,
}

impl SnapshotThreshold {
    /// Creates a new `SnapshotThreshold` from the specified seed and range.
    pub fn new(seed: &str, range: Range<usize>) -> Result<Self> {
        if range.start > range.end {
            return Err(ErrorKind::InvalidInput
                .cause(format!(
                    "The start of range({:?}) must be smaller or equal than the end",
                    range
                ))
                .into());
        }
        let mut rng_seed = [0u8; 32];
        fill_rng_seed(&mut rng_seed, seed);
        let mut this = Self {
            rng: SeedableRng::from_seed(rng_seed),
            range,
            current: Default::default(),
        };
        this.refresh();
        Ok(this)
    }
}

impl SnapshotThreshold {
    /// Returns the current threshold value.
    pub fn value(&self) -> usize {
        self.current
    }

    /// Updates the current threshold value randomly within the range.
    pub fn refresh(&mut self) {
        let range: Vec<usize> = (self.range.start..=self.range.end).collect();
        self.current = *self.rng.choose(&range).expect("Never fails");
    }
}

impl fmt::Display for SnapshotThreshold {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "SnapshotThreshold(range = {:?}, current = {})",
            self.range, self.current
        )
    }
}

/// Fills up the specified seed with `src`. `seed` must be `[u8; 32]`.
fn fill_rng_seed(seed: &mut [u8], src: &str) {
    let len = src.len();
    let mut src = src.to_owned();
    if len < SEED_LENGTH {
        // Zero padding is acceptable because we don't need a random seed here.
        src.extend(iter::repeat("0").take(SEED_LENGTH - len));
    }
    seed.copy_from_slice(src.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use trackable::result::TestResult;

    const SEED: &str = "xCZw9dir5QixSgZVzjvve7UXUZbYf2eD";

    #[test]
    fn it_works() -> TestResult {
        let range = Range { start: 0, end: 100 };
        let mut threshold = track!(SnapshotThreshold::new(SEED, range))?;
        assert_eq!(20, threshold.value());
        threshold.refresh();
        assert_eq!(31, threshold.value());
        Ok(())
    }

    #[test]
    fn it_works_with_minimum_range() -> TestResult {
        let range = Range { start: 1, end: 1 };
        let mut threshold = track!(SnapshotThreshold::new(SEED, range))?;
        assert_eq!(1, threshold.value());
        threshold.refresh();
        assert_eq!(1, threshold.value());
        threshold.refresh();
        assert_eq!(1, threshold.value());
        Ok(())
    }

    #[test]
    fn it_rejects_invalid_range() -> TestResult {
        assert!(SnapshotThreshold::new(SEED, Range { start: 3, end: 2 }).is_err());
        Ok(())
    }

    #[test]
    fn fill_rng_seed_works() {
        let mut seed = [0u8; 32];
        let src = "cUz598523AtArTqzxAcYBXTdTaCqi4Wk";
        fill_rng_seed(&mut seed, src);
        assert_eq!(src.as_bytes(), seed);
    }

    #[test]
    fn fill_rng_seed_fills_insufficient_range() {
        let mut seed = [0u8; 32];
        let src = "cUz";
        let len = SEED_LENGTH - src.len();
        let filled = iter::repeat(48).take(len).collect::<Vec<u8>>();
        fill_rng_seed(&mut seed, &src);
        assert_eq!([99, 85, 122], seed[0..3]);
        assert_eq!(filled.as_slice(), &seed[3..]);
    }

    #[test]
    #[should_panic]
    fn fill_rng_seed_panics() {
        let mut seed = [0u8; 31];
        fill_rng_seed(&mut seed, "abc");
    }
}
