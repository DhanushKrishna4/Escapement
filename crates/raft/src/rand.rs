//! A deterministic, integer-only PRNG.
//!
//! WHY THIS EXISTS: `rand::thread_rng` seeds from the OS, so two runs of the
//! same seed would diverge and the entire premise of the project (seed => an
//! exactly replayable run) would be lost. This generator is pure integer
//! arithmetic with defined wrapping, so it produces identical streams on every
//! platform, including wasm32.
//!
//! Algorithm: SplitMix64 (Steele et al., "Fast Splittable Pseudorandom Number
//! Generators"). Chosen because it is a single multiply-xor-shift chain with no
//! state array, which makes it trivially auditable and cheap enough that the
//! simulator can afford millions of draws per second.

/// Fixed odd increment from the SplitMix64 reference implementation.
const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng { state: seed }
    }

    /// Derive an independent stream from a parent seed and a stream label.
    ///
    /// Used to give each node its own election-timeout stream while keeping the
    /// whole run reproducible from one top-level seed. Mixing the label through
    /// SplitMix's finalizer (rather than, say, `seed + id`) keeps neighbouring
    /// labels from producing correlated streams.
    pub fn derive(seed: u64, label: u64) -> Self {
        Rng::new(mix(seed ^ label.wrapping_mul(GAMMA)))
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(GAMMA);
        mix(self.state)
    }

    /// Uniform integer in `[lo, hi)`. Panics if `hi <= lo`.
    ///
    /// Uses Lemire's multiply-shift instead of `%` to avoid modulo bias, and
    /// because a 128-bit widening multiply is exactly defined on every target
    /// (unlike float scaling, which is not portable).
    pub fn gen_range(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(hi > lo, "gen_range requires a non-empty range");
        let span = hi - lo;
        let product = (self.next_u64() as u128) * (span as u128);
        lo + (product >> 64) as u64
    }

    /// True with probability `numerator / denominator`.
    ///
    /// Integer-only on purpose: floating point is deterministic within a
    /// platform but not guaranteed across platforms, and fault probabilities
    /// feed directly into behaviour.
    pub fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        assert!(denominator > 0, "chance requires a positive denominator");
        self.gen_range(0, denominator) < numerator
    }
}

/// SplitMix64 finalizer.
fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_stream() {
        let mut a = Rng::new(42);
        let mut b = Rng::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = Rng::new(1);
        let mut b = Rng::new(2);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn derived_streams_are_independent() {
        let mut a = Rng::derive(7, 0);
        let mut b = Rng::derive(7, 1);
        let xs: Vec<u64> = (0..8).map(|_| a.next_u64()).collect();
        let ys: Vec<u64> = (0..8).map(|_| b.next_u64()).collect();
        assert_ne!(xs, ys);
    }

    #[test]
    fn gen_range_stays_in_bounds() {
        let mut r = Rng::new(0xDEAD_BEEF);
        for _ in 0..10_000 {
            let v = r.gen_range(150, 300);
            assert!((150..300).contains(&v));
        }
    }

    /// Golden values. If this test ever fails, the PRNG changed and every
    /// recorded seed in the repo now means something different.
    #[test]
    fn stream_is_pinned() {
        let mut r = Rng::new(0);
        let got: Vec<u64> = (0..4).map(|_| r.next_u64()).collect();
        assert_eq!(
            got,
            vec![
                0xE220A8397B1DCDAF,
                0x6E789E6AA1B965F4,
                0x06C45D188009454F,
                0xF88BB8A8724C81EC,
            ]
        );
    }
}
