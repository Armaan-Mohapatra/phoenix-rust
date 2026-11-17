//! Deterministic numerics.
//!
//! Platform libm `exp`/`ln` may differ in the last ulp between Windows
//! (MSVC CRT) and Linux (glibc). Research results must be bit-reproducible
//! across the operator's Windows machine and this Linux validation host, so
//! only IEEE-754 correctly rounded operations (+, -, *, /, sqrt) are used.

/// exp(x) for x <= 0 using range reduction and a degree-13 Taylor series.
/// Relative error < 1e-14 on [-700, 0]; returns 0.0 below -700.
pub fn exp_neg(x: f64) -> f64 {
    if x > 0.0 || x.is_nan() {
        return f64::NAN;
    }
    if x < -700.0 {
        return 0.0;
    }
    const LN2: f64 = std::f64::consts::LN_2;
    let k = (x / LN2).round();
    let r = x - k * LN2; // |r| <= ln2/2
    let mut term = 1.0;
    let mut sum = 1.0;
    for n in 1..=13 {
        term = term * r / n as f64;
        sum += term;
    }
    let ki = k as i64;
    // 2^k via exponent bits; k in [-1010, 0] here, normal range for k>=-1022.
    let scale = f64::from_bits(((ki + 1023) as u64) << 52);
    sum * scale
}

/// Standard normal CDF, Abramowitz & Stegun 7.1.26 via erf; |error| < 1.5e-7.
pub fn norm_cdf(z: f64) -> f64 {
    if z.is_nan() {
        return f64::NAN;
    }
    let x = z.abs() / std::f64::consts::SQRT_2;
    let t = 1.0 / (1.0 + 0.327_591_1 * x);
    let poly = t
        * (0.254_829_592
            + t * (-0.284_496_736
                + t * (1.421_413_741 + t * (-1.453_152_027 + t * 1.061_405_429))));
    let erf = 1.0 - poly * exp_neg(-x * x);
    if z >= 0.0 {
        0.5 * (1.0 + erf)
    } else {
        0.5 * (1.0 - erf)
    }
}

/// Upper-tail critical value z with P(Z > z) = alpha, by bisection on
/// [`norm_cdf`]. alpha is clamped to [1e-12, 0.5].
pub fn z_upper(alpha: f64) -> f64 {
    let a = alpha.clamp(1e-12, 0.5);
    let (mut lo, mut hi) = (0.0f64, 8.0f64);
    for _ in 0..80 {
        let mid = 0.5 * (lo + hi);
        if 1.0 - norm_cdf(mid) > a {
            lo = mid;
        } else {
            hi = mid;
        }
    }
    0.5 * (lo + hi)
}

/// xorshift64* — small, seeded, deterministic. Not cryptographic.
#[derive(Debug, Clone)]
pub struct Rng(u64);
impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(if seed == 0 {
            0x9e37_79b9_7f4a_7c15
        } else {
            seed
        })
    }
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    /// Uniform in [0, 1).
    pub fn uniform(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    /// Approximately standard normal (Irwin-Hall, 12 uniforms). Arithmetic
    /// only, so bit-identical across platforms.
    pub fn normal(&mut self) -> f64 {
        let mut s = 0.0;
        for _ in 0..12 {
            s += self.uniform();
        }
        s - 6.0
    }
    pub fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next_u64() % n
        }
    }
}

/// Stable 64-bit FNV-1a hash for deterministic per-event decisions.
pub fn fnv1a(parts: &[&str]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for p in parts {
        for b in p.bytes().chain(std::iter::once(0xff)) {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exp_matches_reference_values() {
        for (x, want) in [
            (0.0, 1.0),
            (-1.0, 0.367_879_441_171_442_3),
            (-0.5, 0.606_530_659_712_633_4),
            (-10.0, 4.539_992_976_248_485e-5),
            (-50.0, 1.928_749_847_963_918e-22),
        ] {
            let got = exp_neg(x);
            assert!(((got - want) / want).abs() < 1e-13, "{x}: {got} vs {want}");
        }
        assert_eq!(exp_neg(-1000.0), 0.0);
        assert!(exp_neg(1.0).is_nan());
    }
    #[test]
    fn normal_cdf_and_critical_values() {
        assert!((norm_cdf(0.0) - 0.5).abs() < 1e-9);
        assert!((norm_cdf(1.959_963_985) - 0.975).abs() < 2e-7);
        assert!((norm_cdf(-1.644_853_627) - 0.05).abs() < 2e-7);
        assert!((z_upper(0.05) - 1.644_853_6).abs() < 1e-5);
        assert!((z_upper(0.05 / 20.0) - 2.807_033_8).abs() < 1e-4);
        // Monotone.
        let mut prev = 0.0;
        for i in -60..=60 {
            let v = norm_cdf(i as f64 / 10.0);
            assert!(v >= prev);
            prev = v;
        }
    }
    #[test]
    fn rng_is_seeded_and_reproducible() {
        let (mut a, mut b) = (Rng::new(42), Rng::new(42));
        let xs: Vec<u64> = (0..5).map(|_| a.next_u64()).collect();
        let ys: Vec<u64> = (0..5).map(|_| b.next_u64()).collect();
        assert_eq!(xs, ys);
        let mut c = Rng::new(43);
        assert_ne!(xs[0], c.next_u64());
        let mut r = Rng::new(9);
        let n = 20_000;
        let mean: f64 = (0..n).map(|_| r.normal()).sum::<f64>() / n as f64;
        assert!(mean.abs() < 0.03);
    }
}
