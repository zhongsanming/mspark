//! Pure text/number formatting shared by the exporters.
//!
//! Every function here mirrors a specific msprof Python expression; they are
//! golden-critical and therefore unit-tested rather than only exercised through
//! a profile capture.

/// `str(float)` in Python 3: shortest round-trip repr, but switch to exponent
/// form below `1e-4` and at/above `1e16`, with a two-digit exponent.
pub fn py_float(value: f64) -> String {
    if value == 0.0 {
        return "0.0".to_string();
    }
    let abs = value.abs();
    if !(1e-4..1e16).contains(&abs) {
        let text = format!("{value:e}");
        if let Some((mantissa, exponent)) = text.split_once('e') {
            let (sign, digits) = match exponent.strip_prefix('-') {
                Some(digits) => ("-", digits),
                None => ("+", exponent),
            };
            return format!("{mantissa}e{sign}{digits:0>2}");
        }
        text
    } else {
        format!("{value}")
    }
}

/// `repr(round(v, 3))` in Python 3 (round-half-to-even, shortest repr).
pub fn py_round3(value: f64) -> String {
    format!("{:?}", (value * 1000.0).round_ties_even() / 1000.0)
}

/// `Utils::RoundToDecimalPlaces(v, 6)` written without the
/// `format!("{v:.6}").parse()` round trip.
pub fn round6(value: f64) -> f64 {
    (value * 1e6).round_ties_even() / 1e6
}

/// Round `value` to the 0.1 µs grid with ties-to-even.
pub fn round_to_milli(value: i128) -> i128 {
    let q = value.div_euclid(100);
    let r = value.rem_euclid(100);
    match r.cmp(&50) {
        std::cmp::Ordering::Greater => q + 1,
        std::cmp::Ordering::Less => q,
        std::cmp::Ordering::Equal if q % 2 == 0 => q,
        std::cmp::Ordering::Equal => q + 1,
    }
}

/// `format_high_precision_for_csv(trans_into_local_time(duration_ns))`.
///
/// msprof computes `duration/1000 + local_time_offset` in `Decimal`, where the
/// offset is the *shortest decimal repr* of an `f64` (≈0.1 µs resolution at this
/// magnitude). `duration` is a nanosecond count, so the sum is done in
/// milli-microseconds; the offset is rounded onto that 0.1 µs grid with
/// ties-to-even to match the repr.
pub fn nic_timestamp_column(begin_us: i128, cmr_ns: i128, duration_ns: u64) -> String {
    let offset_milli = begin_us * 1000 - cmr_ns;
    let milli = round_to_milli(offset_milli) * 100 + duration_ns as i128;
    format!("{}.{:03}\t", milli / 1000, milli % 1000)
}

/// Parse a `us` decimal string (`123.456`) into exact integer nanoseconds.
/// Doing this through `f64` loses ~256 ns at 1.8e18 ns.
pub fn parse_us_to_ns(s: &str) -> anyhow::Result<i128> {
    let s = s.trim();
    let (int_part, frac_part) = s.split_once('.').unwrap_or((s, ""));
    if !int_part.chars().all(|c| c.is_ascii_digit())
        || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        anyhow::bail!("not a decimal microseconds value: {s:?}");
    }
    let int: i128 = int_part
        .parse()
        .map_err(|_| anyhow::anyhow!("microseconds out of range: {s:?}"))?;
    // ns = us * 1000, keeping the first three fractional digits (msprof prints
    // six, i.e. picosecond padding, and truncates the rest).
    let mut frac_ns = 0i128;
    for (i, ch) in frac_part.chars().take(3).enumerate() {
        frac_ns += (ch as i128 - '0' as i128) * 10i128.pow(2 - i as u32);
    }
    Ok(int * 1000 + frac_ns)
}

/// Days since the Unix epoch → `(year, month, day)` (Howard Hinnant's algorithm).
pub fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_float_repr() {
        assert_eq!(py_float(0.0), "0.0");
        assert_eq!(py_float(1.0), "1");
        assert_eq!(py_float(0.5), "0.5");
        assert_eq!(py_float(1234.5678), "1234.5678");
        assert_eq!(py_float(1e-5), "1e-05");
        assert_eq!(py_float(1e17), "1e+17");
    }

    #[test]
    fn python_round3_is_half_even() {
        assert_eq!(py_round3(0.0005), "0.0");
        assert_eq!(py_round3(0.0015), "0.002");
        assert_eq!(py_round3(1.2345), "1.234");
        assert_eq!(py_round3(1.2355), "1.236");
    }

    #[test]
    fn milli_grid_is_half_even() {
        assert_eq!(round_to_milli(150), 2);
        assert_eq!(round_to_milli(50), 0);
        assert_eq!(round_to_milli(149), 1);
        assert_eq!(round_to_milli(-50), 0);
        assert_eq!(round_to_milli(-150), -2);
        assert_eq!(round_to_milli(1499), 15);
    }

    #[test]
    fn nic_timestamp_keeps_nanosecond_part() {
        // A zero offset leaves the duration untouched.
        assert_eq!(
            nic_timestamp_column(1_000_000, 1_000_000_000, 1_234),
            "1.234\t"
        );
        // A 150 ns offset is on the 0.1 us grid: 150 -> 200 (ties-to-even on
        // 1.5 tenths rounds to the even 2).
        assert_eq!(
            nic_timestamp_column(1_000_000, 1_000_000_000 - 150, 0),
            "0.200\t"
        );
        // A 50 ns offset rounds to zero (0.5 tenths -> even 0).
        assert_eq!(
            nic_timestamp_column(1_000_000, 1_000_000_000 - 50, 0),
            "0.000\t"
        );
    }

    #[test]
    fn microseconds_parse_without_precision_loss() {
        assert_eq!(parse_us_to_ns("1.234").unwrap(), 1234);
        assert_eq!(
            parse_us_to_ns("18446744073.709551").unwrap(),
            18_446_744_073_709
        );
        assert_eq!(parse_us_to_ns("0").unwrap(), 0);
        assert_eq!(parse_us_to_ns("1.2345").unwrap(), 1234);
        assert!(parse_us_to_ns("1e5").is_err());
    }

    #[test]
    fn civil_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(19_723), (2024, 1, 1));
        assert_eq!(civil_from_days(-1), (1969, 12, 31));
    }

    #[test]
    fn round6_matches_format_parse() {
        let cases = [0.1234567, 1.0 / 3.0, 0.000_000_5, 12.999_999_9];
        for value in cases {
            let expected: f64 = format!("{value:.6}").parse().unwrap();
            assert_eq!(round6(value), expected, "value {value}");
        }
    }
}
