//! Output formatting helpers shared by the CSV/JSON exporters.

/// Append `field` to `out`, quoting it the way msprof does (RFC 4180) when it
/// contains a comma, a quote or a newline. Avoids the per-cell allocation of a
/// `csv_field` returning `String`.
pub fn push_csv_field(out: &mut String, field: &str) {
    if field.contains(',') || field.contains('"') || field.contains('\n') || field.contains('\r') {
        out.push('"');
        for ch in field.chars() {
            if ch == '"' {
                out.push('"');
            }
            out.push(ch);
        }
        out.push('"');
    } else {
        out.push_str(field);
    }
}

pub(crate) fn fmt_metric(value: f64) -> String {
    if !value.is_finite() || value < 0.0 {
        return "0.0".to_string();
    }
    let text = format!("{value:.6}");
    let (int_part, frac_part) = text.split_once('.').unwrap_or((&text, ""));
    let mut digits: Vec<u8> = int_part.bytes().map(|b| b - b'0').collect();
    let frac: Vec<u8> = frac_part.bytes().map(|b| b - b'0').collect();
    let int_len = digits.len();
    digits.extend_from_slice(&frac[..3.min(frac.len())]);
    while digits.len() < int_len + 3 {
        digits.push(0);
    }
    if frac.get(3).copied().unwrap_or(0) >= 5 {
        let mut i = digits.len();
        loop {
            if i == 0 {
                digits.insert(0, 1);
                break;
            }
            i -= 1;
            if digits[i] == 9 {
                digits[i] = 0;
            } else {
                digits[i] += 1;
                break;
            }
        }
    }
    let split = digits.len() - 3;
    let int_str: String = digits[..split]
        .iter()
        .map(|&d| (b'0' + d) as char)
        .collect();
    let mut frac_str: String = digits[split..]
        .iter()
        .map(|&d| (b'0' + d) as char)
        .collect();
    while frac_str.len() > 1 && frac_str.ends_with('0') {
        frac_str.pop();
    }
    format!("{int_str}.{frac_str}")
}

/// `Utils::RoundToDecimalPlaces(v, 3)` (std::round = half away from zero).
pub(crate) fn round3(value: f64) -> f64 {
    (value * 1000.0).round() / 1000.0
}

/// `Utils::DoubleToStr(v, 3)`: fixed 3 decimals, then trim trailing zeros
/// (and a trailing dot).
pub(crate) fn double_to_str(value: f64) -> String {
    let text = format!("{value:.3}");
    let text = text.trim_end_matches('0');
    let text = text.trim_end_matches('.');
    text.to_string()
}
