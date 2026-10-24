//! Golden-file verification: a strict RFC 4180 reader, header-name-addressed
//! column comparison, and the `--strict` gate that lets CI fail on a regression.

use std::path::Path;

use anyhow::{Context as _, Result};

/// Set by `--strict` before dispatch.
static STRICT: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// Enable/disable strict failures (called once from `main`).
pub fn set_strict(enabled: bool) {
    STRICT.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

pub fn strict() -> bool {
    STRICT.load(std::sync::atomic::Ordering::Relaxed)
}

/// Fail the run when `ok` is false and `--strict` was requested.
pub fn gate(ok: bool, what: impl FnOnce() -> String) -> Result<()> {
    if ok || !strict() {
        return Ok(());
    }
    anyhow::bail!("{}", what());
}

/// Compare a produced table against a golden CSV.
///
/// Columns are matched **by header name**, not by position, so a reordered
/// golden file is reported instead of silently compared against the wrong
/// columns. `tolerance` marks columns that only have to be within `eps`
/// (HPFloat residuals).
pub fn compare_table_with(
    golden: &Path,
    header: &[&str],
    produced: &[Vec<String>],
    tolerance: Option<(&[usize], f64)>,
) -> Result<()> {
    let rows = read_rows(golden)?;
    let Some((golden_header, body)) = rows.split_first() else {
        anyhow::bail!("golden file is empty");
    };
    if golden_header.len() != header.len() {
        anyhow::bail!(
            "header width mismatch: golden {} vs mspark {}",
            golden_header.len(),
            header.len()
        );
    }
    // golden column -> produced column, by name.
    let mut order = Vec::with_capacity(header.len());
    for (position, name) in golden_header.iter().enumerate() {
        match header.iter().position(|ours| ours == name) {
            Some(index) => order.push(index),
            None => anyhow::bail!(
                "golden column {position} `{name}` is not produced; mspark header: {header:?}"
            ),
        }
    }

    let (tolerance_cols, eps) = tolerance.unwrap_or((&[], 0.0));
    let n = body.len().min(produced.len());
    let mut exact = vec![0usize; header.len()];
    let mut near = vec![0usize; header.len()];
    for i in 0..n {
        for (column, &target) in order.iter().enumerate() {
            let (Some(g), Some(p)) = (body[i].get(column), produced[i].get(target)) else {
                continue;
            };
            if g == p {
                exact[target] += 1;
                near[target] += 1;
            } else if tolerance_cols.contains(&target)
                && let (Ok(gv), Ok(pv)) = (g.parse::<f64>(), p.parse::<f64>())
                && (gv - pv).abs() <= eps
            {
                near[target] += 1;
            }
        }
    }
    println!("golden rows: {}", body.len());
    println!("produced rows: {}", produced.len());
    let mut ok = body.len() == produced.len();
    for (c, name) in header.iter().enumerate() {
        if tolerance.is_some() {
            println!(
                "{c:>2}  {name:<22} exact {}/{n}  near {}/{n}",
                exact[c], near[c]
            );
            ok &= near[c] == body.len() && produced.len() == body.len();
        } else {
            println!("{c:>2}  {name:<22} {}/{n}", exact[c]);
            ok &= exact[c] == body.len() && produced.len() == body.len();
        }
    }
    gate(ok, || {
        format!("table {golden:?} is not an exact match (see the per-column counts above)")
    })
}

/// Compare a **generated** table (header row + body, as written to disk) against
/// a golden CSV. This is what the artifact verifiers use: they export the table
/// through the same code path as production, so verification cannot drift from
/// the writer.
pub fn compare_generated(
    golden: &Path,
    produced: &[Vec<String>],
    tolerance: Option<(&[usize], f64)>,
) -> Result<()> {
    let Some((header, body)) = produced.split_first() else {
        anyhow::bail!("generated table is empty");
    };
    let header: Vec<&str> = header.iter().map(String::as_str).collect();
    compare_table_with(golden, &header, body, tolerance)
}

pub fn compare_table(golden: &Path, header: &[&str], produced: &[Vec<String>]) -> Result<()> {
    compare_table_with(golden, header, produced, None)
}

pub fn compare_table_relaxed(
    golden: &Path,
    header: &[&str],
    produced: &[Vec<String>],
    tolerance_cols: &[usize],
    tolerance: f64,
) -> Result<()> {
    compare_table_with(golden, header, produced, Some((tolerance_cols, tolerance)))
}

/// Minimal RFC 4180 reader: quoted fields may contain commas, doubled quotes and
/// newlines. Fields are **not** trimmed — msprof emits ` N/A` style values with
/// meaningful leading spaces in some tables, and trimming hid differences.
pub fn read_rows(path: &Path) -> Result<Vec<Vec<String>>> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut rows: Vec<Vec<String>> = Vec::new();
    let mut row: Vec<String> = Vec::new();
    let mut field = String::new();
    let mut in_quotes = false;
    let mut chars = text.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' if in_quotes => {
                if chars.peek() == Some(&'"') {
                    field.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            }
            '"' if field.is_empty() => in_quotes = true,
            ',' if !in_quotes => row.push(std::mem::take(&mut field)),
            '\r' if !in_quotes => {}
            '\n' if !in_quotes => {
                row.push(std::mem::take(&mut field));
                if !(row.len() == 1 && row[0].is_empty()) {
                    rows.push(std::mem::take(&mut row));
                } else {
                    row.clear();
                }
            }
            other => field.push(other),
        }
    }
    if !field.is_empty() || !row.is_empty() {
        row.push(field);
        rows.push(row);
    }
    Ok(rows)
}

/// How close a produced table is to the golden one, when "exact" is not the bar.
///
/// Whole-row equality is too coarse: a single cell that is a nanosecond off makes
/// the row "different", so a table that is 99.9 % right reports 0 %. Instead each
/// **column** is compared as a multiset of values, with numeric cells rounded to
/// `places` decimals. That is order-insensitive (msprof's `ORDER BY` is not always
/// reproducible), tolerates float noise, and still drops measurably when a row or
/// a column breaks.
#[derive(Debug, Clone, Copy)]
pub struct ColumnMatch {
    pub matched: usize,
    pub golden: usize,
    pub produced: usize,
}

impl ColumnMatch {
    /// Fraction of this column's values that matched, relative to the longer side.
    pub fn ratio(&self) -> f64 {
        let denominator = self.golden.max(self.produced);
        if denominator == 0 {
            return 1.0;
        }
        self.matched as f64 / denominator as f64
    }
}

/// Per-column closeness of `produced` against `golden`.
pub fn compare_columns(
    golden_rows: &[Vec<String>],
    produced_rows: &[Vec<String>],
    places: u32,
) -> Vec<ColumnMatch> {
    use std::collections::HashMap;

    let width = golden_rows
        .iter()
        .chain(produced_rows)
        .map(Vec::len)
        .max()
        .unwrap_or(0);
    let mut out = Vec::with_capacity(width);
    for column in 0..width {
        let mut counts: HashMap<String, usize> = HashMap::new();
        for row in golden_rows {
            if let Some(cell) = row.get(column) {
                *counts.entry(normalize(cell, places)).or_default() += 1;
            }
        }
        let mut matched = 0usize;
        for row in produced_rows {
            if let Some(cell) = row.get(column) {
                let key = normalize(cell, places);
                if let Some(count) = counts.get_mut(&key)
                    && *count > 0
                {
                    *count -= 1;
                    matched += 1;
                }
            }
        }
        out.push(ColumnMatch {
            matched,
            golden: golden_rows.len(),
            produced: produced_rows.len(),
        });
    }
    out
}

/// Normalise a cell for tolerance-aware comparison: numeric cells are rounded to
/// `places` decimals, everything else is compared verbatim.
fn normalize(cell: &str, places: u32) -> String {
    match cell.trim().parse::<f64>() {
        Ok(value) if value.is_finite() => format!("{value:.places$}", places = places as usize),
        _ => cell.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(cells: &[&[&str]]) -> Vec<Vec<String>> {
        cells
            .iter()
            .map(|row| row.iter().map(|c| c.to_string()).collect())
            .collect()
    }

    #[test]
    fn order_does_not_matter() {
        let golden = rows(&[&["a", "1.0"], &["b", "2.0"]]);
        let produced = rows(&[&["b", "2.0"], &["a", "1.0"]]);
        let columns = compare_columns(&golden, &produced, 3);
        assert_eq!(columns.len(), 2);
        assert!(columns.iter().all(|column| column.ratio() == 1.0));
    }

    #[test]
    fn numeric_noise_is_tolerated() {
        let golden = rows(&[&["sio", "1.480"]]);
        let produced = rows(&[&["sio", "1.48036"]]);
        assert_eq!(compare_columns(&golden, &produced, 3)[1].matched, 1);
        // At full precision the same pair differs, which is why `places` exists.
        assert_eq!(compare_columns(&golden, &produced, 9)[1].matched, 0);
    }

    #[test]
    fn one_bad_cell_does_not_zero_the_row() {
        let golden = rows(&[&["a", "1", "x"]]);
        let produced = rows(&[&["b", "1", "x"]]);
        let columns = compare_columns(&golden, &produced, 3);
        assert_eq!(columns[0].ratio(), 0.0);
        assert_eq!(columns[1].ratio(), 1.0);
    }

    #[test]
    fn missing_rows_lower_only_the_affected_columns() {
        let golden = rows(&[&["a", "1"], &["b", "2"], &["c", "3"], &["d", "4"]]);
        let produced = rows(&[&["a", "1"], &["b", "2"], &["c", "3"]]);
        let columns = compare_columns(&golden, &produced, 3);
        assert_eq!(columns[0].matched, 3);
        assert!((columns[0].ratio() - 0.75).abs() < 1e-9);
    }

    #[test]
    fn duplicate_values_are_counted_once_each() {
        let golden = rows(&[&["a"], &["a"]]);
        let produced = rows(&[&["a"]]);
        assert_eq!(compare_columns(&golden, &produced, 3)[0].matched, 1);
    }
}
