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
