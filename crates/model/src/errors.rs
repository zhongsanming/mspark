//! Error context helpers.
//!
//! `serde_json` reports a position but not a file, and the exporters read a dozen
//! small files per device, so a bare `expected ident at line 1 column 2` is
//! untraceable. Every parse in this crate goes through [`json_error`], which
//! names the file and quotes the offending line.

use std::path::Path;

use anyhow::anyhow;

/// Build the error for a failed JSON parse, naming the file and quoting the
/// line the parser stopped on.
///
/// ```text
/// parsing /p/device_5/sample.json: expected ident at line 1 column 2; line 1
/// starts with `x{"l2CacheTaskProfilingEvents":"0xfb"}`
/// ```
pub fn json_error(path: &Path, text: &str, error: &serde_json::Error) -> anyhow::Error {
    anyhow!(
        "parsing {}: {}; {}",
        path.display(),
        error,
        describe_position(text, error.line(), error.column())
    )
}

/// Read a file, naming it in the error when the read fails.
pub fn read_to_string(path: &Path) -> anyhow::Result<String> {
    std::fs::read_to_string(path)
        .map_err(|error| anyhow!("reading {}: {}", path.display(), io_reason(&error, path)))
}

/// `std::fs::read`, with the path in the message.
pub fn read(path: &Path) -> anyhow::Result<Vec<u8>> {
    std::fs::read(path)
        .map_err(|error| anyhow!("reading {}: {}", path.display(), io_reason(&error, path)))
}

fn io_reason(error: &std::io::Error, path: &Path) -> String {
    match error.kind() {
        std::io::ErrorKind::NotFound => format!("no such file ({error})"),
        std::io::ErrorKind::PermissionDenied => format!("permission denied ({error})"),
        _ if path.is_dir() => format!("is a directory ({error})"),
        _ => error.to_string(),
    }
}

/// Quote the line a parse error occurred on, escaped and truncated.
pub fn describe_position(text: &str, line: usize, column: usize) -> String {
    match text.lines().nth(line.saturating_sub(1)) {
        Some(source) => {
            let mut quoted = String::new();
            for ch in source.chars().take(120) {
                match ch {
                    '\t' => quoted.push_str("\\t"),
                    '\r' => quoted.push_str("\\r"),
                    c if c.is_control() => quoted.push_str(&format!("\\x{:02x}", c as u32)),
                    c => quoted.push(c),
                }
            }
            if source.chars().count() > 120 {
                quoted.push('…');
            }
            format!("line {line} column {column} is `{quoted}`")
        }
        // The parser reported a line past the end: say how much text there was.
        None => format!(
            "line {line} column {column} is past the end of {} bytes of input",
            text.len()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_errors_name_the_file_and_quote_the_line() {
        let text = r#"x{"l2CacheTaskProfilingEvents":"0xfb"}"#;
        let error = serde_json::from_str::<serde_json::Value>(text).unwrap_err();
        let message = json_error(Path::new("/p/device_5/sample.json"), text, &error).to_string();
        assert!(message.contains("/p/device_5/sample.json"), "{message}");
        assert!(message.contains("line 1"), "{message}");
        assert!(message.contains("x{\"l2Cache"), "{message}");
    }

    #[test]
    fn quotes_control_characters_instead_of_printing_them() {
        let text = "{\"a\":\u{0}\u{7}}";
        let error = serde_json::from_str::<serde_json::Value>(text).unwrap_err();
        let message = json_error(Path::new("f.json"), text, &error).to_string();
        assert!(message.contains("\\x00"), "{message}");
        assert!(!message.contains('\u{0}'), "{message}");
    }

    #[test]
    fn truncates_long_lines() {
        let text = format!("\"{}", "a".repeat(500));
        let error = serde_json::from_str::<serde_json::Value>(&text).unwrap_err();
        let message = json_error(Path::new("f.json"), &text, &error).to_string();
        assert!(message.contains('…'), "{message}");
        assert!(message.len() < 400, "{message}");
    }

    #[test]
    fn read_errors_name_the_file() {
        let error = read_to_string(Path::new("/nonexistent/mspark/sample.json")).unwrap_err();
        let message = error.to_string();
        assert!(
            message.contains("/nonexistent/mspark/sample.json"),
            "{message}"
        );
        assert!(message.contains("no such file"), "{message}");
    }
}
