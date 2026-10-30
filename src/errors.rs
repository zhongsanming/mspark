//! Top-level error reporting.
//!
//! `fn main() -> Result<()>` prints anyhow's `Debug` output, which is already a
//! chain, but it gives the reader no next step. This prints the same chain plus a
//! hint, and keeps the first line short enough to be a useful one-line summary.

use anyhow::Error;

/// Render an error report: the context chain, then how to get more detail.
///
/// Context is added as the error propagates, so an unhelpful one-liner usually
/// means a `?` somewhere is converting without naming its file — see
/// `mspark_model::errors::json_error`.
pub fn report(error: &Error) -> String {
    let mut text = format!("{error:?}");
    if std::env::var_os("RUST_BACKTRACE").is_none() {
        text.push_str(
            "\n\nnote: run with `RUST_BACKTRACE=1` to include a backtrace, \
             or `RUST_BACKTRACE=full` for frames without inlining",
        );
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context as _;

    #[test]
    fn prints_the_whole_chain() {
        let error = Err::<(), _>(anyhow::anyhow!("expected ident at line 1 column 2"))
            .context("parsing /p/sample.json")
            .context("computing `api_event`")
            .context("on device 5 (/p/device_5)")
            .context("exporting `api_statistic`")
            .unwrap_err();
        let text = report(&error);
        assert!(text.starts_with("exporting `api_statistic`"), "{text}");
        for expected in [
            "on device 5 (/p/device_5)",
            "computing `api_event`",
            "parsing /p/sample.json",
            "expected ident at line 1 column 2",
        ] {
            assert!(text.contains(expected), "missing {expected:?} in:\n{text}");
        }
    }
}
