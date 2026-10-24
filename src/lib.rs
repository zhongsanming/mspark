//! Library half of `mspark`.
//!
//! The binary owns the CLI and the artifact exporters; everything here is pure
//! and unit-tested, so the golden-critical formatting and comparison rules can
//! be checked without a profile capture.

pub mod text;
pub mod verify;
