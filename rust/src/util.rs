//! Internal helpers: Unix-nanosecond conversion, Loki error mapping and wire formatting.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::core::redact::truncate;
use crate::core::transport::MAX_VENDOR_MESSAGE;
use crate::core::{Error, ErrorCode, HttpTransport};

/// Loki's text when the WAL disk is full or the ingester is stopping; mapped to a retryable
/// `UNAVAILABLE` error.
pub(crate) const INGESTER_SHUTTING_DOWN: &str = "Ingester is shutting down";

/// Unix nanoseconds of `t` (negative before the epoch, saturating at the `i64` range).
pub(crate) fn to_unix_nanos(t: SystemTime) -> i64 {
    match t.duration_since(UNIX_EPOCH) {
        Ok(d) => i64::try_from(d.as_nanos()).unwrap_or(i64::MAX),
        Err(e) => i64::try_from(e.duration().as_nanos())
            .map(|n| -n)
            .unwrap_or(i64::MIN),
    }
}

/// The time `ns` nanoseconds after the Unix epoch (negative = before).
pub(crate) fn from_unix_nanos(ns: i64) -> SystemTime {
    if ns >= 0 {
        UNIX_EPOCH + Duration::from_nanos(ns as u64)
    } else {
        UNIX_EPOCH - Duration::from_nanos(ns.unsigned_abs())
    }
}

/// Decimal Unix nanoseconds (Loki's wire form for timestamps).
pub(crate) fn format_nanos(t: SystemTime) -> String {
    to_unix_nanos(t).to_string()
}

/// Renders `d` in a form both Prometheus and Go parsers accept: whole seconds as `<n>s`,
/// otherwise whole milliseconds as `<n>ms` (at least `1ms`).
pub(crate) fn format_duration(d: Duration) -> String {
    if d.subsec_nanos() == 0 {
        format!("{}s", d.as_secs())
    } else {
        format!("{}ms", d.as_millis().max(1))
    }
}

/// Applies Loki-specific error mappings: "Ingester is shutting down" (WAL disk at least 90 %
/// full, or ingester stopping) becomes a retryable `UNAVAILABLE`.
pub(crate) fn map_loki_error(err: Error) -> Error {
    if !err.message().contains(INGESTER_SHUTTING_DOWN) {
        return err;
    }
    let mut message = err.message().to_owned();
    if !message.contains("WAL") {
        message.push_str(" (the Loki ingester is stopping or its WAL disk is at least 90 % full)");
    }
    let mut out = Error::new(ErrorCode::Unavailable, message).with_retryable(true);
    if let Some(s) = err.http_status() {
        out = out.with_status(s);
    }
    if let Some(id) = err.request_id() {
        out = out.with_request_id(id);
    }
    if let Some(ra) = err.retry_after() {
        out = out.with_retry_after(ra);
    }
    out
}

/// First non-empty line of a response body, trimmed.
pub(crate) fn first_line(body: &[u8]) -> String {
    String::from_utf8_lossy(body)
        .trim()
        .lines()
        .next()
        .unwrap_or("")
        .trim()
        .to_owned()
}

/// Builds the SDK error for an HTTP status the transport returned as a plain response (same
/// message shape as the transport: `<op>: <reason>: <vendor text> (HTTP <status>)`).
pub(crate) fn status_error(
    transport: &HttpTransport,
    operation: &str,
    status: u16,
    body: &[u8],
    request_id: Option<&str>,
) -> Error {
    let code = ErrorCode::from_http_status(status).unwrap_or(ErrorCode::Unavailable);
    let text = first_line(body);
    let reason = if text.is_empty() || text.starts_with('<') {
        code.reason().to_owned()
    } else {
        format!(
            "{}: {}",
            code.reason(),
            truncate(&transport.redact(&text), MAX_VENDOR_MESSAGE)
        )
    };
    let err = Error::from_status(status, format!("{operation}: {reason} (HTTP {status})"));
    let err = match request_id {
        Some(id) => err.with_request_id(id),
        None => err,
    };
    map_loki_error(err)
}

/// Go `strconv.Quote`-style double-quoted literal (used for label-set strings).
pub(crate) fn go_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\x07' => out.push_str("\\a"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x0b' => out.push_str("\\v"),
            c if (c as u32) < 0x80 && (c.is_control()) => {
                out.push_str(&format!("\\x{:02x}", c as u32));
            }
            c if c.is_control() || is_go_unprintable(c) => {
                let n = c as u32;
                if n < 0x10000 {
                    out.push_str(&format!("\\u{n:04x}"));
                } else {
                    out.push_str(&format!("\\U{n:08x}"));
                }
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Spaces and format characters Go's `unicode.IsPrint` rejects (besides control characters).
fn is_go_unprintable(c: char) -> bool {
    matches!(c as u32,
        0x00A0 | 0x00AD | 0x1680 | 0x2000..=0x200F | 0x2028..=0x202F | 0x205F..=0x206F
        | 0x3000 | 0xFEFF | 0xE0000..=0xE007F)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nanos_round_trip() {
        let t = UNIX_EPOCH + Duration::from_nanos(1_795_033_788_144_000_001);
        assert_eq!(to_unix_nanos(t), 1_795_033_788_144_000_001);
        assert_eq!(from_unix_nanos(1_795_033_788_144_000_001), t);
        let before = UNIX_EPOCH - Duration::from_secs(2);
        assert_eq!(to_unix_nanos(before), -2_000_000_000);
        assert_eq!(from_unix_nanos(-2_000_000_000), before);
        assert_eq!(format_nanos(UNIX_EPOCH), "0");
    }

    #[test]
    fn durations() {
        assert_eq!(format_duration(Duration::from_secs(60)), "60s");
        assert_eq!(format_duration(Duration::from_millis(1500)), "1500ms");
        assert_eq!(format_duration(Duration::from_nanos(5)), "1ms");
    }

    #[test]
    fn quoting() {
        assert_eq!(go_quote("a\"b\\c"), r#""a\"b\\c""#);
        assert_eq!(
            go_quote("\x07\x08\x0c\n\r\t\x0b\x01"),
            r#""\a\b\f\n\r\t\v\x01""#
        );
        assert_eq!(go_quote("\u{85}\u{a0}ü😀"), "\"\\u0085\\u00a0ü😀\"");
        assert_eq!(go_quote("\u{e0001}"), "\"\\U000e0001\"");
    }

    #[test]
    fn ingester_mapping() {
        let e = Error::from_status(500, "push: x: Ingester is shutting down (HTTP 500)")
            .with_request_id("r1")
            .with_retry_after(Duration::from_secs(1));
        let m = map_loki_error(e);
        assert_eq!(m.code(), ErrorCode::Unavailable);
        assert!(m.is_retryable());
        assert_eq!(m.http_status(), Some(500));
        assert_eq!(m.request_id(), Some("r1"));
        assert_eq!(m.retry_after(), Some(Duration::from_secs(1)));
        assert!(m.message().contains("WAL disk"));
        let plain = map_loki_error(Error::validation("other"));
        assert_eq!(plain.code(), ErrorCode::Validation);
        let wal = map_loki_error(Error::unavailable("Ingester is shutting down WAL"));
        assert!(!wal.message().contains("90 %"));
        assert_eq!(first_line(b"  \n first \nsecond"), "first");
    }
}
