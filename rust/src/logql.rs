//! LogQL builder with correct escaping.

use std::fmt;
use std::time::Duration;

use crate::core::{Error, Result};
use crate::push::valid_label_name;

/// One label matcher of a stream selector: `name op "value"` with `op` one of `=`, `!=`,
/// `=~`, `!~`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Matcher {
    /// Label name.
    pub name: String,
    /// Operator.
    pub op: String,
    /// Value (a regex for `=~` / `!~`).
    pub value: String,
}

impl Matcher {
    /// A matcher.
    pub fn new(name: impl Into<String>, op: impl Into<String>, value: impl Into<String>) -> Self {
        Matcher {
            name: name.into(),
            op: op.into(),
            value: value.into(),
        }
    }
}

/// A value compared by [`LogQL::where_`]: a string, a finite number or a duration.
#[derive(Debug, Clone, PartialEq)]
pub enum LogQLValue {
    /// String (compared with `=`, `!=`, `=~`, `!~` or `==`).
    Str(String),
    /// Number (compared with `==`, `!=`, `>`, `>=`, `<`, `<=`).
    Num(f64),
    /// Duration, rendered like Go's `time.Duration` (`1m30s`, `250ms`).
    Duration(Duration),
}

impl From<&str> for LogQLValue {
    fn from(v: &str) -> Self {
        LogQLValue::Str(v.to_owned())
    }
}
impl From<String> for LogQLValue {
    fn from(v: String) -> Self {
        LogQLValue::Str(v)
    }
}
impl From<&String> for LogQLValue {
    fn from(v: &String) -> Self {
        LogQLValue::Str(v.clone())
    }
}
impl From<Duration> for LogQLValue {
    fn from(v: Duration) -> Self {
        LogQLValue::Duration(v)
    }
}
macro_rules! num_value {
    ($($t:ty),*) => {$(
        impl From<$t> for LogQLValue {
            fn from(v: $t) -> Self {
                LogQLValue::Num(v as f64)
            }
        }
    )*};
}
num_value!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize, f32, f64);

/// Builds LogQL queries with correct escaping. Start with [`LogQL::selector`] or
/// [`LogQL::selector_matchers`], chain pipeline stages, then call [`LogQL::build`] (or
/// `to_string`):
///
/// ```
/// use grow_grafana_loki::LogQL;
///
/// let q = LogQL::selector([("app", "x")])
///     .line_contains("err")
///     .json()
///     .where_("status", ">=", 500)
///     .build()
///     .unwrap();
/// assert_eq!(q, r#"{app="x"} |= "err" | json | status >= 500"#);
/// ```
///
/// A builder is immutable: every method returns a new builder. The first invalid input (label
/// name, operator, value) is reported by `build` as a `VALIDATION` error.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct LogQL {
    matchers: Vec<Matcher>,
    stages: Vec<String>,
    err: Option<String>,
}

fn logql_err(reason: String) -> Error {
    Error::validation(format!("logql: {reason}"))
}

impl LogQL {
    /// Starts a query from equality matchers. Names are sorted, so the output is
    /// deterministic; use [`LogQL::selector_matchers`] to control the order.
    pub fn selector<I, K, V>(labels: I) -> LogQL
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut pairs: Vec<(String, String)> = labels
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        pairs.sort();
        LogQL::selector_matchers(pairs.into_iter().map(|(k, v)| Matcher::new(k, "=", v)))
    }

    /// Starts a query from matchers in the given order.
    pub fn selector_matchers(matchers: impl IntoIterator<Item = Matcher>) -> LogQL {
        matchers
            .into_iter()
            .fold(LogQL::default(), |q, m| q.match_(&m.name, &m.op, &m.value))
    }

    fn with(&self, matcher: Option<Matcher>, stage: Option<String>, err: Option<String>) -> LogQL {
        let mut out = self.clone();
        if out.err.is_none() {
            out.err = err;
        }
        out.matchers.extend(matcher);
        out.stages.extend(stage);
        out
    }

    /// Adds a label matcher (`op` `=`, `!=`, `=~` or `!~`) to the selector.
    pub fn match_(&self, name: &str, op: &str, value: &str) -> LogQL {
        if !valid_label_name(name) {
            return self.with(None, None, Some(format!("invalid label name {name:?}")));
        }
        if !matches!(op, "=" | "!=" | "=~" | "!~") {
            return self.with(None, None, Some(format!("invalid matcher operator {op:?}")));
        }
        self.with(Some(Matcher::new(name, op, value)), None, None)
    }

    fn line(&self, op: &str, text: &str) -> LogQL {
        self.with(None, Some(format!("{op} {}", quote(text))), None)
    }

    /// Adds `|= "text"` (case-sensitive substring).
    pub fn line_contains(&self, text: &str) -> LogQL {
        self.line("|=", text)
    }

    /// Adds `!= "text"`.
    pub fn line_not_contains(&self, text: &str) -> LogQL {
        self.line("!=", text)
    }

    /// Adds `|~ "regex"` (RE2).
    pub fn line_matches(&self, regex: &str) -> LogQL {
        self.line("|~", regex)
    }

    /// Adds `!~ "regex"`.
    pub fn line_not_matches(&self, regex: &str) -> LogQL {
        self.line("!~", regex)
    }

    /// Adds the `| json` parser.
    pub fn json(&self) -> LogQL {
        self.with(None, Some("| json".into()), None)
    }

    /// Adds the `| logfmt` parser.
    pub fn logfmt(&self) -> LogQL {
        self.with(None, Some("| logfmt".into()), None)
    }

    /// Adds a label filter `| field op value` (`where` in the other languages). Strings take
    /// `=`, `!=`, `=~`, `!~` or `==`; numbers and durations take `==`, `!=`, `>`, `>=`, `<`,
    /// `<=`.
    pub fn where_(&self, field: &str, op: &str, value: impl Into<LogQLValue>) -> LogQL {
        if !valid_label_name(field) {
            return self.with(None, None, Some(format!("invalid field name {field:?}")));
        }
        let rendered = match value.into() {
            LogQLValue::Str(s) => {
                if !matches!(op, "=" | "!=" | "=~" | "!~" | "==") {
                    return self.with(
                        None,
                        None,
                        Some(format!("operator {op:?} cannot compare a string")),
                    );
                }
                quote(&s)
            }
            LogQLValue::Duration(d) => {
                if !numeric_op(op) {
                    return self.with(
                        None,
                        None,
                        Some(format!("invalid comparison operator {op:?}")),
                    );
                }
                go_duration(d)
            }
            LogQLValue::Num(f) => {
                if !f.is_finite() {
                    return self.with(
                        None,
                        None,
                        Some(format!(
                            "value {f} must be a string, a finite number or a duration"
                        )),
                    );
                }
                if !numeric_op(op) {
                    return self.with(
                        None,
                        None,
                        Some(format!("invalid comparison operator {op:?}")),
                    );
                }
                f.to_string()
            }
        };
        self.with(None, Some(format!("| {field} {op} {rendered}")), None)
    }

    /// Appends a raw pipeline stage (for example `line_format "{{.msg}}"`), prefixed with
    /// `| `. The text is not validated or escaped.
    pub fn pipe(&self, stage: &str) -> LogQL {
        self.with(None, Some(format!("| {}", stage.trim())), None)
    }

    /// The query, or the first `VALIDATION` error. A selector needs at least one matcher that
    /// cannot match the empty string (Loki rule).
    pub fn build(&self) -> Result<String> {
        if let Some(e) = &self.err {
            return Err(logql_err(e.clone()));
        }
        if self.matchers.is_empty() {
            return Err(logql_err(
                "a stream selector needs at least one matcher".into(),
            ));
        }
        if !has_non_empty_matcher(&self.matchers) {
            return Err(logql_err(
                "a stream selector needs at least one matcher that does not match the empty value"
                    .into(),
            ));
        }
        let parts: Vec<String> = self
            .matchers
            .iter()
            .map(|m| format!("{}{}{}", m.name, m.op, quote(&m.value)))
            .collect();
        let mut out = format!("{{{}}}", parts.join(", "));
        if !self.stages.is_empty() {
            out.push(' ');
            out.push_str(&self.stages.join(" "));
        }
        Ok(out)
    }
}

/// Renders the query, or an empty string when [`LogQL::build`] would fail.
impl fmt::Display for LogQL {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.build().unwrap_or_default())
    }
}

fn numeric_op(op: &str) -> bool {
    matches!(op, "==" | "!=" | ">" | ">=" | "<" | "<=")
}

fn has_non_empty_matcher(ms: &[Matcher]) -> bool {
    ms.iter().any(|m| match m.op.as_str() {
        "=" => !m.value.is_empty(),
        "!=" => m.value.is_empty(),
        "=~" => !m.value.is_empty() && m.value != ".*" && m.value != "^.*$",
        _ => false,
    })
}

/// Renders `s` as a LogQL double-quoted string literal with Go/JSON escaping (`\"` `\\` `\n`
/// `\t` `\r` and `\u` escapes for other control characters; other Unicode is kept as is).
pub fn quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Formats `d` like Go's `time.Duration.String` (`1h2m3.5s`, `250ms`, `1.5µs`, `0s`).
pub(crate) fn go_duration(d: Duration) -> String {
    let ns = d.as_nanos();
    if ns == 0 {
        return "0s".into();
    }
    fn frac(value: u128, unit: u128) -> String {
        let whole = value / unit;
        let rest = value % unit;
        if rest == 0 {
            return whole.to_string();
        }
        let width = unit.to_string().len() - 1;
        let digits = format!("{rest:0width$}");
        format!("{whole}.{}", digits.trim_end_matches('0'))
    }
    if ns < 1_000 {
        return format!("{ns}ns");
    }
    if ns < 1_000_000 {
        return format!("{}µs", frac(ns, 1_000));
    }
    if ns < 1_000_000_000 {
        return format!("{}ms", frac(ns, 1_000_000));
    }
    let total_secs = ns / 1_000_000_000;
    let sub = ns % 1_000_000_000;
    let (h, m, s) = (total_secs / 3600, (total_secs / 60) % 60, total_secs % 60);
    let secs = frac(s * 1_000_000_000 + sub, 1_000_000_000);
    match (h, m) {
        (0, 0) => format!("{secs}s"),
        (0, m) => format!("{m}m{secs}s"),
        (h, m) => format!("{h}h{m}m{secs}s"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn go_durations() {
        assert_eq!(go_duration(Duration::ZERO), "0s");
        assert_eq!(go_duration(Duration::from_nanos(5)), "5ns");
        assert_eq!(go_duration(Duration::from_nanos(1500)), "1.5µs");
        assert_eq!(go_duration(Duration::from_micros(2)), "2µs");
        assert_eq!(go_duration(Duration::from_millis(250)), "250ms");
        assert_eq!(go_duration(Duration::from_micros(1250)), "1.25ms");
        assert_eq!(go_duration(Duration::from_millis(1500)), "1.5s");
        assert_eq!(go_duration(Duration::from_secs(90)), "1m30s");
        assert_eq!(go_duration(Duration::from_secs(3600)), "1h0m0s");
        assert_eq!(go_duration(Duration::from_millis(3_723_500)), "1h2m3.5s");
    }
}
