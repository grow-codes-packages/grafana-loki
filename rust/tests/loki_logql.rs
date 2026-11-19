//! LogQL builder: selectors, line filters, parsers, label filters and escaping.

use std::time::Duration;

use grow_grafana_loki as sdk;
use sdk::core::ErrorCode;
use sdk::{quote, LogQL, LogQLValue, Matcher};

#[test]
fn ticket_example() {
    let q = LogQL::selector([("app", "x")])
        .line_contains("err")
        .json()
        .where_("status", ">=", 500)
        .build()
        .unwrap();
    assert_eq!(q, r#"{app="x"} |= "err" | json | status >= 500"#);
}

#[test]
fn parser_stages_and_label() {
    let q = LogQL::selector([("app", "x")])
        .label("env", "!=", "dev")
        .unpack()
        .regexp(r"(?P<ip>\S+)")
        .pattern("<ip> <_>")
        .line_format("{{.ip}}")
        .build()
        .unwrap();
    assert_eq!(
        q,
        r#"{app="x", env!="dev"} | unpack | regexp "(?P<ip>\\S+)" | pattern "<ip> <_>" | line_format "{{.ip}}""#
    );
}

#[test]
fn selectors_and_stages() {
    let team = String::from("core");
    let q = LogQL::selector([("b", "2"), ("a", "1")]);
    assert_eq!(q.to_string(), r#"{a="1", b="2"}"#);
    let q = LogQL::selector_matchers([
        Matcher::new("svc", "=~", "api|web"),
        Matcher::new("env", "!=", "dev"),
        Matcher::new("x", "!~", "y"),
    ])
    .line_not_contains("debug")
    .line_matches("(?i)timeout")
    .line_not_matches("health")
    .logfmt()
    .where_("level", "=", "error")
    .where_("latency", ">", Duration::from_millis(250))
    .where_("ratio", "<=", 0.5)
    .where_("n", "==", 3u8)
    .where_("user", "=~", String::from("ad.*"))
    .where_("team", "!=", &team)
    .pipe("  line_format \"{{.msg}}\"  ");
    assert_eq!(
        q.build().unwrap(),
        r#"{svc=~"api|web", env!="dev", x!~"y"} != "debug" |~ "(?i)timeout" !~ "health" | logfmt | level = "error" | latency > 250ms | ratio <= 0.5 | n == 3 | user =~ "ad.*" | team != "core" | line_format "{{.msg}}""#
    );
    // immutability: the base builder is unchanged
    let base = LogQL::selector([("a", "1")]);
    let _ = base.json();
    assert_eq!(base.to_string(), r#"{a="1"}"#);
    assert_eq!(
        LogQL::selector([("a", "1")])
            .match_("b", "!=", "")
            .build()
            .unwrap(),
        r#"{a="1", b!=""}"#
    );
}

#[test]
fn escaping() {
    let nasty = "q\"uote \\back\nnew\ttab\r üñï 🚀 \u{1}\u{7f}";
    assert_eq!(
        quote(nasty),
        "\"q\\\"uote \\\\back\\nnew\\ttab\\r üñï 🚀 \\u0001\\u007f\""
    );
    let q = LogQL::selector([("nasty", "v\"al\\ue ü")])
        .line_contains(nasty)
        .build()
        .unwrap();
    assert!(q.starts_with(r#"{nasty="v\"al\\ue ü"} |= "q\"uote"#));
}

#[test]
fn validation_errors() {
    let err = |q: LogQL| {
        let e = q.build().unwrap_err();
        assert_eq!(e.code(), ErrorCode::Validation);
        assert_eq!(q.to_string(), "");
        e.message().to_owned()
    };
    assert!(err(LogQL::default()).contains("at least one matcher"));
    assert!(err(LogQL::selector([("a", "")])).contains("empty value"));
    assert!(err(LogQL::selector_matchers([Matcher::new("a", "=~", ".*")])).contains("empty value"));
    assert!(
        err(LogQL::selector_matchers([Matcher::new("a", "=~", "^.*$")])).contains("empty value")
    );
    assert!(err(LogQL::selector_matchers([Matcher::new("a", "=~", "")])).contains("empty value"));
    assert!(err(LogQL::selector_matchers([Matcher::new("a", "!~", "x")])).contains("empty value"));
    assert!(err(LogQL::selector_matchers([Matcher::new("a", "!=", "x")])).contains("empty value"));
    assert!(err(LogQL::selector([("bad-name", "x")])).contains("invalid label name"));
    assert!(err(LogQL::selector([("a", "1")]).match_("b", "==", "x"))
        .contains("invalid matcher operator"));
    let base = LogQL::selector([("a", "1")]);
    assert!(err(base.where_("bad-field", "=", "x")).contains("invalid field name"));
    assert!(err(base.where_("f", ">", "x")).contains("cannot compare a string"));
    assert!(err(base.where_("f", "=~", 5)).contains("invalid comparison operator"));
    assert!(
        err(base.where_("f", "=", Duration::from_secs(1))).contains("invalid comparison operator")
    );
    assert!(err(base.where_("f", ">", f64::NAN)).contains("finite number"));
    assert!(err(base.where_("f", ">", f64::INFINITY)).contains("finite number"));
    // the first error wins
    let e = err(base.where_("bad-1", "=", "x").where_("f", ">", "y"));
    assert!(e.contains("bad-1"));
    assert_eq!(LogQLValue::from(1.5f32), LogQLValue::Num(1.5));
    assert_eq!(LogQLValue::from(-2i64), LogQLValue::Num(-2.0));
}
