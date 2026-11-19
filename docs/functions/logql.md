# LogQL builder

`LogQL` builds queries without string concatenation: every literal is quoted and escaped the
way Loki expects (Go/JSON escaping for `"`, `\`, newlines, tabs and control characters; Unicode
stays as is), label and field names are validated, and operators are checked. Builders are
immutable: every method returns a new builder. The escaping was verified against the real server
(quotes, backslashes, newlines, tabs, umlauts and emoji round-trip).

| Method | Renders | Notes |
| - | - | - |
| `selector({k: v})` | `{k="v", …}` | sorted by name; at least one matcher that does not match the empty value is required (Loki rejects `{a=~".*"}`) |
| `label(name, op, value)` | adds `name op "value"` to the selector | `=`, `!=`, `=~`, `!~` (Go also `Match`, Rust also `match_`) |
| `lineContains(text)` / `lineNotContains(text)` | `\|= "text"` / `!= "text"` | case-sensitive substring |
| `lineMatches(re)` / `lineNotMatches(re)` | `\|~ "re"` / `!~ "re"` | RE2; `(?i)` for case-insensitive |
| `json()` / `logfmt()` / `unpack()` | `\| json` / `\| logfmt` / `\| unpack` | parsers |
| `regexp(re)` / `pattern(p)` | `\| regexp "re"` / `\| pattern "p"` | parsers with named captures |
| `lineFormat(template)` | `\| line_format "template"` | |
| `where(field, op, value)` | `\| field op value` | strings: `=`, `!=`, `=~`, `!~`; numbers: `==`, `!=`, `>`, `>=`, `<`, `<=` (Go and Rust also accept durations) |
| `pipe(stage)` | `\| stage` | raw stage, not escaped (e.g. `drop pod`) |
| `build()` | the query text | `VALIDATION` for an invalid builder (TypeScript also `toString()`) |
| `quote(text)` | `"text"` | the escaping helper on its own |

=== "TypeScript"

    ```ts
    import { LogQL } from "@grow-codes/grafana-loki";

    const q = LogQL.selector({ app: "x" }).lineContains("err").json().where("status", ">=", 500);
    q.build(); // {app="x"} |= "err" | json | status >= 500
    await client.queryRange(q.build(), { limit: 20 });
    ```

=== "Rust"

    ```rust
    use grow_grafana_loki::LogQL;

    let q = LogQL::selector([("app", "x")])
        .line_contains("err")
        .json()
        .where_("status", ">=", 500) // `where` is a Rust keyword
        .build()?;
    assert_eq!(q, r#"{app="x"} |= "err" | json | status >= 500"#);
    ```

=== "Go"

    ```go
    q, err := loki.Selector(map[string]string{"app": "x"}).
        LineContains("err").JSON().Where("status", ">=", 500).Build()
    // q == `{app="x"} |= "err" | json | status >= 500`
    ```

=== "Python"

    ```python
    from grow_grafana_loki import LogQL

    q = LogQL.selector({"app": "x"}).line_contains("err").json().where("status", ">=", 500)
    assert q.build() == '{app="x"} |= "err" | json | status >= 500'
    ```

=== "Java"

    ```java
    String q = LogQL.selector(Map.of("app", "x"))
        .lineContains("err").json().where("status", ">=", 500).build();
    // {app="x"} |= "err" | json | status >= 500
    ```

Use `formatQuery()` to let the server normalize a query, for example in tests that prove a
built query parses.
