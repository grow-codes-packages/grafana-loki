package loki

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestLogQL(t *testing.T) {
	q, err := Selector(map[string]string{"app": "x"}).LineContains("err").JSON().Where("status", ">=", 500).Build()
	if err != nil || q != `{app="x"} |= "err" | json | status >= 500` {
		t.Fatalf("%q %v", q, err)
	}
	q2 := Selector(map[string]string{"service_name": "api", "env": "prod"}).
		Match("pod", "=~", "api-.*").
		LineNotContains("debug").LineMatches(`(?i)time\s?out`).LineNotMatches("health").
		Logfmt().Where("level", "=", "error").Where("latency", ">", 250*time.Millisecond).
		Where("ratio", "<=", 0.25).Where("code", "!=", uint8(3)).Pipe(` line_format "{{.msg}}" `).String()
	want := `{env="prod", service_name="api", pod=~"api-.*"} != "debug" |~ "(?i)time\\s?out" !~ "health" | logfmt | level = "error" | latency > 250ms | ratio <= 0.25 | code != 3 | line_format "{{.msg}}"`
	if q2 != want {
		t.Fatalf("\n got %s\nwant %s", q2, want)
	}
	if got := SelectorMatchers(Matcher{Name: "b", Op: "!=", Value: ""}, Matcher{Name: "a", Op: "=", Value: "1"}).String(); got != `{b!="", a="1"}` {
		t.Fatal(got)
	}
	if got := Quote("q\"b\\n\nt\tr\r\x01\x7fü😀"); got != `"q\"b\\n\nt\tr\r\u0001\u007fü😀"` {
		t.Fatal(got)
	}

	bad := []*LogQL{
		Selector(nil),
		Selector(map[string]string{"bad-name": "x"}),
		Selector(map[string]string{"a": "x"}).Match("b", "==", "x"),
		Selector(map[string]string{"a": ""}),
		SelectorMatchers(Matcher{Name: "a", Op: "=~", Value: ".*"}),
		SelectorMatchers(Matcher{Name: "a", Op: "!~", Value: "x"}),
		Selector(map[string]string{"a": "x"}).Where("bad-field", "=", "v"),
		Selector(map[string]string{"a": "x"}).Where("f", ">", "str"),
		Selector(map[string]string{"a": "x"}).Where("f", "=~", 5),
		Selector(map[string]string{"a": "x"}).Where("f", "~", time.Second),
		Selector(map[string]string{"a": "x"}).Where("f", ">", math.NaN()),
		Selector(map[string]string{"a": "x"}).Where("f", ">", []int{1}),
		// The first error wins even when later stages are fine.
		Selector(map[string]string{"bad-name": "x"}).Where("bad-too", "=", "v").JSON(),
	}
	for i, b := range bad {
		if _, err := b.Build(); !errors.Is(err, ErrValidation) {
			t.Errorf("case %d: %v", i, err)
		}
		if b.String() != "" {
			t.Errorf("case %d: String must be empty on error", i)
		}
	}
	// Builders are immutable.
	base := Selector(map[string]string{"a": "x"})
	_ = base.JSON()
	if base.String() != `{a="x"}` {
		t.Fatal(base.String())
	}
}
