package loki

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Matcher is one label matcher of a stream selector: Name Op "Value" with
// Op one of =, !=, =~, !~.
type Matcher struct {
	Name  string
	Op    string
	Value string
}

// LogQL builds LogQL queries with correct escaping. Start with Selector or
// SelectorMatchers, chain pipeline stages, then call Build (or String):
//
//	q, err := loki.Selector(map[string]string{"app": "x"}).
//		LineContains("err").JSON().Where("status", ">=", 500).Build()
//	// {app="x"} |= "err" | json | status >= 500
//
// A builder is immutable: every method returns a new builder. The first
// invalid input (label name, operator, value) is reported by Build as a
// VALIDATION error.
type LogQL struct {
	matchers []Matcher
	stages   []string
	err      error
}

// Selector starts a query from equality matchers. Map keys are sorted, so the
// output is deterministic; use SelectorMatchers to control the order.
func Selector(labels map[string]string) *LogQL {
	names := make([]string, 0, len(labels))
	for k := range labels {
		names = append(names, k)
	}
	sort.Strings(names)
	ms := make([]Matcher, len(names))
	for i, k := range names {
		ms[i] = Matcher{Name: k, Op: "=", Value: labels[k]}
	}
	return SelectorMatchers(ms...)
}

// SelectorMatchers starts a query from matchers in the given order.
func SelectorMatchers(matchers ...Matcher) *LogQL {
	q := &LogQL{}
	for _, m := range matchers {
		q = q.Match(m.Name, m.Op, m.Value)
	}
	return q
}

func (q *LogQL) with(m *Matcher, stage string, err error) *LogQL {
	out := &LogQL{
		matchers: append([]Matcher(nil), q.matchers...),
		stages:   append([]string(nil), q.stages...),
		err:      q.err,
	}
	if out.err == nil && err != nil {
		out.err = err
	}
	if m != nil {
		out.matchers = append(out.matchers, *m)
	}
	if stage != "" {
		out.stages = append(out.stages, stage)
	}
	return out
}

func logqlErr(reason string) error { return ValidationError("logql", reason) }

// Match adds a label matcher (op =, !=, =~ or !~) to the selector.
func (q *LogQL) Match(name, op, value string) *LogQL {
	switch {
	case !validLabelName(name):
		return q.with(nil, "", logqlErr(fmt.Sprintf("invalid label name %q", name)))
	case op != "=" && op != "!=" && op != "=~" && op != "!~":
		return q.with(nil, "", logqlErr(fmt.Sprintf("invalid matcher operator %q", op)))
	}
	return q.with(&Matcher{Name: name, Op: op, Value: value}, "", nil)
}

func (q *LogQL) line(op, text string) *LogQL { return q.with(nil, op+" "+Quote(text), nil) }

// LineContains adds |= "text" (case-sensitive substring).
func (q *LogQL) LineContains(text string) *LogQL { return q.line("|=", text) }

// LineNotContains adds != "text".
func (q *LogQL) LineNotContains(text string) *LogQL { return q.line("!=", text) }

// LineMatches adds |~ "regex" (RE2).
func (q *LogQL) LineMatches(regex string) *LogQL { return q.line("|~", regex) }

// LineNotMatches adds !~ "regex".
func (q *LogQL) LineNotMatches(regex string) *LogQL { return q.line("!~", regex) }

// JSON adds the | json parser.
func (q *LogQL) JSON() *LogQL { return q.with(nil, "| json", nil) }

// Logfmt adds the | logfmt parser.
func (q *LogQL) Logfmt() *LogQL { return q.with(nil, "| logfmt", nil) }

// Where adds a label filter: | field op value. String values take =, !=, =~,
// !~ or ==; numbers (any Go integer or float) and time.Duration take ==, !=,
// >, >=, <, <=.
func (q *LogQL) Where(field, op string, value any) *LogQL {
	if !validLabelName(field) {
		return q.with(nil, "", logqlErr(fmt.Sprintf("invalid field name %q", field)))
	}
	var rendered string
	switch v := value.(type) {
	case string:
		switch op {
		case "=", "!=", "=~", "!~", "==":
		default:
			return q.with(nil, "", logqlErr(fmt.Sprintf("operator %q cannot compare a string", op)))
		}
		rendered = Quote(v)
	case time.Duration:
		if !numericOp(op) {
			return q.with(nil, "", logqlErr(fmt.Sprintf("invalid comparison operator %q", op)))
		}
		rendered = v.String()
	default:
		f, ok := toFloat(value)
		if !ok || math.IsNaN(f) || math.IsInf(f, 0) {
			return q.with(nil, "", logqlErr(fmt.Sprintf("value %v must be a string, a finite number or a duration", value)))
		}
		if !numericOp(op) {
			return q.with(nil, "", logqlErr(fmt.Sprintf("invalid comparison operator %q", op)))
		}
		rendered = strconv.FormatFloat(f, 'f', -1, 64)
	}
	return q.with(nil, "| "+field+" "+op+" "+rendered, nil)
}

// Pipe appends a raw pipeline stage (for example `line_format "{{.msg}}"`),
// prefixed with "| ". The text is not validated or escaped.
func (q *LogQL) Pipe(stage string) *LogQL { return q.with(nil, "| "+strings.TrimSpace(stage), nil) }

func numericOp(op string) bool {
	switch op {
	case "==", "!=", ">", ">=", "<", "<=":
		return true
	}
	return false
}

func toFloat(v any) (float64, bool) {
	switch x := NormalizeAttrValue(v).(type) {
	case int64:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}

// Build returns the query, or the first VALIDATION error. A selector needs at
// least one matcher that cannot match the empty string (Loki rule), checked
// for = and != matchers with literal values.
func (q *LogQL) Build() (string, error) {
	if q.err != nil {
		return "", q.err
	}
	if len(q.matchers) == 0 {
		return "", logqlErr("a stream selector needs at least one matcher")
	}
	if !hasNonEmptyMatcher(q.matchers) {
		return "", logqlErr("a stream selector needs at least one matcher that does not match the empty value")
	}
	parts := make([]string, len(q.matchers))
	for i, m := range q.matchers {
		parts[i] = m.Name + m.Op + Quote(m.Value)
	}
	out := "{" + strings.Join(parts, ", ") + "}"
	if len(q.stages) > 0 {
		out += " " + strings.Join(q.stages, " ")
	}
	return out, nil
}

func hasNonEmptyMatcher(ms []Matcher) bool {
	for _, m := range ms {
		switch m.Op {
		case "=":
			if m.Value != "" {
				return true
			}
		case "!=":
			if m.Value == "" {
				return true
			}
		case "=~":
			// ".*" style patterns match empty; treat anything else as selective.
			if m.Value != "" && m.Value != ".*" && m.Value != "^.*$" {
				return true
			}
		}
	}
	return false
}

// String returns the query, or "" when Build would fail.
func (q *LogQL) String() string {
	s, _ := q.Build()
	return s
}

// Quote renders s as a LogQL double-quoted string literal with Go/JSON
// escaping (\" \\ \n \t \r and \u escapes for other control characters;
// other Unicode is kept as is).
func Quote(s string) string {
	var b strings.Builder
	b.Grow(len(s) + 2)
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\n':
			b.WriteString(`\n`)
		case '\t':
			b.WriteString(`\t`)
		case '\r':
			b.WriteString(`\r`)
		default:
			if r < 0x20 || r == 0x7f {
				fmt.Fprintf(&b, `\u%04x`, r)
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}
