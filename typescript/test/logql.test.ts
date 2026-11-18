import { describe, expect, it } from "vitest";
import {
  DEFAULT_ALLOWED_LABELS,
  LogQL,
  Severity,
  attrToString,
  formatLabelSet,
  isValidLabelName,
  normalizeLabelName,
  quoteLogQL,
  recordsToStreams,
  validateLabelName,
  type LogRecord,
} from "../src/index.js";

describe("LogQL builder", () => {
  it("builds the ticket example", () => {
    expect(
      LogQL.selector({ app: "x" }).lineContains("err").json().where("status", ">=", 500).toString(),
    ).toBe('{app="x"} |= "err" | json | status >= 500');
  });

  it("supports every stage and escapes literals", () => {
    const q = LogQL.selector({ app: 'we"ird\\' })
      .label("env", "!=", "dev")
      .label("pod", "=~", "web-.+")
      .label("ns", "!~", "kube-.*")
      .lineContains('a\nb\t"c" ü 🚀')
      .lineNotContains("health")
      .lineMatches("(?i)error")
      .lineNotMatches("debug")
      .logfmt()
      .unpack()
      .regexp("(?P<ip>\\S+)")
      .pattern("<ip> - <_>")
      .lineFormat("{{.msg}}")
      .where("level", "=", "error")
      .where("latency", "<", 0.25)
      .where("count", "==", 1e21)
      .build();
    expect(q).toBe(
      '{app="we\\"ird\\\\", env!="dev", pod=~"web-.+", ns!~"kube-.*"} |= "a\\nb\\t\\"c\\" ü 🚀" != "health" |~ "(?i)error" !~ "debug" | logfmt | unpack | regexp "(?P<ip>\\\\S+)" | pattern "<ip> - <_>" | line_format "{{.msg}}" | level = "error" | latency < 0.25 | count == 1000000000000000000000',
    );
    expect(LogQL.quote("x")).toBe('"x"');
    expect(quoteLogQL('"')).toBe('"\\""');
  });

  it("is immutable", () => {
    const base = LogQL.selector({ a: "b" });
    const child = base.json();
    expect(base.toString()).toBe('{a="b"}');
    expect(child.toString()).toBe('{a="b"} | json');
  });

  it("validates names, operators, numbers and empty-compatible selectors", () => {
    expect(() => LogQL.selector({ "bad-name": "x" })).toThrow(/label name/);
    expect(() => LogQL.selector().label("a", "==" as "=", "x")).toThrow(/operator/);
    expect(() => LogQL.selector({ a: "b" }).where("bad.field", "=", "x")).toThrow(/field name/);
    expect(() => LogQL.selector({ a: "b" }).where("f", "=~", 1)).toThrow(/numbers/);
    expect(() => LogQL.selector({ a: "b" }).where("f", ">", "x")).toThrow(/strings/);
    expect(() => LogQL.selector({ a: "b" }).where("f", ">", Number.NaN)).toThrow(/finite/);
    expect(() => LogQL.selector().toString()).toThrow(/at least one matcher/);
    expect(() => LogQL.selector({ a: "" }).toString()).toThrow(/empty value/);
    expect(() => LogQL.selector().label("a", "=~", ".*").toString()).toThrow(/empty value/);
    expect(() => LogQL.selector().label("a", "!=", "x").toString()).toThrow(/empty value/);
    expect(() => LogQL.selector().label("a", "!~", ".+").toString()).toThrow(/empty value/);
    expect(LogQL.selector().label("a", "!=", "").toString()).toBe('{a!=""}');
    expect(LogQL.selector().label("a", "=~", ".+").toString()).toBe('{a=~".+"}');
    expect(LogQL.selector().label("a", "!~", ".*").toString()).toBe('{a!~".*"}');
    // invalid JS regex: left to the server
    expect(LogQL.selector().label("a", "=~", "(").toString()).toBe('{a=~"("}');
  });
});

describe("labels and the cardinality guard", () => {
  it("validates and normalizes names", () => {
    expect(isValidLabelName("service_name")).toBe(true);
    expect(isValidLabelName("__name__")).toBe(false);
    expect(isValidLabelName("1x")).toBe(false);
    expect(() => validateLabelName("a-b")).toThrow(/must match/);
    expect(normalizeLabelName("service.name")).toBe("service_name");
    expect(normalizeLabelName("1abc")).toBe("_1abc");
    expect(normalizeLabelName("")).toBe("_");
    expect(formatLabelSet({ b: "2", a: 'x"' })).toBe('{a="x\\"", b="2"}');
    expect(attrToString(1)).toBe("1");
    expect(attrToString(true)).toBe("true");
    expect(attrToString({ k: [1] })).toBe('{"k":[1]}');
    expect(DEFAULT_ALLOWED_LABELS).toContain("service_name");
  });

  const base: LogRecord = {
    timestamp: new Date(1_700_000_000_000),
    severity: Severity.Warn,
    severityText: "WARN",
    body: "hello",
    attributes: {
      "http.status": 503,
      user: { id: 1 },
      pod: "web-1",
      host_name: "",
      __reserved: "x",
    },
    resource: { "service.name": "api", "deployment.environment": "prod", "service.version": "1.2" },
    traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
    spanId: "00f067aa0ba902b7",
    traceFlags: 1,
  };

  it("puts allowed keys into labels and everything else into metadata", () => {
    const streams = recordsToStreams([base, { ...base, body: "second", severityText: "" }]);
    expect(streams).toHaveLength(1);
    const s = streams[0]!;
    expect(s.labels).toEqual({
      service_name: "api",
      deployment_environment: "prod",
      pod: "web-1",
      level: "warn",
    });
    expect(s.entries[0]).toEqual({
      timestamp: "1700000000000000000",
      line: "hello",
      metadata: {
        service_version: "1.2",
        http_status: "503",
        user: '{"id":1}',
        host_name: "",
        __reserved: "x",
        severity_text: "WARN",
        severity_number: "13",
        trace_id: "4bf92f3577b34da6a3ce929d0e0e4736",
        span_id: "00f067aa0ba902b7",
        trace_flags: "1",
      },
    });
    expect(s.entries[1]!.metadata!.severity_text).toBe("WARN");
  });

  it("honours a custom allow list and groups by label set", () => {
    const streams = recordsToStreams(
      [
        {
          ...base,
          traceId: undefined,
          spanId: undefined,
          traceFlags: undefined,
        } as unknown as LogRecord,
        {
          ...base,
          resource: { "service.name": "other" },
          spanId: undefined,
        } as unknown as LogRecord,
      ],
      { allowedLabels: ["service_name", "__reserved"] },
    );
    expect(streams.map((s) => s.labels)).toEqual([
      { service_name: "api" },
      { service_name: "other" },
    ]);
    expect(streams[0]!.entries[0]!.metadata).not.toHaveProperty("trace_id");
    expect(streams[0]!.entries[0]!.metadata).not.toHaveProperty("level");
    expect(streams[0]!.labels).not.toHaveProperty("__reserved");
    expect(streams[1]!.entries[0]!.metadata).toMatchObject({
      trace_id: base.traceId,
      trace_flags: "1",
    });
    expect(streams[1]!.entries[0]!.metadata).not.toHaveProperty("span_id");
  });
});
