// Service-owned: a small, immutable LogQL query builder with safe escaping.

import { ValidationError } from "./core/index.js";
import { LABEL_NAME_PATTERN } from "./labels.js";

/** Selector / label-filter operators for string values. */
export type MatchOperator = "=" | "!=" | "=~" | "!~";
/** Numeric label-filter operators. */
export type NumericOperator = "==" | "!=" | ">" | ">=" | "<" | "<=";

const MATCH_OPS: ReadonlySet<string> = new Set(["=", "!=", "=~", "!~"]);
const NUMERIC_OPS: ReadonlySet<string> = new Set(["==", "!=", ">", ">=", "<", "<="]);

/** One stream-selector matcher. */
interface Matcher {
  name: string;
  op: MatchOperator;
  value: string;
}

/**
 * Quotes a LogQL string literal with Go/JSON escaping (`"`, `\`, newlines, tabs; Unicode stays
 * as is). Verified against Loki 3.7 (`format_query` and round trips).
 */
export function quoteLogQL(value: string): string {
  return JSON.stringify(value);
}

function checkName(name: string, what: string): void {
  if (!LABEL_NAME_PATTERN.test(name)) {
    throw new ValidationError(
      `LogQL: invalid ${what} ${JSON.stringify(name)}: must match [a-zA-Z_][a-zA-Z0-9_]*`,
    );
  }
}

function formatNumber(value: number): string {
  if (!Number.isFinite(value)) throw new ValidationError("LogQL: numbers must be finite");
  return value.toLocaleString("en-US", { useGrouping: false, maximumFractionDigits: 20 });
}

function matchesEmpty(m: Matcher): boolean {
  switch (m.op) {
    case "=":
      return m.value === "";
    case "!=":
      return m.value !== "";
    default: {
      let re: RegExp;
      try {
        re = new RegExp(`^(?:${m.value})$`);
      } catch {
        return false; // let the server report RE2 syntax problems
      }
      return m.op === "=~" ? re.test("") : !re.test("");
    }
  }
}

/**
 * Immutable LogQL builder. Every method returns a new builder.
 *
 * ```ts
 * LogQL.selector({ app: "x" }).lineContains("err").json().where("status", ">=", 500).toString()
 * // {app="x"} |= "err" | json | status >= 500
 * ```
 */
export class LogQL {
  readonly #matchers: readonly Matcher[];
  readonly #stages: readonly string[];

  private constructor(matchers: readonly Matcher[], stages: readonly string[]) {
    this.#matchers = matchers;
    this.#stages = stages;
  }

  /** Starts a query from equality matchers (`{app="x", env="prod"}`). */
  static selector(labels: Readonly<Record<string, string>> = {}): LogQL {
    let q = new LogQL([], []);
    for (const [name, value] of Object.entries(labels)) q = q.label(name, "=", value);
    return q;
  }

  /** Quotes a string literal (see {@link quoteLogQL}). */
  static quote(value: string): string {
    return quoteLogQL(value);
  }

  /** Adds a stream-selector matcher (`=`, `!=`, `=~`, `!~`). */
  label(name: string, op: MatchOperator, value: string): LogQL {
    checkName(name, "label name");
    if (!MATCH_OPS.has(op)) throw new ValidationError(`LogQL: invalid matcher operator ${op}`);
    return new LogQL([...this.#matchers, { name, op, value }], this.#stages);
  }

  #stage(text: string): LogQL {
    return new LogQL(this.#matchers, [...this.#stages, text]);
  }

  /** Line filter `|= "text"` (case-sensitive substring). */
  lineContains(text: string): LogQL {
    return this.#stage(`|= ${quoteLogQL(text)}`);
  }

  /** Line filter `!= "text"`. */
  lineNotContains(text: string): LogQL {
    return this.#stage(`!= ${quoteLogQL(text)}`);
  }

  /** Line filter `|~ "regex"` (RE2; prefix `(?i)` for case-insensitive). */
  lineMatches(regex: string): LogQL {
    return this.#stage(`|~ ${quoteLogQL(regex)}`);
  }

  /** Line filter `!~ "regex"`. */
  lineNotMatches(regex: string): LogQL {
    return this.#stage(`!~ ${quoteLogQL(regex)}`);
  }

  /** Parser stage `| json`. */
  json(): LogQL {
    return this.#stage("| json");
  }

  /** Parser stage `| logfmt`. */
  logfmt(): LogQL {
    return this.#stage("| logfmt");
  }

  /** Parser stage `| unpack` (lines packed by Promtail's `pack` stage). */
  unpack(): LogQL {
    return this.#stage("| unpack");
  }

  /** Parser stage `| regexp "<re>"` with named capture groups. */
  regexp(pattern: string): LogQL {
    return this.#stage(`| regexp ${quoteLogQL(pattern)}`);
  }

  /** Parser stage `| pattern "<pattern>"`. */
  pattern(pattern: string): LogQL {
    return this.#stage(`| pattern ${quoteLogQL(pattern)}`);
  }

  /** Formatting stage `| line_format "<template>"`. */
  lineFormat(template: string): LogQL {
    return this.#stage(`| line_format ${quoteLogQL(template)}`);
  }

  /**
   * Label filter on an extracted label or structured-metadata key. Numbers use the numeric
   * operators (`==`, `!=`, `>`, `>=`, `<`, `<=`); strings use `=`, `!=`, `=~`, `!~`.
   */
  where(field: string, op: MatchOperator | NumericOperator, value: string | number): LogQL {
    checkName(field, "field name");
    if (typeof value === "number") {
      if (!NUMERIC_OPS.has(op)) {
        throw new ValidationError(`LogQL: operator ${op} cannot compare numbers`);
      }
      return this.#stage(`| ${field} ${op} ${formatNumber(value)}`);
    }
    if (!MATCH_OPS.has(op)) {
      throw new ValidationError(`LogQL: operator ${op} cannot compare strings`);
    }
    return this.#stage(`| ${field} ${op} ${quoteLogQL(value)}`);
  }

  /**
   * Renders the query. Throws `ValidationError` when the selector is empty or every matcher
   * also matches the empty string (Loki rejects such selectors).
   */
  toString(): string {
    if (this.#matchers.length === 0) {
      throw new ValidationError("LogQL: a stream selector needs at least one matcher");
    }
    if (this.#matchers.every(matchesEmpty)) {
      throw new ValidationError(
        "LogQL: the selector needs at least one matcher that does not match the empty value",
      );
    }
    const selector = `{${this.#matchers.map((m) => `${m.name}${m.op}${quoteLogQL(m.value)}`).join(", ")}}`;
    return [selector, ...this.#stages].join(" ");
  }

  /** Same as {@link toString}. */
  build(): string {
    return this.toString();
  }
}
