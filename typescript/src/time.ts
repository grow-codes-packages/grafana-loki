// Service-owned: time helpers for the Loki HTTP API (nanosecond timestamps).

import { ValidationError } from "./core/index.js";

/**
 * A point in time accepted by the Loki API:
 *
 * - `Date` (millisecond precision),
 * - `bigint` Unix nanoseconds (lossless),
 * - `string`: a decimal Unix-nanosecond string (`"1795034767066713072"`) or an RFC 3339 date.
 */
export type LokiTime = Date | bigint | string;

const NS_PER_MS = 1_000_000n;
const DIGITS = /^\d+$/;

/** Converts a `Date` to a decimal Unix-nanosecond string. */
export function dateToNanos(date: Date): string {
  const ms = date.getTime();
  if (!Number.isFinite(ms)) throw new ValidationError("invalid date");
  return (BigInt(ms) * NS_PER_MS).toString();
}

/** Converts a decimal Unix-nanosecond string (or bigint) to a `Date` (millisecond precision). */
export function nanosToDate(ns: string | bigint): Date {
  const value = typeof ns === "bigint" ? ns : BigInt(ns);
  return new Date(Number(value / NS_PER_MS));
}

/**
 * Converts a {@link LokiTime} to the decimal Unix-nanosecond string Loki expects in push bodies
 * and query parameters. Throws `ValidationError` for an unparsable value.
 */
export function toNanos(value: LokiTime, field = "time"): string {
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) throw new ValidationError(`${field}: invalid date`);
    return dateToNanos(value);
  }
  if (typeof value === "bigint") {
    if (value < 0n) throw new ValidationError(`${field}: must not be negative`);
    return value.toString();
  }
  const text = value.trim();
  if (DIGITS.test(text)) return text;
  const parsed = Date.parse(text);
  if (text === "" || Number.isNaN(parsed)) {
    throw new ValidationError(`${field}: expected a Date, bigint nanoseconds or RFC 3339 text`);
  }
  return dateToNanos(new Date(parsed));
}

/** `toNanos` for optional values. */
export function optionalNanos(value: LokiTime | undefined, field: string): string | undefined {
  return value === undefined ? undefined : toNanos(value, field);
}

/** Converts float Unix seconds (metric samples) to a `Date`. */
export function secondsToDate(seconds: number): Date {
  return new Date(Math.round(seconds * 1000));
}

/** Renders a duration in milliseconds as a Loki/Prometheus duration (`"1500ms"`). */
export function msToDuration(ms: number, field: string): string {
  if (!Number.isFinite(ms) || ms <= 0) {
    throw new ValidationError(`${field}: must be a positive number of milliseconds`);
  }
  return `${Math.round(ms)}ms`;
}
