package codes.grow.packages.loki;

import java.time.Instant;

/**
 * One metric sample.
 *
 * @param timestamp sample time
 * @param value sample value ({@code NaN} and infinities are kept)
 */
public record Sample(Instant timestamp, double value) {}
