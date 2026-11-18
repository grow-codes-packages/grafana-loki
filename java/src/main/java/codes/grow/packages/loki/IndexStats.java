package codes.grow.packages.loki;

/**
 * Approximate index statistics ({@link LokiClient#indexStats}). Read from the TSDB index only, so
 * data still in the ingester counts after the next chunk flush; {@code bytes} is tracked in KB per
 * chunk, so small chunks report 0.
 *
 * @param streams number of streams
 * @param chunks number of chunks
 * @param bytes approximate bytes
 * @param entries number of entries
 */
public record IndexStats(long streams, long chunks, long bytes, long entries) {}
