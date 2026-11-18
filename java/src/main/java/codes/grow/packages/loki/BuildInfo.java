package codes.grow.packages.loki;

/**
 * Build information of the server ({@code /loki/api/v1/status/buildinfo}).
 *
 * @param version version, e.g. {@code 3.7.10}
 * @param revision git revision
 * @param branch git branch
 * @param buildUser build user
 * @param buildDate build date
 * @param goVersion Go version (may be empty)
 */
public record BuildInfo(
    String version,
    String revision,
    String branch,
    String buildUser,
    String buildDate,
    String goVersion) {}
