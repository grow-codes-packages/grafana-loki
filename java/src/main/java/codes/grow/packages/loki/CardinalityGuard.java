package codes.grow.packages.loki;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Keeps Loki's index small: only allowlisted names become index labels, everything else is sent as
 * structured metadata (per entry, not indexed).
 */
public final class CardinalityGuard {
  private final TreeSet<String> allowed;

  /**
   * Creates a guard.
   *
   * @param allowed label names that may become index labels
   */
  public CardinalityGuard(Collection<String> allowed) {
    this.allowed = new TreeSet<>(Objects.requireNonNull(allowed, "allowed"));
  }

  /**
   * Returns a guard with {@link Labels#DEFAULT_ALLOWED_LABELS}.
   *
   * @return the default guard
   */
  public static CardinalityGuard defaults() {
    return new CardinalityGuard(Labels.DEFAULT_ALLOWED_LABELS);
  }

  /**
   * Returns the allowlist, sorted.
   *
   * @return immutable list
   */
  public List<String> allowed() {
    return List.copyOf(allowed);
  }

  /**
   * Returns whether a name may be an index label.
   *
   * @param name label name
   * @return true when allowlisted
   */
  public boolean isAllowed(String name) {
    return allowed.contains(name);
  }

  /**
   * Returns a copy of the stream whose labels outside the allowlist are moved to the structured
   * metadata of every entry (an entry's own metadata wins).
   *
   * @param stream stream
   * @return the guarded stream
   */
  public LokiStream apply(LokiStream stream) {
    Map<String, String> labels = new LinkedHashMap<>();
    Map<String, String> moved = new LinkedHashMap<>();
    stream
        .labels()
        .forEach(
            (k, v) -> {
              if (allowed.contains(k)) {
                labels.put(k, v);
              } else {
                moved.put(k, v);
              }
            });
    if (moved.isEmpty()) {
      return stream;
    }
    List<LokiEntry> entries = new ArrayList<>(stream.entries().size());
    for (LokiEntry e : stream.entries()) {
      Map<String, String> md = new LinkedHashMap<>(moved);
      md.putAll(e.structuredMetadata());
      entries.add(new LokiEntry(e.timestamp(), e.line(), md));
    }
    return new LokiStream(labels, entries);
  }

  /**
   * Splits attributes into index labels (allowlisted, non-empty scalar text) and structured
   * metadata. Keys are normalized with {@link Labels#normalizeLabelName}; values are rendered with
   * {@link Labels#attrToString}. Keys are processed in sorted order, so the result is deterministic
   * when two keys normalize to the same name.
   *
   * @param attributes attributes
   * @param labels receives index labels
   * @param metadata receives structured metadata
   */
  public void split(
      Map<String, ?> attributes, Map<String, String> labels, Map<String, String> metadata) {
    for (Map.Entry<String, ?> e : new TreeMap<String, Object>(attributes).entrySet()) {
      String name = Labels.normalizeLabelName(e.getKey());
      if (name.isEmpty()) {
        continue;
      }
      String value = Labels.attrToString(e.getValue());
      if (allowed.contains(name) && !value.isEmpty() && !name.startsWith("__")) {
        labels.put(name, value);
      } else {
        metadata.put(name, value);
      }
    }
  }

  @Override
  public String toString() {
    return "CardinalityGuard" + allowed;
  }
}
