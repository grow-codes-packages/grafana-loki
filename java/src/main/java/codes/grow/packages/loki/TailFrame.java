package codes.grow.packages.loki;

import java.util.List;

/**
 * One frame of the tail WebSocket.
 *
 * @param streams streams with new entries
 * @param droppedEntries entries the server dropped since the last frame
 */
public record TailFrame(List<StreamResult> streams, List<DroppedEntry> droppedEntries) {
  /**
   * Copies the lists.
   *
   * @param streams streams
   * @param droppedEntries dropped entries
   */
  public TailFrame {
    streams = List.copyOf(streams);
    droppedEntries = List.copyOf(droppedEntries);
  }
}
