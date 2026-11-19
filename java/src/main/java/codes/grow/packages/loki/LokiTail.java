package codes.grow.packages.loki;

import codes.grow.packages.loki.core.Json;
import codes.grow.packages.loki.core.LokiException;
import codes.grow.packages.loki.core.Redaction;
import codes.grow.packages.loki.core.UnavailableException;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/**
 * Handle of an open tail stream ({@link LokiClient#tail}). Frames are delivered on the WebSocket's
 * thread; callbacks must not block for long. Exceptions thrown by callbacks are ignored.
 */
public final class LokiTail implements AutoCloseable {
  private final AtomicBoolean stopped = new AtomicBoolean();
  private final AtomicBoolean closing = new AtomicBoolean();
  private final AtomicLong dropped = new AtomicLong();
  private final CompletableFuture<Void> closed = new CompletableFuture<>();
  private volatile WebSocket socket;

  LokiTail() {}

  /** Closes the WebSocket with close code 1000. Idempotent. */
  public void stop() {
    if (stopped.compareAndSet(false, true)) {
      WebSocket ws = socket;
      if (ws != null) {
        closeSocket(ws);
      }
    }
  }

  /** Binds the open socket (closes it at once when {@link #stop()} came first). */
  void attach(WebSocket ws) {
    socket = ws;
    if (stopped.get()) {
      closeSocket(ws);
    }
  }

  private void closeSocket(WebSocket ws) {
    if (!closing.compareAndSet(false, true)) {
      return;
    }
    ws.sendClose(WebSocket.NORMAL_CLOSURE, "")
        .orTimeout(2, TimeUnit.SECONDS)
        .whenComplete(
            (w, err) ->
                CompletableFuture.delayedExecutor(500, TimeUnit.MILLISECONDS)
                    .execute(
                        () -> {
                          ws.abort();
                          closed.complete(null);
                        }));
  }

  /** Same as {@link #stop()}. */
  @Override
  public void close() {
    stop();
  }

  /**
   * Returns a future that completes once the socket is closed (after {@link #stop()} or when the
   * server closes it).
   *
   * @return the future
   */
  public CompletableFuture<Void> closed() {
    return closed;
  }

  /**
   * Returns whether the stream is still open.
   *
   * @return true until stopped or closed by the server
   */
  public boolean isOpen() {
    return !stopped.get() && !closed.isDone();
  }

  /**
   * Returns the number of entries the server reported as dropped.
   *
   * @return dropped entries so far
   */
  public long droppedCount() {
    return dropped.get();
  }

  /** WebSocket listener that decodes frames and reports errors. */
  final class Listener implements WebSocket.Listener {
    private final Consumer<TailFrame> onFrame;
    private final LiveTailOptions options;
    private final UnaryOperator<String> redact;
    private final StringBuilder text = new StringBuilder();

    Listener(Consumer<TailFrame> onFrame, LiveTailOptions options, UnaryOperator<String> redact) {
      this.onFrame = onFrame;
      this.options = options;
      this.redact = redact;
    }

    @Override
    public void onOpen(WebSocket webSocket) {
      attach(webSocket);
      webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
      text.append(data);
      if (last) {
        String frame = text.toString();
        text.setLength(0);
        deliver(frame);
      }
      webSocket.request(1);
      return null;
    }

    private void deliver(String raw) {
      TailFrame frame;
      try {
        frame = Decoders.tailFrame(Json.tree(raw.getBytes(StandardCharsets.UTF_8)));
      } catch (LokiException e) {
        report(new LokiServerException("tail: malformed frame", null, null, null));
        return;
      }
      if (!frame.droppedEntries().isEmpty()) {
        dropped.addAndGet(frame.droppedEntries().size());
        List<DroppedEntry> d = frame.droppedEntries();
        safe(() -> options.onDropped().accept(d));
      }
      if (!frame.streams().isEmpty() && !stopped.get()) {
        safe(() -> onFrame.accept(frame));
      }
    }

    @Override
    public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
      webSocket.request(1);
      return null;
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
      if (!stopped.get() && statusCode != WebSocket.NORMAL_CLOSURE) {
        String why = Redaction.truncate(redact.apply(reason == null ? "" : reason.trim()), 300);
        report(
            new UnavailableException(
                "tail: connection closed (code "
                    + statusCode
                    + ")"
                    + (why.isEmpty() ? "" : ": " + why)));
      }
      stopped.set(true);
      closed.complete(null);
      return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
      if (!stopped.get()) {
        report(
            new UnavailableException(
                "tail: connection error (" + error.getClass().getSimpleName() + ")"));
      }
      stopped.set(true);
      closed.complete(null);
    }

    private void report(LokiException e) {
      safe(() -> options.onError().accept(e));
    }
  }

  private static void safe(Runnable r) {
    try {
      r.run();
    } catch (RuntimeException ignored) {
      // callbacks must not break the stream
    }
  }
}
