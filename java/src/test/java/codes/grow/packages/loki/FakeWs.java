package codes.grow.packages.loki;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** A one-connection WebSocket server for tail tests (RFC 6455, text frames only). */
final class FakeWs implements AutoCloseable {
  /** Server side of an accepted connection. */
  interface Script {
    void run(Conn conn) throws Exception;
  }

  /** An accepted connection. */
  static final class Conn {
    private final InputStream in;
    private final OutputStream out;

    Conn(InputStream in, OutputStream out) {
      this.in = in;
      this.out = out;
    }

    void text(String s) throws IOException {
      frame(0x1, s.getBytes(StandardCharsets.UTF_8), true);
    }

    /** Sends a text message split into two fragments. */
    void fragmented(String s) throws IOException {
      byte[] b = s.getBytes(StandardCharsets.UTF_8);
      int half = b.length / 2;
      frame(0x1, java.util.Arrays.copyOfRange(b, 0, half), false);
      frame(0x0, java.util.Arrays.copyOfRange(b, half, b.length), true);
    }

    void binary(byte[] b) throws IOException {
      frame(0x2, b, true);
    }

    void ping() throws IOException {
      frame(0x9, new byte[] {1, 2}, true);
    }

    void close(int code, String reason) throws IOException {
      byte[] r = reason.getBytes(StandardCharsets.UTF_8);
      byte[] p = new byte[2 + r.length];
      p[0] = (byte) (code >> 8);
      p[1] = (byte) code;
      System.arraycopy(r, 0, p, 2, r.length);
      frame(0x8, p, true);
    }

    private synchronized void frame(int opcode, byte[] payload, boolean fin) throws IOException {
      ByteArrayOutputStream b = new ByteArrayOutputStream();
      b.write((fin ? 0x80 : 0) | opcode);
      if (payload.length < 126) {
        b.write(payload.length);
      } else {
        b.write(126);
        b.write(payload.length >> 8);
        b.write(payload.length);
      }
      b.write(payload);
      out.write(b.toByteArray());
      out.flush();
    }

    /** Reads one client frame; returns its opcode (-1 at end of stream). */
    int read() throws IOException {
      int b0 = in.read();
      if (b0 < 0) {
        return -1;
      }
      int b1 = in.read();
      long len = b1 & 0x7f;
      if (len == 126) {
        len = (in.read() << 8) | in.read();
      } else if (len == 127) {
        len = 0;
        for (int i = 0; i < 8; i++) {
          len = (len << 8) | in.read();
        }
      }
      byte[] mask = in.readNBytes(4);
      in.readNBytes((int) len);
      if (mask.length < 4) {
        return -1;
      }
      return b0 & 0x0f;
    }

    /** Reads frames until a close frame (opcode 8) or end of stream. */
    boolean awaitClose() throws IOException {
      while (true) {
        int op = read();
        if (op == 8) {
          return true;
        }
        if (op < 0) {
          return false;
        }
      }
    }
  }

  private final ServerSocket server;
  private final CompletableFuture<Map<String, String>> handshake = new CompletableFuture<>();
  private final CompletableFuture<String> requestLine = new CompletableFuture<>();
  private final CompletableFuture<Void> done = new CompletableFuture<>();

  /** Accepts the upgrade and runs {@code script}. */
  static FakeWs accept(Script script) throws IOException {
    return new FakeWs(0, null, script);
  }

  /** Rejects the upgrade with an HTTP status and text body. */
  static FakeWs reject(int status, String body) throws IOException {
    return new FakeWs(status, body, null);
  }

  private FakeWs(int status, String body, Script script) throws IOException {
    server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
    Thread t =
        new Thread(
            () -> {
              try (Socket s = server.accept()) {
                serve(s, status, body, script);
                done.complete(null);
              } catch (Exception e) {
                done.completeExceptionally(e);
              }
            },
            "fake-ws");
    t.setDaemon(true);
    t.start();
  }

  private void serve(Socket s, int status, String body, Script script) throws Exception {
    InputStream in = s.getInputStream();
    OutputStream out = s.getOutputStream();
    ByteArrayOutputStream head = new ByteArrayOutputStream();
    int c;
    int state = 0;
    while (state < 4 && (c = in.read()) >= 0) {
      head.write(c);
      state = (c == '\r' && state % 2 == 0) || (c == '\n' && state % 2 == 1) ? state + 1 : 0;
    }
    String[] lines = head.toString(StandardCharsets.ISO_8859_1).split("\r\n");
    requestLine.complete(lines[0]);
    Map<String, String> headers = new LinkedHashMap<>();
    for (int i = 1; i < lines.length; i++) {
      int colon = lines[i].indexOf(':');
      if (colon > 0) {
        headers.put(
            lines[i].substring(0, colon).trim().toLowerCase(Locale.ROOT),
            lines[i].substring(colon + 1).trim());
      }
    }
    handshake.complete(headers);
    if (script == null) {
      byte[] b = body.getBytes(StandardCharsets.UTF_8);
      out.write(
          ("HTTP/1.1 "
                  + status
                  + " Error\r\nContent-Type: text/plain\r\nContent-Length: "
                  + b.length
                  + "\r\nConnection: close\r\n\r\n")
              .getBytes(StandardCharsets.ISO_8859_1));
      out.write(b);
      out.flush();
      return;
    }
    String key = headers.get("sec-websocket-key");
    String accept =
        Base64.getEncoder()
            .encodeToString(
                MessageDigest.getInstance("SHA-1")
                    .digest(
                        (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
                            .getBytes(StandardCharsets.ISO_8859_1)));
    out.write(
        ("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                + "Sec-WebSocket-Accept: "
                + accept
                + "\r\n\r\n")
            .getBytes(StandardCharsets.ISO_8859_1));
    out.flush();
    script.run(new Conn(in, out));
  }

  /** The base URL (http://…) that the client turns into ws://…. */
  String url() {
    return "http://127.0.0.1:" + server.getLocalPort();
  }

  Map<String, String> handshake() throws Exception {
    return handshake.get(5, TimeUnit.SECONDS);
  }

  String requestLine() throws Exception {
    return requestLine.get(5, TimeUnit.SECONDS);
  }

  void awaitDone() throws Exception {
    done.get(10, TimeUnit.SECONDS);
  }

  @Override
  public void close() throws IOException {
    server.close();
  }
}
