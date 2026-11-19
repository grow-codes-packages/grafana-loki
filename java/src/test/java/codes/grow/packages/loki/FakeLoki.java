package codes.grow.packages.loki;

import codes.grow.packages.loki.core.Json;
import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/** A scriptable HTTP server that records raw request bodies and replays recorded fixtures. */
final class FakeLoki implements AutoCloseable {
  /** A scripted response. */
  record Reply(int status, String contentType, byte[] body, Map<String, String> headers) {
    static Reply json(int status, String json) {
      return new Reply(status, "application/json", json.getBytes(StandardCharsets.UTF_8), Map.of());
    }

    static Reply text(int status, String text) {
      return new Reply(
          status, "text/plain; charset=utf-8", text.getBytes(StandardCharsets.UTF_8), Map.of());
    }

    static Reply empty(int status) {
      return new Reply(status, null, new byte[0], Map.of());
    }

    Reply header(String name, String value) {
      Map<String, String> h = new LinkedHashMap<>(headers);
      h.put(name, value);
      return new Reply(status, contentType, body, h);
    }
  }

  /** A recorded request. */
  record Request(
      String method,
      String path,
      Map<String, List<String>> query,
      Map<String, List<String>> headers,
      byte[] body) {
    String header(String name) {
      for (Map.Entry<String, List<String>> e : headers.entrySet()) {
        if (e.getKey().equalsIgnoreCase(name)) {
          return e.getValue().get(0);
        }
      }
      return null;
    }

    String param(String name) {
      List<String> v = query.get(name);
      return v == null ? null : v.get(0);
    }

    String text() {
      return new String(body, StandardCharsets.UTF_8);
    }
  }

  private final HttpServer server;
  private final ExecutorService executor = Executors.newCachedThreadPool();
  private final ConcurrentLinkedQueue<Reply> queue = new ConcurrentLinkedQueue<>();
  private final List<Request> requests = new CopyOnWriteArrayList<>();
  private volatile Reply fallback = Reply.json(200, "{}");

  FakeLoki() {
    try {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    server.createContext("/", this::handle);
    server.setExecutor(executor);
    server.start();
  }

  /** Loads {@code fixtures/<name>.json} and returns its recorded response. */
  static Reply fixture(String name) {
    JsonNode f = load(name);
    JsonNode r = f.get("response");
    String ct = r.path("contentType").asText("");
    JsonNode body = r.get("body");
    byte[] bytes;
    if (body == null || body.isNull()) {
      bytes = new byte[0];
    } else if (body.isTextual()) {
      bytes = body.asText().getBytes(StandardCharsets.UTF_8);
    } else if (body.has("base64")) {
      bytes = Base64.getDecoder().decode(body.get("base64").asText());
    } else {
      bytes = body.toString().getBytes(StandardCharsets.UTF_8);
    }
    return new Reply(r.get("status").asInt(), ct.isEmpty() ? null : ct, bytes, Map.of());
  }

  /** Loads a fixture file. */
  static JsonNode load(String name) {
    try (InputStream in = FakeLoki.class.getResourceAsStream("/fixtures/" + name + ".json")) {
      if (in == null) {
        throw new IllegalArgumentException("missing fixture " + name);
      }
      return Json.tree(in.readAllBytes());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  String url() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  FakeLoki enqueue(Reply r) {
    queue.add(r);
    return this;
  }

  FakeLoki enqueue(String fixture) {
    return enqueue(fixture(fixture));
  }

  FakeLoki fallback(Reply r) {
    fallback = r;
    return this;
  }

  List<Request> requests() {
    return new ArrayList<>(requests);
  }

  Request last() {
    return requests.get(requests.size() - 1);
  }

  private void handle(HttpExchange ex) throws IOException {
    try {
      byte[] body = ex.getRequestBody().readAllBytes();
      Map<String, List<String>> query = new LinkedHashMap<>();
      String raw = ex.getRequestURI().getRawQuery();
      if (raw != null) {
        for (String part : raw.split("&")) {
          int eq = part.indexOf('=');
          String k =
              URLDecoder.decode(eq < 0 ? part : part.substring(0, eq), StandardCharsets.UTF_8);
          String v =
              eq < 0 ? "" : URLDecoder.decode(part.substring(eq + 1), StandardCharsets.UTF_8);
          query.computeIfAbsent(k, x -> new ArrayList<>()).add(v);
        }
      }
      requests.add(
          new Request(
              ex.getRequestMethod(),
              ex.getRequestURI().getRawPath(),
              query,
              Map.copyOf(ex.getRequestHeaders()),
              body));
      Reply r = queue.poll();
      if (r == null) {
        r = fallback;
      }
      if (r.contentType() != null) {
        ex.getResponseHeaders().add("Content-Type", r.contentType());
      }
      r.headers().forEach((k, v) -> ex.getResponseHeaders().add(k, v));
      ex.sendResponseHeaders(r.status(), r.body().length == 0 ? -1 : r.body().length);
      if (r.body().length > 0) {
        try (OutputStream os = ex.getResponseBody()) {
          os.write(r.body());
        }
      }
    } catch (IOException e) {
      // client went away
    } finally {
      ex.close();
    }
  }

  @Override
  public void close() {
    server.stop(0);
    executor.shutdownNow();
  }
}
