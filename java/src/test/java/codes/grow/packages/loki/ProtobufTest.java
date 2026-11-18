package codes.grow.packages.loki;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProtobufTest {
  /** Minimal reader for the test: decodes a PushRequest into "labels|s.ns|line|k=v,..." rows. */
  static List<String> decodePush(byte[] b) {
    List<String> rows = new ArrayList<>();
    Reader r = new Reader(b);
    while (r.more()) {
      r.tag();
      Reader stream = new Reader(r.bytes());
      String labels = null;
      while (stream.more()) {
        int field = stream.tag();
        if (field == 1) {
          labels = new String(stream.bytes(), StandardCharsets.UTF_8);
        } else {
          Reader entry = new Reader(stream.bytes());
          long secs = 0;
          long nanos = 0;
          String line = "";
          List<String> md = new ArrayList<>();
          while (entry.more()) {
            int f = entry.tag();
            if (f == 1) {
              Reader ts = new Reader(entry.bytes());
              while (ts.more()) {
                if (ts.tag() == 1) {
                  secs = ts.varint();
                } else {
                  nanos = ts.varint();
                }
              }
            } else if (f == 2) {
              line = new String(entry.bytes(), StandardCharsets.UTF_8);
            } else {
              Reader pair = new Reader(entry.bytes());
              pair.tag();
              String k = new String(pair.bytes(), StandardCharsets.UTF_8);
              pair.tag();
              String v = new String(pair.bytes(), StandardCharsets.UTF_8);
              md.add(k + "=" + v);
            }
          }
          rows.add(labels + "|" + secs + "." + nanos + "|" + line + "|" + String.join(",", md));
        }
      }
    }
    return rows;
  }

  private static final class Reader {
    private final byte[] b;
    private int i;

    Reader(byte[] b) {
      this.b = b;
    }

    boolean more() {
      return i < b.length;
    }

    long varint() {
      long v = 0;
      int shift = 0;
      while (true) {
        byte x = b[i++];
        v |= (long) (x & 0x7f) << shift;
        if ((x & 0x80) == 0) {
          return v;
        }
        shift += 7;
      }
    }

    int tag() {
      return (int) (varint() >>> 3);
    }

    byte[] bytes() {
      int n = (int) varint();
      byte[] out = java.util.Arrays.copyOfRange(b, i, i + n);
      i += n;
      return out;
    }
  }

  @Test
  void rpcStatusMessage() {
    byte[] status =
        Base64.getDecoder()
            .decode(
                FakeLoki.load("otlpLogs.empty-json")
                    .get("response")
                    .get("body")
                    .get("base64")
                    .asText());
    assertThat(Protobuf.rpcStatusMessage(status)).contains("at least one valid stream");
    Protobuf.Writer w = new Protobuf.Writer();
    w.varintField(1, 3); // code
    w.string(2, "boom");
    w.string(3, "details");
    assertThat(Protobuf.rpcStatusMessage(w.toByteArray())).isEqualTo("boom");
    assertThat(Protobuf.rpcStatusMessage(new byte[0])).isNull();
    assertThat(Protobuf.rpcStatusMessage("plain text".getBytes(StandardCharsets.UTF_8))).isNull();
    assertThat(Protobuf.rpcStatusMessage(new byte[] {0x12, 0x7f, 'a'}))
        .isNull(); // length beyond end
    assertThat(Protobuf.rpcStatusMessage(new byte[] {(byte) 0x80})).isNull(); // truncated varint
    byte[] longVarint = new byte[12];
    java.util.Arrays.fill(longVarint, (byte) 0xff);
    assertThat(Protobuf.rpcStatusMessage(longVarint)).isNull();
    assertThat(Protobuf.rpcStatusMessage(new byte[] {0x0d, 1, 2, 3, 4})).isNull(); // fixed32
    Protobuf.Writer neg = new Protobuf.Writer();
    neg.varintField(1, -1);
    assertThat(Protobuf.rpcStatusMessage(neg.toByteArray())).isNull();
    assertThat(Protobuf.snappy(new byte[0])).isNotNull();
  }
}
