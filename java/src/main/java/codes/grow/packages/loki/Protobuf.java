package codes.grow.packages.loki;

import io.airlift.compress.snappy.SnappyCompressor;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Hand-written protobuf encoding of {@code logproto.PushRequest}, from the field numbers of Loki's
 * Apache-2.0 {@code pkg/push/push.proto} (v3.7.10); no Loki code is used:
 *
 * <pre>
 * PushRequest      { repeated StreamAdapter streams = 1; }
 * StreamAdapter    { string labels = 1; repeated EntryAdapter entries = 2; }
 * EntryAdapter     { google.protobuf.Timestamp timestamp = 1; string line = 2;
 *                    repeated LabelPairAdapter structuredMetadata = 3; }
 * LabelPairAdapter { string name = 1; string value = 2; }
 * Timestamp        { int64 seconds = 1; int32 nanos = 2; }
 * </pre>
 *
 * <p>The body is compressed with Snappy block format (aircompressor, Apache-2.0).
 */
final class Protobuf {
  private Protobuf() {}

  /** Encodes and Snappy-compresses a push request (timestamps must be set). */
  static byte[] pushRequest(List<LokiStream> streams) {
    Writer out = new Writer();
    for (LokiStream s : streams) {
      Writer sw = new Writer();
      sw.string(1, Labels.formatLabelSet(s.labels()));
      for (LokiEntry e : s.entries()) {
        sw.bytes(2, entry(e));
      }
      out.bytes(1, sw.toByteArray());
    }
    return snappy(out.toByteArray());
  }

  private static byte[] entry(LokiEntry e) {
    Writer ts = new Writer();
    if (e.timestamp().getEpochSecond() != 0) {
      ts.varintField(1, e.timestamp().getEpochSecond());
    }
    if (e.timestamp().getNano() != 0) {
      ts.varintField(2, e.timestamp().getNano());
    }
    Writer w = new Writer();
    w.bytes(1, ts.toByteArray());
    w.string(2, e.line());
    for (Map.Entry<String, String> m : new TreeMap<>(e.structuredMetadata()).entrySet()) {
      Writer pair = new Writer();
      pair.string(1, m.getKey());
      pair.string(2, m.getValue());
      w.bytes(3, pair.toByteArray());
    }
    return w.toByteArray();
  }

  /** Snappy block format (not the framed stream format). */
  static byte[] snappy(byte[] input) {
    SnappyCompressor c = new SnappyCompressor();
    byte[] out = new byte[c.maxCompressedLength(input.length)];
    int n = c.compress(input, 0, input.length, out, 0, out.length);
    return Arrays.copyOf(out, n);
  }

  /**
   * Returns the message (field 2) of a {@code google.rpc.Status} body (Loki's OTLP error bodies),
   * or {@code null} when the bytes are not such a message.
   */
  static String rpcStatusMessage(byte[] body) {
    int i = 0;
    String message = null;
    try {
      while (i < body.length) {
        long[] tag = varint(body, i);
        i = (int) tag[1];
        int field = (int) (tag[0] >>> 3);
        int wire = (int) (tag[0] & 7);
        if (wire == 0) {
          i = (int) varint(body, i)[1];
        } else if (wire == 2) {
          long[] len = varint(body, i);
          int start = (int) len[1];
          int end = Math.addExact(start, (int) len[0]);
          if (len[0] < 0 || end > body.length) {
            return null;
          }
          if (field == 2) {
            message = new String(body, start, end - start, StandardCharsets.UTF_8);
          }
          i = end;
        } else {
          return null;
        }
      }
    } catch (ArithmeticException | ArrayIndexOutOfBoundsException e) {
      return null;
    }
    return message;
  }

  /** Reads a varint at {@code pos}: returns {value, next position}. */
  private static long[] varint(byte[] b, int pos) {
    long value = 0;
    int shift = 0;
    int i = pos;
    while (true) {
      byte x = b[i++];
      value |= (long) (x & 0x7f) << shift;
      if ((x & 0x80) == 0) {
        return new long[] {value, i};
      }
      shift += 7;
      if (shift > 63) {
        throw new ArithmeticException("varint too long");
      }
    }
  }

  /** Minimal protobuf writer. */
  static final class Writer {
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    void varint(long v) {
      long x = v;
      while ((x & ~0x7fL) != 0) {
        out.write((int) ((x & 0x7f) | 0x80));
        x >>>= 7;
      }
      out.write((int) x);
    }

    void tag(int field, int wire) {
      varint(((long) field << 3) | wire);
    }

    void varintField(int field, long v) {
      tag(field, 0);
      varint(v);
    }

    void bytes(int field, byte[] b) {
      tag(field, 2);
      varint(b.length);
      out.writeBytes(b);
    }

    void string(int field, String s) {
      bytes(field, s.getBytes(StandardCharsets.UTF_8));
    }

    byte[] toByteArray() {
      return out.toByteArray();
    }
  }
}
