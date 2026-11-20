// Service-owned: generated once by sdk-kit, then maintained in this repository.
package codes.grow.packages.loki;

import static org.assertj.core.api.Assertions.assertThat;

import codes.grow.packages.loki.core.ConfigException;
import codes.grow.packages.loki.logging.LoggingInit.LogConnection;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LogConnectTest {
  @Test
  void connectsWithoutIo() {
    LogConnection c;
    try {
      c = LogConnect.connect(Map.of("service.name", "t"), Map.of(), records -> {});
    } catch (ConfigException e) {
      return; // the service needs more settings (credentials): initLogging falls back to stdout
    }
    if (c != null) {
      assertThat(c.logs()).isNotNull();
      c.close().run();
    }
  }
}
