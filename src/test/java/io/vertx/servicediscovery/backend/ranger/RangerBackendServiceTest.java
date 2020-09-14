package io.vertx.servicediscovery.backend.ranger;

import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import io.vertx.servicediscovery.spi.ServiceDiscoveryBackend;
import io.vertx.servicediscovery.spi.ServiceDiscoveryBackendTest;
import org.apache.curator.test.TestingServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;

import java.io.IOException;

public class RangerBackendServiceTest extends ServiceDiscoveryBackendTest {

  private static TestingServer server;

  @BeforeClass
  public static void startZookeeper() throws Exception {
    server = new TestingServer();
    server.start();
  }

  @AfterClass
  public static void stopZookeeper() throws IOException {
    server.stop();
  }

  @Override
  protected ServiceDiscoveryBackend createBackend() {
    RangerBackendService backend = new RangerBackendService();
    backend.init(vertx, new JsonObject()
        .put("zkConnectionString",  server.getConnectString())
        .put("host", "localhost")
        .put("port", 8080)
        .put("env", "test")
        .put("namespace", "test")
        .put("service", "my-service")
        .put("baseSleepTimeBetweenRetries",  10)
        .put("connectionTimeoutMs",  1000));
    return backend;
  }

  @Ignore
  public void testInsertionOfMultipleRecords() {
  }
}
