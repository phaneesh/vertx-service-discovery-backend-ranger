package io.vertx.servicediscovery.backend.ranger;

import com.google.common.collect.ImmutableMap;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.ServiceDiscoveryOptions;
import io.vertx.servicediscovery.Status;

import java.util.Map;

public class RangerBackendServicePublisher {

  public static ServiceDiscovery publish(Vertx vertx, JsonObject config, Handler<AsyncResult<Record>> handler) {
    Map<String, String> shardInfo = ImmutableMap.<String, String>builder()
        .put("environment", config.getString("env"))
        .build();
    JsonObject nodeData = new JsonObject();
    nodeData.put("host", config.getString("host"));
    nodeData.put("port", config.getInteger("port"));
    nodeData.put("healthcheckStatus", true);
    nodeData.put("nodeData", shardInfo);
    Record record = new Record();
    record.setMetadata(nodeData);
    record.setStatus(Status.UP);
    record.setName(config.getString("service"));
    record.setType("ranger");
    ServiceDiscovery discovery = ServiceDiscovery.create(vertx, new ServiceDiscoveryOptions()
        .setBackendConfiguration(config));

    discovery.publish(record, r -> {
      if (r.succeeded()) {
        handler.handle(Future.succeededFuture(r.result()));
      } else {
        handler.handle(Future.failedFuture(r.cause()));
      }
    });
    return discovery;
  }
}
