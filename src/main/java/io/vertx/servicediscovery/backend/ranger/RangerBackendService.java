package io.vertx.servicediscovery.backend.ranger;


import com.google.common.collect.ImmutableMap;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.Status;
import io.vertx.servicediscovery.spi.ServiceDiscoveryBackend;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class RangerBackendService implements ServiceDiscoveryBackend, ConnectionStateListener {

  private static final Charset CHARSET = StandardCharsets.UTF_8;

  private String registrationId;

  private String namespace;

  private String service;

  private String basePath;

  private int connectionTimeoutMs;

  private CuratorFramework client;

  private Vertx vertx;

  private JsonObject nodeData;

  private Map<String, String> shardInfo;

  private ConnectionState connectionState = ConnectionState.LOST;

  @Override
  public void init(Vertx vertx, JsonObject config) {
    this.vertx = vertx;
    this.registrationId = config.getString("host") + ":" + config.getInteger("port");
    this.namespace = config.getString("namespace");
    this.service = config.getString("service");
    this.basePath = namespace.startsWith("/") ? this.namespace : "/" + this.namespace;
    this.basePath = this.basePath + "/" + this.service;
    this.connectionTimeoutMs = config.getInteger("connectionTimeoutMs", 1000);
    this.nodeData = new JsonObject();
    this.nodeData.put("host", config.getString("host"));
    this.nodeData.put("port", config.getInteger("port"));
    this.nodeData.put("healthcheckStatus", true);
    this.nodeData.put("nodeData", shardInfo);
    this.shardInfo = ImmutableMap.<String, String>builder()
        .put("environment", config.getString("env"))
        .build();
    this.client = CuratorFrameworkFactory.builder()
        .connectString(config.getString("zkConnectionString"))
        .connectionTimeoutMs(config.getInteger("connectionTimeoutMs", 1000))
        .retryPolicy(new RetryForever(config.getInteger("baseSleepTimeBetweenRetries", 1000))).build();
    client.getConnectionStateListenable().addListener(this);
    this.client.start();
  }

  @Override
  public void store(Record record, Handler<AsyncResult<Record>> resultHandler) {
    if (record.getRegistration() != null) {
      resultHandler.handle(Future.failedFuture("The record has already been registered"));
      return;
    }
    nodeData.put("lastUpdatedTimeStamp", System.currentTimeMillis());
    record.setRegistration(registrationId);
    record.setMetadata(nodeData);
    record.setStatus(Status.UP);
    record.setName(service);
    String content = record.toJson().encode();
    Context context = Vertx.currentContext();
    ensureConnected(x -> {
      if (x.failed()) {
        resultHandler.handle(Future.failedFuture(x.cause()));
      } else {
        try {
          client.create()
              .creatingParentsIfNeeded()
              .withMode(CreateMode.EPHEMERAL)
              .inBackground((curatorFramework, curatorEvent)
                  -> callback(context, record, resultHandler, curatorEvent))
              .withUnhandledErrorListener((s, throwable)
                  -> resultHandler.handle(Future.failedFuture(throwable)))
              .forPath(getPath(record.getRegistration()), content.getBytes(CHARSET));
        } catch (Exception e) {
          resultHandler.handle(Future.failedFuture(e));
        }
      }
    });
  }

  @Override
  public void remove(Record record, Handler<AsyncResult<Record>> handler) {
    remove(record.getRegistration(), handler);
  }

  @Override
  public void remove(String registration, Handler<AsyncResult<Record>> resultHandler) {
    Objects.requireNonNull(registration, "No registration id in the record");
    Context context = Vertx.currentContext();
    ensureConnected(x -> {
      if (x.failed()) {
        resultHandler.handle(Future.failedFuture(x.cause()));
      } else {
        getRecordByRegistration(context, registration, record -> {
          if (record == null) {
            resultHandler.handle(Future.failedFuture("Unknown registration " + registration));
          } else {
            try {
              DeleteBuilder delete = client.delete();
              delete.guaranteed();
              delete
                  .deletingChildrenIfNeeded()
                  .inBackground((curatorFramework, curatorEvent)
                      -> callback(context, record, resultHandler, curatorEvent))

                  .withUnhandledErrorListener((s, throwable)
                      -> resultHandler.handle(Future.failedFuture(throwable)))

                  .forPath(getPath(registration));
            } catch (Exception e) {
              resultHandler.handle(Future.failedFuture(e));
            }
          }
        });
      }
    });
  }

  @Override
  public void update(Record record, Handler<AsyncResult<Void>> resultHandler) {
    Objects.requireNonNull(record.getRegistration(), "No registration id in the record");
    Context context = Vertx.currentContext();
    ensureConnected(x -> {
      if (x.failed()) {
        resultHandler.handle(Future.failedFuture(x.cause()));
      } else {
        try {
          JsonObject nodeData = record.toJson();
          nodeData.put("lastUpdatedTimeStamp", System.currentTimeMillis());
          client.setData()
              .inBackground((framework, event)
                  -> runOnContextIfPossible(context, () -> {
                if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
                  resultHandler.handle(Future.succeededFuture());
                } else {
                  KeeperException.Code code = KeeperException.Code.get(event.getResultCode());
                  resultHandler.handle(Future.failedFuture(KeeperException.create(code)));
                }
              }))
              .withUnhandledErrorListener((message, e) -> resultHandler.handle(Future.failedFuture(e)))
              .forPath(getPath(record.getRegistration()),
                  nodeData.encode().getBytes(CHARSET));
        } catch (Exception e) {
          resultHandler.handle(Future.failedFuture(e));
        }
      }
    });
  }

  @Override
  public void getRecords(Handler<AsyncResult<List<Record>>> resultHandler) {
    Context context = Vertx.currentContext();
    ensureConnected(
        x -> {
          if (x.failed()) {
            resultHandler.handle(Future.failedFuture(x.cause()));
          } else {
            try {
              client.getChildren()
                  .inBackground((fmk, event) -> {
                    List<String> children = event.getChildren();
                    List<Future> futures = new ArrayList<>();
                    for (String child : children) {
                      Promise<Record> promise = Promise.promise();
                      getRecord(child, promise);
                      futures.add(promise.future());
                    }
                    CompositeFuture.all(futures)
                        .onComplete(
                            ar -> runOnContextIfPossible(context, () -> {
                              if (ar.failed()) {
                                resultHandler.handle(Future.failedFuture(ar.cause()));
                              } else {
                                List<Record> records = new ArrayList<>();
                                for (Future future : futures) {
                                  records.add((Record) future.result());
                                }
                                resultHandler.handle(Future.succeededFuture(records));
                              }
                            }));
                  })
                  .withUnhandledErrorListener((message, e) -> resultHandler.handle(Future.failedFuture(e)))
                  .forPath(basePath);
            } catch (Exception e) {
              resultHandler.handle(Future.failedFuture(e));
            }
          }
        }
    );
  }

  @Override
  public void getRecord(String registration, Handler<AsyncResult<Record>> handler) {
    Objects.requireNonNull(registration);
    Context context = Vertx.currentContext();

    ensureConnected(x -> {
      if (x.failed()) {
        handler.handle(Future.failedFuture(x.cause()));
      } else {
        try {
          client.getData()
              .inBackground((fmk, curatorEvent)
                  -> runOnContextIfPossible(context, () -> {
                if (curatorEvent.getResultCode() == KeeperException.Code.OK.intValue()) {
                  JsonObject json
                      = new JsonObject(new String(curatorEvent.getData(), CHARSET));
                  handler.handle(Future.succeededFuture(new Record(json)));
                } else if (curatorEvent.getResultCode() == KeeperException.Code.NONODE.intValue()) {
                  handler.handle(Future.succeededFuture(null));
                } else {
                  KeeperException.Code code = KeeperException.Code.get(curatorEvent.getResultCode());
                  handler.handle(Future.failedFuture(KeeperException.create(code)));
                }
              }))
              .withUnhandledErrorListener((message, e) -> handler.handle(Future.failedFuture(e)))
              .forPath(getPath(registration));
        } catch (Exception e) {
          handler.handle(Future.failedFuture(e));
        }
      }
    });
  }

  @Override
  public void stateChanged(CuratorFramework client, ConnectionState newState) {
    this.connectionState = newState;
  }

  private synchronized void ensureConnected(Handler<AsyncResult<Void>> handler) {
    switch (connectionState) {
      case CONNECTED:
      case RECONNECTED:
        handler.handle(Future.succeededFuture());
        break;
      case READ_ONLY:
      case LOST:
      case SUSPENDED:
        vertx.executeBlocking(
            future -> {
              try {
                if (client.blockUntilConnected(connectionTimeoutMs, TimeUnit.MILLISECONDS)) {
                  future.complete();
                } else {
                  future.fail(new TimeoutException());
                }
              } catch (Exception e) {
                future.fail(e);
              }
            }, ar -> {
              if (ar.failed()) {
                handler.handle(Future.failedFuture(KeeperException.create(KeeperException.Code.CONNECTIONLOSS)));
              } else {
                handler.handle(Future.succeededFuture());
              }
            });
        break;
    }
  }

  private void runOnContextIfPossible(Context context, Runnable runnable) {
    if (context != null) {
      context.runOnContext(v -> runnable.run());
    } else {
      runnable.run();
    }
  }

  private void callback(Context context, Record record, Handler<AsyncResult<Record>> resultHandler, CuratorEvent curatorEvent) {
    runOnContextIfPossible(context, () -> {
      if (curatorEvent.getResultCode() == KeeperException.Code.OK.intValue()) {
        resultHandler.handle(Future.succeededFuture(record));
      } else {
        KeeperException.Code code =
            KeeperException.Code.get(curatorEvent.getResultCode());
        resultHandler.handle(Future.failedFuture(KeeperException.create(code)));
      }
    });
  }

  private String getPath(String registration) {
    return basePath + "/" + registration;

  }

  private void getRecordByRegistration(Context context, String registration, Handler<Record> handler) {
    ensureConnected(x -> {
      if (x.failed()) {
        handler.handle(null);
      } else {
        try {
          client.getData()
              .inBackground((curatorFramework, curatorEvent)
                  -> runOnContextIfPossible(context, () -> {
                if (curatorEvent.getResultCode() == KeeperException.Code.OK.intValue()) {
                  JsonObject json
                      = new JsonObject(new String(curatorEvent.getData(), CHARSET));
                  handler.handle(new Record(json));
                } else {
                  handler.handle(null);
                }
              }))
              .forPath(getPath(registration));
        } catch (Exception e) {
          handler.handle(null);
        }
      }
    });
  }
}