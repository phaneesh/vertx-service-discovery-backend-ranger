# Vert.x Ranger Service Discovery Module

### Usage Example
```java
JsonObject config = new JsonObject()
        .put("zkConnectionString",  "localhost:2181")
        .put("host", "localhost")
        .put("port", 8080)
        .put("env", "test")
        .put("namespace", "test")
        .put("service", "my-service")
        .put("baseSleepTimeBetweenRetries",  10)
        .put("connectionTimeoutMs",  1000));
ServiceDiscoverOptions options = new ServiceDiscoveryOptions()
                                        .setBackendConfiguration(config);
ServiceDiscovery.create(vertx, options);
```
