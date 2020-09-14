# Vert.x Ranger Service Discovery Module

### Maven
```xml
    <repositories>
        <repository>
            <id>clojars</id>
            <name>Clojars repository</name>
            <url>https://clojars.org/repo</url>
        </repository>
    </repositories>

    <dependency>
        <groupId>io.raven.vertx</groupId>
        <artifactId>vertx-service-discovery-backend-ranger</artifactId>
        <version>3.9.1-1</version>
    </dependency>
```
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
