Vertx etcd cluster manager

This project provides a simple way to manage a vertx etcd cluster.

## Usage

```java
EtcdClient client = EtcdClient.forEndpoints("http://localhost:2379").withPlainText().build();
Vertx vertx = await(Vertx.builder()
			.withClusterManager(
					new EtcdClusterManager(client, "/test" + System.currentTimeMillis(), UUID.randomUUID().toString()))
        .buildClustered());
```
