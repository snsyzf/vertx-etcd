package io.vertx.spi.cluster.etcd;

import com.google.protobuf.ByteString;
import com.ibm.etcd.client.EtcdClient;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.spi.cluster.etcd.impl.EtcdWatchedMapImpl;
import io.vertx.spi.cluster.etcd.impl.Internal;
import org.jetbrains.annotations.NotNull;

import java.util.Map;

/**
 * WatchedMap.
 *
 * @author <a href="mailto:snsyzf@qq.com">yzf</a>
 * @version 1.0
 */
public interface WatchedMap<K, V> extends Map<K, V>, AutoCloseable {

	static <K, V> Future<WatchedMap<K, V>> create(@NotNull EtcdClient client, @NotNull Vertx vertx,
			@NotNull String namespace, @NotNull String name) {
		return vertx.executeBlocking(() -> {
			final var v = new EtcdWatchedMapImpl<K, V>(client, vertx,
					ByteString.copyFromUtf8(namespace)
						.concat(Internal.DELIMITER)
						.concat(ByteString.copyFromUtf8(name)));
			v.start();
			return v;
		});
	}

	int addListener(Handler<WatchUpdate<K, V>> listener);

	void removeListener(int listenerID);

}
