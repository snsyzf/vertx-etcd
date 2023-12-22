package io.vertx.spi.cluster.etcd;

import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import com.ibm.etcd.client.EtcdClient;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.spi.cluster.etcd.impl.EtcdWatchedMultiMapImpl;
import io.vertx.spi.cluster.etcd.impl.Internal;

/**
 * WatchedMultiMap.
 *
 * @author <a href="mailto:snsyzf@qq.com">yzf</a>
 * @version 1.0
 */
public interface WatchedMultiMap<K, V> extends Multimap<K, V>, AutoCloseable {

	static <K, V> Future<WatchedMultiMap<K, V>> create(EtcdClient client, Vertx vertx, String namespace, String name) {
		return vertx.executeBlocking(() -> {
			final var v = new EtcdWatchedMultiMapImpl<K, V>(client, vertx,
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
