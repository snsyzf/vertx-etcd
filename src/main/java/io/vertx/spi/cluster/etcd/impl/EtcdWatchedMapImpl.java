package io.vertx.spi.cluster.etcd.impl;

import com.google.protobuf.ByteString;
import com.ibm.etcd.api.Event;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.kv.WatchUpdate;
import io.grpc.stub.StreamObserver;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.spi.cluster.etcd.WatchEvent;
import io.vertx.spi.cluster.etcd.WatchedMap;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

/**
 * EtcdWatchedMapImpl.
 *
 * @author <a href="mailto:snsyzf@qq.com">yzf</a>
 * @version 1.0
 */
public class EtcdWatchedMapImpl<K, V> extends EtcdMap<K, V> implements WatchedMap<K, V>, StreamObserver<WatchUpdate> {

	private static final Logger log = LoggerFactory.getLogger(EtcdWatchedMapImpl.class);

	private KvClient.Watch watcher;

	private final Map<K, V> values = new ConcurrentHashMap<>();

	private final Map<Integer, Handler<io.vertx.spi.cluster.etcd.WatchUpdate<K, V>>> listeners = new LinkedHashMap<>();

	private int listenerId;

	public EtcdWatchedMapImpl(EtcdClient client, Vertx vertx, ByteString prefix) {
		super(client, vertx, prefix);
	}

	public synchronized void start() {
		if (this.watcher != null) {
			return;
		}
		this.watcher = this.client.getKvClient().watch(this.prefix).asPrefix().prevKv().progressNotify().start(this);
		final var response = this.client.getKvClient().get(this.prefix).serializable(true).asPrefix().sync();
		if (!response.getKvsList().isEmpty()) {
			for (int i = 0; i < response.getKvsCount(); i++) {
				final var kv = response.getKvs(i);

				this.values.put(Internal.asObject(withoutPrefixKey(kv.getKey())), Internal.asObject(kv.getValue()));
			}
		}
	}

	@Override
	public synchronized void close() {
		if (this.watcher != null) {
			this.watcher.close();
		}
	}

	@Override
	public int addListener(Handler<io.vertx.spi.cluster.etcd.WatchUpdate<K, V>> listener) {
		synchronized (this) {
			int id = ++this.listenerId;
			this.listeners.put(id, listener);
			return id;
		}
	}

	@Override
	public void removeListener(int listenerID) {
		synchronized (this) {
			this.listeners.remove(listenerID);
		}
	}

	@Override
	public int size() {
		return this.values.size();
	}

	@Override
	public boolean isEmpty() {
		return this.values.isEmpty();
	}

	@Override
	public boolean containsKey(Object key) {
		return this.values.containsKey(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return this.values.containsValue(value);
	}

	@Override
	public V get(Object key) {
		return this.values.get(key);
	}

	@Override
	public V put(K key, V value) {
		this.vertx.executeBlocking(Executors.callable(() -> {
			super.put0(key, value);
		}));
		return this.values.put(key, value);
	}

	@Override
	public V remove(Object key) {
		this.vertx.executeBlocking(Executors.callable(() -> {
			super.remove0(key);
		}));
		return this.values.remove(key);
	}

	@Override
	public void putAll(@NotNull Map<? extends K, ? extends V> m) {
		this.vertx.executeBlocking(Executors.callable(() -> m.forEach(super::put0)));
		this.values.putAll(m);
	}

	@Override
	public void clear() {
		this.vertx.executeBlocking(Executors.callable(super::clear0));
		this.values.clear();
	}

	@Override
	public @NotNull Set<K> keySet() {
		return this.values.keySet();
	}

	@Override
	public @NotNull Collection<V> values() {
		return this.values.values();
	}

	@Override
	public @NotNull Set<Entry<K, V>> entrySet() {
		return this.values.entrySet();
	}

	@Override
	public void onNext(@NotNull WatchUpdate update) {
		for (Event event : update.getEvents()) {
			if (event.getType() == Event.EventType.PUT) {
				K key = Internal.asObject(withoutPrefixKey(event.getKv().getKey()));
				V value = Internal.asObject(event.getKv().getValue());

				this.values.put(key, value);

				if (!this.listeners.isEmpty()) {
					io.vertx.spi.cluster.etcd.WatchUpdate<K, V> e = new io.vertx.spi.cluster.etcd.WatchUpdate<>(
							WatchEvent.PUT, key, value);
					for (Handler<io.vertx.spi.cluster.etcd.WatchUpdate<K, V>> handler : this.listeners.values()) {
						handler.handle(e);
					}
				}
			}
			else if (event.getType() == Event.EventType.DELETE) {
				K key = Internal.asObject(withoutPrefixKey(event.getPrevKv().getKey()));
				V value = Internal.asObject(event.getPrevKv().getValue());

				this.values.remove(key);

				if (!this.listeners.isEmpty()) {
					io.vertx.spi.cluster.etcd.WatchUpdate<K, V> e = new io.vertx.spi.cluster.etcd.WatchUpdate<>(
							WatchEvent.REMOVE, key, value);
					for (Handler<io.vertx.spi.cluster.etcd.WatchUpdate<K, V>> handler : this.listeners.values()) {
						handler.handle(e);
					}
				}
			}
		}
	}

	@Override
	public void onError(Throwable t) {
		log.warn("etcd watch exception", t);
	}

	@Override
	public void onCompleted() {
		log.debug("etcd watch completed");
	}

}
