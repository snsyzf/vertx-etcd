package io.vertx.spi.cluster.etcd.impl;

import com.google.common.collect.*;
import com.google.protobuf.ByteString;
import com.ibm.etcd.api.Event;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.kv.WatchUpdate;
import io.grpc.stub.StreamObserver;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.spi.cluster.etcd.WatchEvent;
import io.vertx.spi.cluster.etcd.WatchedMultiMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

/**
 * EtcdWatchedMultiMapImpl.
 *
 * @author <a href="mailto:snsyzf@qq.com">yzf</a>
 * @version 1.0
 */
public class EtcdWatchedMultiMapImpl<K, V> extends EtcdMap<K, V>
		implements WatchedMultiMap<K, V>, StreamObserver<WatchUpdate> {

	private final ListMultimap<K, V> values = Multimaps.synchronizedListMultimap(LinkedListMultimap.create());

	private final Map<Integer, Handler<io.vertx.spi.cluster.etcd.WatchUpdate<K, V>>> listeners = Maps
		.newConcurrentMap();

	private int listenerID = 0;

	private KvClient.Watch watcher;

	public EtcdWatchedMultiMapImpl(EtcdClient client, Vertx vertx, ByteString prefix) {
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

				final K kk = Internal.asObject(kv.getValue());
				final V vv = Internal.asObject(withoutPrefixKey(kv.getKey()).substring(kv.getValue().size()));

				this.values.put(kk, vv);
			}
		}
	}

	@Override
	public int addListener(Handler<io.vertx.spi.cluster.etcd.WatchUpdate<K, V>> listener) {
		synchronized (this) {
			int id = ++this.listenerID;
			this.listeners.put(id, listener);
			return id;
		}
	}

	@Override
	public void removeListener(int listenerID) {
		this.listeners.remove(listenerID);
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
	public boolean containsKey(@Nullable Object key) {
		return this.values.containsKey(key);
	}

	@Override
	public boolean containsValue(@Nullable Object value) {
		return this.values.containsValue(value);
	}

	@Override
	public boolean containsEntry(@Nullable Object key, @Nullable Object value) {
		return this.values.containsEntry(key, value);
	}

	@Override
	public boolean put(K key, V value) {
		if (this.values.put(key, value)) {
			this.vertx.executeBlocking(Executors.callable(() -> {
				final var kk = Internal.asBytes(key);

				this.client.getKvClient().put(withPrefixKey(kk.concat(Internal.asBytes(value))), kk).sync();
			}));
			return true;
		}
		return false;
	}

	@Override
	public boolean remove(@Nullable Object key, @Nullable Object value) {
		if (this.values.remove(key, value)) {
			this.vertx.executeBlocking(Executors.callable(() -> {
				this.client.getKvClient()
					.delete(withPrefixKey(Internal.asBytes(key).concat(Internal.asBytes(value))))
					.sync();
			}));
			return true;
		}
		return false;
	}

	@Override
	public boolean putAll(K key, @NotNull Iterable<? extends V> values) {
		if (this.values.putAll(key, values)) {
			this.vertx.executeBlocking(Executors.callable(() -> {
				final var kk = Internal.asBytes(key);
				final KvClient.FluentTxnOps<?> batch = this.client.getKvClient().batch();
				for (V value : values) {
					batch.put(this.client.getKvClient()
						.put(withPrefixKey(kk.concat(Internal.asBytes(value))), kk)
						.asRequest());
				}
				batch.sync();
			}));
			return true;
		}
		return false;
	}

	@Override
	public boolean putAll(@NotNull Multimap<? extends K, ? extends V> multimap) {
		if (this.values.putAll(multimap)) {
			this.vertx.executeBlocking(Executors.callable(() -> {
				final KvClient.FluentTxnOps<?> batch = this.client.getKvClient().batch();
				for (Map.Entry<? extends K, ? extends Collection<? extends V>> entry : multimap.asMap().entrySet()) {
					final var kk = Internal.asBytes(entry.getKey());

					for (V value : entry.getValue()) {
						batch.put(this.client.getKvClient()
							.put(withPrefixKey(kk.concat(Internal.asBytes(value))), kk)
							.asRequest());
					}
				}
				batch.sync();
			}));
			return true;
		}
		return false;
	}

	@Override
	public @NotNull Collection<V> replaceValues(K key, @NotNull Iterable<? extends V> values) {
		this.vertx.executeBlocking(Executors.callable(() -> {
			final var kk = Internal.asBytes(key);

			final KvClient.FluentTxnOps<?> batch = this.client.getKvClient().batch();
			batch.delete(this.client.getKvClient().delete(withPrefixKey(kk)).asPrefix().asRequest());

			for (V value : values) {
				batch.put(this.client.getKvClient()
					.put(withPrefixKey(kk).concat(Internal.asBytes(value)), kk)
					.asRequest());
			}
			batch.sync();
		}));
		return this.values.replaceValues(key, values);
	}

	@Override
	public @NotNull Collection<V> removeAll(@Nullable Object key) {
		this.vertx.executeBlocking(Executors.callable(() -> {
			this.client.getKvClient().delete(withPrefixKey(Internal.asBytes(key))).asPrefix().sync();
		}));
		return this.values.removeAll(key);
	}

	@Override
	public void clear() {
		this.values.clear();
		this.vertx.executeBlocking(Executors.callable(() -> {
			this.client.getKvClient().delete(this.prefix).asPrefix().sync();
		}));
	}

	@Override
	public @NotNull Collection<V> get(K key) {
		return this.values.get(key);
	}

	@Override
	public @NotNull Set<K> keySet() {
		return this.values.keySet();
	}

	@Override
	public @NotNull Multiset<K> keys() {
		return this.values.keys();
	}

	@Override
	public @NotNull Collection<V> values() {
		return this.values.values();
	}

	@Override
	public @NotNull Collection<Map.Entry<K, V>> entries() {
		return this.values.entries();
	}

	@Override
	public @NotNull Map<K, Collection<V>> asMap() {
		return this.values.asMap();
	}

	@Override
	public void onNext(WatchUpdate update) {
		for (Event event : update.getEvents()) {
			if (event.getType() == Event.EventType.PUT) {
				final K kk = Internal.asObject(event.getKv().getValue());
				final V vv = Internal
					.asObject(withoutPrefixKey(event.getKv().getKey()).substring(event.getKv().getValue().size()));

				this.values.put(kk, vv);

				if (!this.listeners.isEmpty()) {
					final var e = new io.vertx.spi.cluster.etcd.WatchUpdate<>(WatchEvent.PUT, kk, vv);
					for (Handler<io.vertx.spi.cluster.etcd.WatchUpdate<K, V>> handler : this.listeners.values()) {
						handler.handle(e);
					}
				}
			}
			else if (event.getType() == Event.EventType.DELETE) {
				final K kk = Internal.asObject(event.getPrevKv().getValue());
				final V vv = Internal
					.asObject(withoutPrefixKey(event.getKv().getKey()).substring(event.getPrevKv().getValue().size()));

				this.values.remove(kk, vv);

				if (!this.listeners.isEmpty()) {
					final var e = new io.vertx.spi.cluster.etcd.WatchUpdate<>(WatchEvent.REMOVE, kk, vv);
					for (Handler<io.vertx.spi.cluster.etcd.WatchUpdate<K, V>> handler : this.listeners.values()) {
						handler.handle(e);
					}
				}
			}
		}
	}

	@Override
	public void onError(Throwable t) {

	}

	@Override
	public void onCompleted() {

	}

	@Override
	public void close() throws Exception {
		synchronized (this) {
			if (this.watcher != null) {
				this.watcher.close();
			}
		}
	}

}
