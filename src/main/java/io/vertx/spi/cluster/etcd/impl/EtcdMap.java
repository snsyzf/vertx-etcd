package io.vertx.spi.cluster.etcd.impl;

import com.google.protobuf.ByteString;
import com.ibm.etcd.client.EtcdClient;
import io.vertx.core.Vertx;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

/**
 * EtcdMap.
 *
 * @author <a href="mailto:snsyzf@qq.com">yzf</a>
 * @version 1.0
 */
public class EtcdMap<K, V> {

	protected final EtcdClient client;

	protected final Vertx vertx;

	protected final ByteString prefix;

	public EtcdMap(EtcdClient client, Vertx vertx, ByteString prefix) {
		this.client = client;
		this.vertx = vertx;
		this.prefix = prefix;
	}

	ByteString withPrefixKey(@NotNull ByteString key) {
		if (key.startsWith(Internal.DELIMITER)) {
			throw new IllegalArgumentException();
		}
		return this.prefix.concat(Internal.DELIMITER).concat(key);
	}

	ByteString withoutPrefixKey(@NotNull ByteString key) {
		return key.substring(this.prefix.size() + Internal.DELIMITER.size());
	}

	protected final @Nullable V get0(Object k) {
		final var response = this.client.getKvClient()
			.get(withPrefixKey(Internal.asBytes(k)))
			.serializable(true)
			.sync();
		if (response.getKvsList().isEmpty()) {
			return null;
		}
		return Internal.asObject(response.getKvs(0).getValue());
	}

	protected final @Nullable V put0(K k, V v) {
		final var response = this.client.getKvClient()
			.put(withPrefixKey(Internal.asBytes(k)), Internal.asBytes(v))
			.prevKv()
			.sync();
		if (response.hasPrevKv()) {
			return Internal.asObject(response.getPrevKv().getValue());
		}
		return null;
	}

	protected final @Nullable V remove0(Object k) {
		final var response = this.client.getKvClient().delete(withPrefixKey(Internal.asBytes(k))).prevKv().sync();
		if (response.getPrevKvsList().isEmpty()) {
			return null;
		}
		return Internal.asObject(response.getPrevKvs(0).getValue());
	}

	protected final int size0() {
		return (int) this.client.getKvClient().get(this.prefix).asPrefix().countOnly().sync().getCount();
	}

	protected final void clear0() {
		this.client.getKvClient().delete(this.prefix).asPrefix().sync();
	}

	protected final Set<K> keys0() {
		final var response = this.client.getKvClient().get(this.prefix).asPrefix().keysOnly().sync();
		if (response.getKvsList().isEmpty()) {
			return Set.of();
		}
		Set<K> keys = new HashSet<>(response.getKvsCount());
		for (int i = 0; i < response.getKvsCount(); i++) {
			keys.add(Internal.asObject(withoutPrefixKey(response.getKvs(i).getKey())));
		}
		return keys;
	}

	protected final List<V> values0() {
		final var response = this.client.getKvClient().get(this.prefix).asPrefix().sync();
		if (response.getKvsList().isEmpty()) {
			return List.of();
		}
		List<V> values = new ArrayList<>(response.getKvsCount());
		for (int i = 0; i < response.getKvsCount(); i++) {
			values.add(Internal.asObject(response.getKvs(i).getValue()));
		}
		return values;
	}

	protected final Map<K, V> entries0() {
		final var response = this.client.getKvClient().get(this.prefix).asPrefix().serializable(true).sync();
		if (response.getKvsList().isEmpty()) {
			return Map.of();
		}
		Map<K, V> entries = new LinkedHashMap<>(response.getKvsCount());
		for (int i = 0; i < response.getKvsCount(); i++) {
			final var kv = response.getKvs(i);
			entries.put(Internal.asObject(withoutPrefixKey(kv.getKey())), Internal.asObject(kv.getValue()));
		}
		return entries;
	}

}
