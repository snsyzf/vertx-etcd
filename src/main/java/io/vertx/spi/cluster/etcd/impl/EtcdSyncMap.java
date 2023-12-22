package io.vertx.spi.cluster.etcd.impl;

import com.google.protobuf.ByteString;
import com.ibm.etcd.client.EtcdClient;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * EtcdSyncMap.
 *
 * @author <a href="mailto:snsyzf@qq.com">yzf</a>
 * @version 1.0
 */
public class EtcdSyncMap<K, V> extends EtcdMap<K, V> implements Map<K, V> {

	public EtcdSyncMap(EtcdClient client, Vertx vertx, ByteString prefix) {
		super(client, vertx, prefix);
	}

	@Override
	public int size() {
		return super.size0();
	}

	@Override
	public boolean isEmpty() {
		return size() == 0;
	}

	@Override
	public boolean containsKey(Object key) {
		return this.client.getKvClient()
			.txnIf()
			.exists(withPrefixKey(Internal.asBytes(key)))
			.then()
			.noop()
			.sync()
			.getSucceeded();
	}

	@Override
	public boolean containsValue(Object value) {
		return values().contains(value);
	}

	@Override
	public V get(Object key) {
		try {
			return super.get0(key);
		}
		catch (Exception e) {
			throw new VertxException(e);
		}
	}

	@Override
	public V put(K key, V value) {
		try {
			return super.put0(key, value);
		}
		catch (Exception e) {
			throw new VertxException(e);
		}
	}

	@Override
	public V remove(Object key) {
		try {
			return super.remove0(key);
		}
		catch (Exception e) {
			throw new VertxException(e);
		}
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		m.forEach(this::put);
	}

	@Override
	public void clear() {
		super.clear0();
	}

	@Override
	public Set<K> keySet() {
		try {
			return super.keys0();
		}
		catch (Exception e) {
			throw new VertxException(e);
		}
	}

	@Override
	public Collection<V> values() {
		try {
			return super.values0();
		}
		catch (Exception e) {
			throw new VertxException(e);
		}
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		try {
			return super.entries0().entrySet();
		}
		catch (Exception e) {
			throw new VertxException(e);
		}
	}

}
