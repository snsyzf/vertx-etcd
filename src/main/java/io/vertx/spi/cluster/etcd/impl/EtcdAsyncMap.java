package io.vertx.spi.cluster.etcd.impl;

import com.google.protobuf.ByteString;
import com.ibm.etcd.api.PutRequest;
import com.ibm.etcd.api.ResponseOp;
import com.ibm.etcd.client.EtcdClient;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.shareddata.AsyncMap;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * EtcdAsyncMap.
 *
 * @author <a href="mailto:snsyzf@qq.com">yzf</a>
 * @version 1.0
 */
public class EtcdAsyncMap<K, V> extends EtcdMap<K, V> implements AsyncMap<K, V> {

	public EtcdAsyncMap(EtcdClient client, Vertx vertx, ByteString prefix) {
		super(client, vertx, prefix);
	}

	@Override
	public Future<@io.vertx.codegen.annotations.Nullable V> get(K k) {
		return vertx.executeBlocking(() -> super.get0(k), false);
	}

	@Override
	public Future<Void> put(K k, V v) {
		return vertx.executeBlocking(() -> super.put0(k, v), false).mapEmpty();
	}

	@Override
	public Future<Void> put(K k, V v, long ttl) {
		if (ttl < 1000) {
			throw new IllegalArgumentException();
		}
		return this.vertx.executeBlocking(() -> {
			final var kk = withPrefixKey(Internal.asBytes(k));
			final var vv = Internal.asBytes(v);
			final var leaseId = this.client.getLeaseClient().grant(TimeUnit.MILLISECONDS.toSeconds(ttl)).sync().getID();

			return this.client.getKvClient().put(kk, vv, leaseId).sync();
		}, false).mapEmpty();
	}

	@Override
	public Future<@io.vertx.codegen.annotations.Nullable V> putIfAbsent(K k, V v) {
		return putIfAbsent(k, v, 0);
	}

	@Override
	public Future<@io.vertx.codegen.annotations.Nullable V> putIfAbsent(K k, V v, long ttl) {
		if (ttl != 0 && ttl < 1000) {
			throw new IllegalArgumentException();
		}
		return this.vertx.executeBlocking(() -> {
			final var kk = withPrefixKey(Internal.asBytes(k));
			final var vv = Internal.asBytes(v);

			final PutRequest request;
			if (ttl == 0) {
				request = this.client.getKvClient().put(kk, vv).asRequest();
			}
			else {
				final var id = this.client.getLeaseClient().grant(TimeUnit.MILLISECONDS.toSeconds(ttl)).sync().getID();
				request = this.client.getKvClient().put(kk, vv, id).asRequest();
			}

			final var response = this.client.getKvClient()
				.txnIf()
				.notExists(kk)
				.then()
				.put(request)
				.elseDo()
				.get(this.client.getKvClient().get(kk).asRequest())
				.sync();

			if (response.getSucceeded()) {
				for (ResponseOp op : response.getResponsesList()) {
					if (op.getResponseCase() == ResponseOp.ResponseCase.RESPONSE_PUT) {
						return null;
					}
					else if (op.getResponseCase() == ResponseOp.ResponseCase.RESPONSE_RANGE) {
						return Internal.asObject(op.getResponseRange().getKvs(0).getValue());
					}
				}
			}
			throw new VertxException("putIfAbsent exception");
		}, false);
	}

	@Override
	public Future<@io.vertx.codegen.annotations.Nullable V> remove(K k) {
		return vertx.executeBlocking(() -> super.remove0(k), false);
	}

	@Override
	public Future<Boolean> removeIfPresent(K k, V v) {
		return vertx.executeBlocking(() -> {
			final var kk = withPrefixKey(Internal.asBytes(k));

			return this.client.getKvClient()
				.txnIf()
				.exists(kk)
				.then()
				.delete(this.client.getKvClient().delete(kk).asRequest())
				.sync()
				.getSucceeded();
		}, false);
	}

	@Override
	public Future<@io.vertx.codegen.annotations.Nullable V> replace(K k, V v) {
		return vertx.executeBlocking(() -> super.put0(k, v), false);
	}

	@Override
	public Future<Boolean> replaceIfPresent(K k, V oldValue, V newValue) {
		return this.vertx.executeBlocking(() -> {
			final var kk = withPrefixKey(Internal.asBytes(k));
			final var ovv = Internal.asBytes(oldValue);
			final var nvv = Internal.asBytes(newValue);

			return this.client.getKvClient()
				.txnIf()
				.cmpEqual(kk)
				.value(ovv)
				.then()
				.put(this.client.getKvClient().put(kk, nvv).asRequest())
				.sync()
				.getSucceeded();
		}, false);
	}

	@Override
	public Future<Void> clear() {
		return vertx.executeBlocking(Executors.callable(super::clear0), false).mapEmpty();
	}

	@Override
	public Future<Integer> size() {
		return vertx.executeBlocking(super::size0, false);
	}

	@Override
	public Future<Set<K>> keys() {
		return vertx.executeBlocking(super::keys0, false);
	}

	@Override
	public Future<List<V>> values() {
		return vertx.executeBlocking(super::values0, false);
	}

	@Override
	public Future<Map<K, V>> entries() {
		return vertx.executeBlocking(super::entries0, false);
	}

}
