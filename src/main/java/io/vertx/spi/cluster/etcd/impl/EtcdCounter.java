package io.vertx.spi.cluster.etcd.impl;

import com.google.protobuf.ByteString;
import com.ibm.etcd.client.EtcdClient;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.shareddata.Counter;

import static com.google.common.primitives.Longs.fromByteArray;
import static com.google.common.primitives.Longs.toByteArray;

/**
 * EtcdCounter.
 *
 * @author <a href="mailto:snsyzf@qq.com">yzf</a>
 * @version 1.0
 */
public class EtcdCounter implements Counter {

	private final ByteString key;

	private final EtcdClient client;

	private final Vertx vertx;

	public EtcdCounter(EtcdClient client, Vertx vertx, ByteString key) {
		this.client = client;
		this.vertx = vertx;
		this.key = key;
	}

	@Override
	public Future<Long> get() {
		return this.vertx.executeBlocking(() -> {
			final var response = this.client.getKvClient().get(this.key).serializable(true).sync();
			if (response.getKvsList().isEmpty()) {
				return 0L;
			}
			return fromByteArray(response.getKvs(0).getValue().toByteArray());
		}, false);
	}

	@Override
	public Future<Long> incrementAndGet() {
		return addAndGet(1);
	}

	@Override
	public Future<Long> getAndIncrement() {
		return addAndGet(1).map(it -> it - 1);
	}

	@Override
	public Future<Long> decrementAndGet() {
		return addAndGet(-1);
	}

	@Override
	public Future<Long> addAndGet(long value) {

		return get().compose(cur -> {
			if (cur == 0L) {
				return this.vertx.executeBlocking(() -> {
					final var response = this.client.getKvClient()
						.txnIf()
						.notExists(this.key)
						.then()
						.put(this.client.getKvClient()
							.put(this.key, ByteString.copyFrom(toByteArray(value)))
							.asRequest())
						.sync();
					if (response.getSucceeded()) {
						return value;
					}
					throw new VertxException("if not exists set value exception");
				}, false);
			}
			else {
				return this.vertx.executeBlocking(() -> {
					final var response = this.client.getKvClient()
						.txnIf()
						.cmpEqual(this.key)
						.value(ByteString.copyFrom(toByteArray(cur)))
						.then()
						.put(this.client.getKvClient()
							.put(this.key, ByteString.copyFrom(toByteArray(cur + value)))
							.asRequest())
						.sync();
					if (response.getSucceeded()) {
						return cur + value;
					}
					throw new VertxException("if value exists update value exception");
				}, false);
			}
		});
	}

	@Override
	public Future<Long> getAndAdd(long value) {
		return addAndGet(value).map(it -> it - value);
	}

	@Override
	public Future<Boolean> compareAndSet(long expected, long value) {
		return this.vertx.executeBlocking(() -> this.client.getKvClient()
			.txnIf()
			.cmpEqual(this.key)
			.value(ByteString.copyFrom(toByteArray(expected)))
			.then()
			.put(this.client.getKvClient().put(this.key, ByteString.copyFrom(toByteArray(value))).asRequest())
			.sync()
			.getSucceeded(), false);
	}

}
