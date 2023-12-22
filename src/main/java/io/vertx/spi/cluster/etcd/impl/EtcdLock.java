package io.vertx.spi.cluster.etcd.impl;

import com.google.protobuf.ByteString;
import com.ibm.etcd.client.EtcdClient;
import io.vertx.core.Vertx;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.shareddata.Lock;

import java.util.concurrent.Executor;

/**
 * EtcdLock.
 *
 * @author <a href="mailto:snsyzf@qq.com">yzf</a>
 * @version 1.0
 */
public class EtcdLock implements Lock {

	private static final Logger log = LoggerFactory.getLogger(EtcdLock.class);

	private final EtcdClient client;

	private final Vertx vertx;

	private final ByteString key;

	private final long timeout;

	private long timer;

	private long leaseId;

	private volatile ByteString lockingKey;

	private final Executor lockReleaseExec;

	public EtcdLock(EtcdClient client, Vertx vertx, ByteString key, long timeout, Executor lockReleaseExec) {
		this.client = client;
		this.key = key;
		this.vertx = vertx;
		this.timeout = timeout;
		this.lockReleaseExec = lockReleaseExec;
	}

	public void lock() {
		try {
			this.leaseId = this.client.getLeaseClient().grant(this.timeout).timeout(this.timeout).sync().getID();
			this.timer = this.vertx.setPeriodic(1000, id -> this.client.getLeaseClient().keepAliveOnce(leaseId));
			this.lockingKey = this.client.getLockClient().lock(this.key).withLease(leaseId).sync().getKey();
		}
		catch (Exception e) {
			revoke();
			throw e;
		}
	}

	private void revoke() {
		if (this.leaseId != 0) {
			try {
				this.client.getLeaseClient().revoke(this.leaseId).get();
			}
			catch (Exception e) {
				log.error(e);
			}
		}
	}

	private void unlock() {
		if (this.lockingKey != null) {
			try {
				this.client.getLockClient().unlock(this.lockingKey).sync();
			}
			catch (Exception e) {
				log.error(e);
			}
		}
	}

	@Override
	public void release() {
		this.vertx.cancelTimer(this.timer);

		this.lockReleaseExec.execute(() -> {
			revoke();
			unlock();
		});
	}

}
