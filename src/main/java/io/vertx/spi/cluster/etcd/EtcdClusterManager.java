package io.vertx.spi.cluster.etcd;

import com.google.common.base.VerifyException;
import com.google.protobuf.ByteString;
import com.ibm.etcd.api.Event;
import com.ibm.etcd.api.KeyValue;
import com.ibm.etcd.client.EtcdClient;
import com.ibm.etcd.client.kv.KvClient;
import com.ibm.etcd.client.kv.WatchUpdate;
import io.grpc.stub.StreamObserver;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import io.vertx.core.shareddata.Lock;
import io.vertx.core.spi.cluster.*;
import io.vertx.spi.cluster.etcd.impl.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * EtcdClusterManager.
 *
 * @author <a href="mailto:snsyzf@qq.com">yzf</a>
 * @version 1.0
 */
public class EtcdClusterManager implements ClusterManager {

	private static final Logger log = LoggerFactory.getLogger(EtcdClusterManager.class);

	private final Map<String, EtcdLock> locks = new ConcurrentHashMap<>();

	private final Map<String, EtcdCounter> counters = new ConcurrentHashMap<>();

	private final Map<String, Map<?, ?>> syncMaps = new ConcurrentHashMap<>();

	private final Map<String, AsyncMap<?, ?>> asyncMaps = new ConcurrentHashMap<>();

	private final ByteString nodePrefix;

	private final ByteString subsPrefix;

	private final ByteString syncMapPrefix;

	private final ByteString asyncMapPrefix;

	private final ByteString counterPrefix;

	private final ByteString lockPrefix;

	private Vertx vertx;

	private NodeSelector nodeSelector;

	private NodeListener nodeListener;

	private NodeInfo nodeInfo;

	private final EtcdClient client;

	private ExecutorService lockReleaseExec;

	private String nodeId;

	private KvClient.Watch watcher;

	private volatile boolean active;

	private volatile long timer;

	private volatile long leaseId;

	public EtcdClusterManager(EtcdClient client, String namespace, String nodeId) {
		this.client = client;
		this.nodeId = nodeId;

		ByteString namespaceBytes;
		if (!namespace.isEmpty()) {
			if (namespace.charAt(0) != '/' || namespace.charAt(namespace.length() - 1) == '/') {
				throw new IllegalArgumentException();
			}
			namespaceBytes = ByteString.copyFromUtf8(namespace);
		}
		else {
			namespaceBytes = ByteString.empty();
		}

		this.nodePrefix = namespaceBytes.concat(ByteString.copyFromUtf8("/vertx/nodes"));
		this.subsPrefix = namespaceBytes.concat(ByteString.copyFromUtf8("/vertx/subs"));
		this.syncMapPrefix = namespaceBytes.concat(ByteString.copyFromUtf8("/vertx/syncMaps"));
		this.asyncMapPrefix = namespaceBytes.concat(ByteString.copyFromUtf8("/vertx/asyncMaps"));
		this.counterPrefix = namespaceBytes.concat(ByteString.copyFromUtf8("/vertx/counters"));
		this.lockPrefix = namespaceBytes.concat(ByteString.copyFromUtf8("/vertx/locks"));
	}

	public EtcdClient getClient() {
		return this.client;
	}

	@Override
	public void init(Vertx vertx, NodeSelector nodeSelector) {
		this.vertx = vertx;
		this.nodeSelector = nodeSelector;
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K, V> void getAsyncMap(String name, Promise<AsyncMap<K, V>> promise) {
		this.vertx.executeBlocking(
				() -> (AsyncMap<K, V>) this.asyncMaps.computeIfAbsent(name,
						x -> new EtcdAsyncMap<>(this.client, this.vertx, Internal.concat(this.asyncMapPrefix, name))),
				false, promise);
	}

	@SuppressWarnings("unchecked")
	@Override
	public <K, V> Map<K, V> getSyncMap(String name) {
		return (Map<K, V>) this.syncMaps.computeIfAbsent(name,
				x -> new EtcdSyncMap<>(this.client, this.vertx, Internal.concat(this.syncMapPrefix, name)));
	}

	@Override
	public void getLockWithTimeout(String name, long timeout, Promise<Lock> promise) {
		this.vertx.executeBlocking(() -> {
			EtcdLock lock = this.locks.get(name);
			if (lock == null) {
				lock = new EtcdLock(this.client, this.vertx, Internal.concat(this.lockPrefix, name), timeout,
						this.lockReleaseExec);
			}
			try {
				lock.lock();
				this.locks.put(name, lock);
				return lock;
			}
			catch (Exception e) {
				throw new VerifyException("get lock exception", e);
			}
		}, false, promise);
	}

	@Override
	public void getCounter(String name, Promise<Counter> promise) {
		this.vertx.executeBlocking(
				() -> this.counters.computeIfAbsent(name,
						x -> new EtcdCounter(this.client, this.vertx, Internal.concat(this.counterPrefix, name))),
				false, promise);
	}

	@Override
	public String getNodeId() {
		return this.nodeId;
	}

	@Override
	public List<String> getNodes() {
		return null;
	}

	@Override
	public void nodeListener(NodeListener listener) {
		this.nodeListener = listener;
	}

	@Override
	public void setNodeInfo(NodeInfo nodeInfo, Promise<Void> promise) {
		synchronized (this) {
			this.nodeInfo = nodeInfo;
		}

		try {
			Buffer buffer = Buffer.buffer();
			nodeInfo.writeToBuffer(buffer);

			this.vertx
				.executeBlocking(() -> this.client.getKvClient()
					.put(Internal.concat(this.nodePrefix, nodeId), ByteString.copyFrom(buffer.getBytes()))
					.sync(), false)
				.<Void>mapEmpty()
				.onComplete(promise);
		}
		catch (Exception e) {
			log.error("create node failed. ", e);
		}
	}

	@Override
	public NodeInfo getNodeInfo() {
		synchronized (this) {
			return this.nodeInfo;
		}
	}

	@Override
	public void getNodeInfo(String nodeId, Promise<NodeInfo> promise) {
		this.vertx.executeBlocking(() -> {
			final var response = this.client.getKvClient()
				.get(Internal.concat(this.nodePrefix, nodeId))
				.serializable(true)
				.sync();
			if (response.getKvsList().isEmpty()) {
				throw new VertxException("Not found node info by " + nodeId);
			}
			Buffer buffer = Buffer.buffer(response.getKvs(0).getValue().toByteArray());
			NodeInfo info = new NodeInfo();
			info.readFromBuffer(0, buffer);
			return info;
		}, false);
	}

	@Override
	public void join(Promise<Void> promise) {
		this.vertx.executeBlocking(() -> {
			if (!this.active) {
				this.active = true;

				this.lockReleaseExec = Executors
					.newCachedThreadPool(r -> new Thread(r, "vertx-etcd-service-release-lock-thread"));

				if (this.nodeId == null) {
					this.nodeId = UUID.randomUUID().toString();
				}

				this.leaseId = this.client.getLeaseClient().grant(10).sync().getID();

				this.client.getKvClient()
					.put(Internal.concat(this.nodePrefix, this.nodeId), ByteString.empty(), this.leaseId)
					.sync();

				this.timer = this.vertx.setPeriodic(1000, x -> keepAliveLease());

				this.watcher = this.client.getKvClient()
					.watch(this.nodePrefix)
					.asPrefix()
					.progressNotify()
					.prevKv()
					.start(new StreamObserver<>() {
						@Override
						public void onNext(WatchUpdate value) {

							for (Event event : value.getEvents()) {
								if (event.getType() == Event.EventType.PUT) {
									if (nodeListener != null) {
										nodeListener.nodeAdded(event.getKv()
											.getKey()
											.substring(nodePrefix.size() + Internal.DELIMITER.size() + 1)
											.toStringUtf8());
									}
								}
								else if (event.getType() == Event.EventType.DELETE) {
									if (nodeListener != null) {
										nodeListener.nodeLeft(event.getPrevKv()
											.getKey()
											.substring(nodePrefix.size() + Internal.DELIMITER.size() + 1)
											.toStringUtf8());
									}
								}
							}

						}

						@Override
						public void onError(Throwable t) {
							log.error("watch nodes exception", t);
						}

						@Override
						public void onCompleted() {
							log.info("watch nodes completed");
						}
					});

				if (this.nodeListener != null) {
					final var response = this.client.getKvClient()
						.get(this.nodePrefix)
						.asPrefix()
						.serializable(true)
						.keysOnly()
						.sync();
					for (KeyValue kv : response.getKvsList()) {
						this.nodeListener.nodeAdded(kv.getKey()
							.substring(this.nodePrefix.size() + Internal.DELIMITER.size() + 1)
							.toStringUtf8());
					}
				}
			}
			return null;
		}, false, promise);
	}

	@Override
	public void leave(Promise<Void> promise) {
		this.vertx.executeBlocking(Executors.callable(() -> {
			synchronized (EtcdClusterManager.this) {
				if (this.active) {
					this.active = false;
					this.vertx.cancelTimer(this.timer);

					this.lockReleaseExec.shutdown();

					try {
						this.client.getLeaseClient().revoke(this.leaseId).get();
					}
					catch (Exception e) {
						log.warn("revoke lease exception");
					}

					try {

						this.watcher.close();
						this.client.close();
					}
					catch (Exception e) {
						log.warn("close etcd client exception", e);
					}
				}
			}
		}, null), false, promise);
	}

	@Override
	public boolean isActive() {
		return this.active;
	}

	@Override
	public void addRegistration(String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
		this.vertx.executeBlocking(Executors.callable(() -> {
			final var key = this.subsPrefix.concat(Internal.DELIMITER)
				.concat(ByteString.copyFromUtf8(address))
				.concat(Internal.DELIMITER)
				.concat(ByteString.copyFromUtf8(registrationInfo.nodeId() + "-" + registrationInfo.seq()));

			this.client.getKvClient().put(key, Internal.asSerializableBytes(registrationInfo), leaseId).sync();

			if (this.nodeSelector != null) {
				this.nodeSelector.registrationsUpdated(new RegistrationUpdateEvent(address, getRegistrations(address)));
			}
		})).<Void>mapEmpty().onComplete(promise);
	}

	@Override
	public void removeRegistration(String address, RegistrationInfo registrationInfo, Promise<Void> promise) {
		this.vertx.executeBlocking(Executors.callable(() -> {
			final var key = this.subsPrefix.concat(Internal.DELIMITER)
				.concat(ByteString.copyFromUtf8(address))
				.concat(Internal.DELIMITER)
				.concat(ByteString.copyFromUtf8(registrationInfo.nodeId() + "-" + registrationInfo.seq()));

			final var response = this.client.getKvClient()
				.txnIf()
				.cmpEqual(key)
				.value(Internal.asSerializableBytes(registrationInfo))
				.then()
				.delete(this.client.getKvClient().delete(key).asRequest())
				.sync();
			if (response.getSucceeded()) {
				if (this.nodeSelector != null) {
					this.nodeSelector
						.registrationsUpdated(new RegistrationUpdateEvent(address, getRegistrations(address)));
				}
			}
		})).<Void>mapEmpty().onComplete(promise);
	}

	@Override
	public void getRegistrations(String address, Promise<List<RegistrationInfo>> promise) {
		this.vertx.executeBlocking(() -> getRegistrations(address), false).onComplete(promise);
	}

	private List<RegistrationInfo> getRegistrations(String address) {
		final var response = this.client.getKvClient()
			.get(this.subsPrefix.concat(Internal.DELIMITER).concat(ByteString.copyFromUtf8(address)))
			.asPrefix()
			.serializable(true)
			.sync();

		if (response.getKvsList().isEmpty()) {
			return List.of();
		}
		List<RegistrationInfo> list = new ArrayList<>(response.getKvsCount());
		for (int i = 0; i < response.getKvsCount(); i++) {
			list.add(Internal.asSerializableObject(response.getKvs(i).getValue(), RegistrationInfo::new));
		}
		return list;
	}

	private void keepAliveLease() {
		this.client.getLeaseClient().keepAliveOnce(this.leaseId);
	}

}
