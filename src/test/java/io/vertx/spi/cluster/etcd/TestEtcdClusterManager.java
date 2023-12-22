package io.vertx.spi.cluster.etcd;

import com.ibm.etcd.client.EtcdClient;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.VertxException;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.Counter;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * TestEtcdClusterManager.
 *
 * @author <a href="mailto:snsyzf@qq.com">yzf</a>
 * @version 1.0
 */
public class TestEtcdClusterManager {

	private static Vertx vertx;

	@BeforeAll
	public static void init() {
		EtcdClient client = EtcdClient.forEndpoints("http://localhost:2379").withPlainText().build();
		vertx = await(Vertx.builder()
			.withClusterManager(
					new EtcdClusterManager(client, "/test" + System.currentTimeMillis(), UUID.randomUUID().toString()))
			.buildClustered());
	}

	@org.junit.jupiter.api.Test
	public void testCounter() {
		Counter counter = await(vertx.sharedData().getCounter("counter1"));

		assertEquals(0, await(counter.get()));
		assertEquals(1, await(counter.incrementAndGet()));
		assertEquals(1, await(counter.getAndIncrement()));
		assertEquals(2, await(counter.get()));
		assertEquals(3, await(counter.addAndGet(1)));
		assertEquals(3, await(counter.getAndAdd(1)));
		assertEquals(4, await(counter.get()));
		assertEquals(3, await(counter.decrementAndGet()));
		assertTrue(await(counter.compareAndSet(3, 5)));
	}

	@Test
	public void testAsyncMap() {
		AsyncMap<Integer, Integer> map = await(vertx.sharedData().getAsyncMap("asyncmap1"));

		assertNull(await(map.put(1, 1)));
		assertEquals(1, await(map.get(1)));
		assertThrows(VertxException.class, () -> await(map.putIfAbsent(1, 1)));
		assertNull(await(map.clear()));
		assertNull(await(map.get(1)));
		assertNull(await(map.replace(1, 1)));
		assertTrue(await(map.replaceIfPresent(1, 1, 2)));
		assertNull(await(map.put(2, 2)));
		assertEquals(2, await(map.size()));
		assertEquals(Set.of(1, 2), await(map.keys()));
		assertEquals(List.of(2, 2), await(map.values()));
		assertEquals(Map.of(1, 2, 2, 2), await(map.entries()));
	}

	private static <T> T await(Future<T> future) {
		try {
			return future.toCompletionStage().toCompletableFuture().get();
		}
		catch (Exception e) {
			throw new VertxException(e);
		}
	}

}
