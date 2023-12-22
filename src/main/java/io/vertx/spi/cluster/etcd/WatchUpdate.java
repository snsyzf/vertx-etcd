package io.vertx.spi.cluster.etcd;

import java.util.Objects;

/**
 * WatchUpdate.
 *
 * @author <a href="mailto:snsyzf@qq.com">yzf</a>
 * @version 1.0
 */
public record WatchUpdate<K, V>(WatchEvent event, K key, V value) {

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		WatchUpdate<?, ?> that = (WatchUpdate<?, ?>) o;
		return event == that.event && Objects.equals(key, that.key) && Objects.equals(value, that.value);
	}

	@Override
	public String toString() {
		return "WatchUpdate{" + "event=" + event + ", key=" + key + ", value=" + value + '}';
	}

}
