package io.vertx.spi.cluster.etcd.impl;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import io.fury.config.CompatibleMode;
import io.fury.config.Language;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.shareddata.ClusterSerializable;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.function.Supplier;

/**
 * Internal.
 *
 * @author <a href="mailto:snsyzf@qq.com">yzf</a>
 * @version 1.0
 */
public class Internal {

	public static final ByteString DELIMITER = com.google.protobuf.ByteString.copyFromUtf8("/");

	private static final io.fury.ThreadSafeFury FURY = io.fury.Fury.builder()
		.withCodegen(true)
		.registerGuavaTypes(true)
		.withCompatibleMode(CompatibleMode.COMPATIBLE)
		.withMetaContextShare(false)
		.withStringCompressed(true)
		.withLanguage(Language.JAVA)
		.requireClassRegistration(false)
		.withNumberCompressed(true)
		.ignoreBasicTypesRef(true)
		.withAsyncCompilation(true)
		.buildThreadSafeFury();

	public static ByteString concat(ByteString namespace, @NotNull ByteString key) {
		if (key.startsWith(DELIMITER)) {
			return namespace.concat(key);
		}
		return namespace.concat(DELIMITER).concat(key);
	}

	public static ByteString concat(ByteString namespace, String key) {
		return concat(namespace, ByteString.copyFromUtf8(key));
	}

	public static <T extends ClusterSerializable> @NotNull T asSerializableObject(@NotNull ByteString bytes,
			@NotNull Supplier<T> supplier) {
		Buffer buffer = Buffer.buffer(bytes.toByteArray());
		final var value = supplier.get();
		value.readFromBuffer(0, buffer);
		return value;
	}

	public static <T extends ClusterSerializable> @NotNull ByteString asSerializableBytes(@NotNull T value) {
		Buffer buffer = Buffer.buffer();
		value.writeToBuffer(buffer);
		return ByteString.copyFrom(buffer.getBytes());
	}

	@Contract("_ -> new")
	@VisibleForTesting
	public static @NotNull ByteString asBytes(Object object) {
		return ByteString.copyFrom(FURY.serialize(object));
	}

	@VisibleForTesting
	@SuppressWarnings("unchecked")
	public static <T> T asObject(ByteString bytes) {
		if (bytes == null || bytes.isEmpty())
			return null; // TTL
		return (T) FURY.deserialize(bytes.toByteArray());
	}

}
