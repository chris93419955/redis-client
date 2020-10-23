/*
 * Copyright 2011-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.auto.demo.redis.core.codec;

import java.nio.ByteBuffer;

/**
 * A {@link RedisCodec} encodes keys and values sent to Redis, and decodes keys and values in the command output.
 *
 * The methods are called by multiple threads and must be thread-safe.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 * @author Will Glozer
 * @author Mark Paluch
 * @author Dimitris Mandalidis
 */
public interface RedisCodec<K, V> {

    /**
     * Decode the key output by redis.
     *
     * @param bytes Raw bytes of the key, must not be {@code null}.
     * @return The decoded key, may be {@code null}.
     */
    K decodeKey(ByteBuffer bytes);

    /**
     * Decode the value output by redis.
     *
     * @param bytes Raw bytes of the value, must not be {@code null}.
     * @return The decoded value, may be {@code null}.
     */
    V decodeValue(ByteBuffer bytes);

    /**
     * Encode the key for output to redis.
     *
     * @param key the key, may be {@code null}.
     * @return The encoded key, never {@code null}.
     */
    ByteBuffer encodeKey(K key);

    /**
     * Encode the value for output to redis.
     *
     * @param value the value, may be {@code null}.
     * @return The encoded value, never {@code null}.
     */
    ByteBuffer encodeValue(V value);

}
