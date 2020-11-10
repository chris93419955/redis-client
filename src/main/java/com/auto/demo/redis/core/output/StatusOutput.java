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
package com.auto.demo.redis.core.output;

import com.auto.demo.redis.core.codec.RedisCodec;

import java.nio.ByteBuffer;

import static com.auto.demo.redis.core.protocol.LettuceCharsets.buffer;

/**
 * Status message output.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 * @author Will Glozer
 */
public class StatusOutput<K, V> extends CommandOutput<K, V, String> {

    private static final ByteBuffer OK = buffer("OK");

    public StatusOutput(RedisCodec<K, V> codec) {
        super(codec, null);
    }

    @Override
    public void set(ByteBuffer bytes) {
        output = OK.equals(bytes) ? "OK" : decodeAscii(bytes);
    }

}