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
package com.auto.demo.redis.core;

import com.auto.demo.redis.core.codec.RedisCodec;
import com.auto.demo.redis.core.internal.LettuceAssert;
import com.auto.demo.redis.core.output.*;
import com.auto.demo.redis.core.protocol.BaseRedisCommandBuilder;
import com.auto.demo.redis.core.protocol.Command;
import com.auto.demo.redis.core.protocol.CommandArgs;

import java.util.Map;

import static com.auto.demo.redis.core.protocol.CommandKeyword.*;
import static com.auto.demo.redis.core.protocol.CommandType.*;

/**
 * @param <K>
 * @param <V>
 * @author Mark Paluch
 * @author Zhang Jessey
 */
@SuppressWarnings({"unchecked", "varargs"})
public class RedisCommandBuilder<K, V> extends BaseRedisCommandBuilder<K, V> {

    private static final String MUST_NOT_CONTAIN_NULL_ELEMENTS = "must not contain null elements";

    private static final String MUST_NOT_BE_EMPTY = "must not be empty";

    private static final String MUST_NOT_BE_NULL = "must not be null";

    private static final byte[] MINUS_BYTES = {'-'};

    private static final byte[] PLUS_BYTES = {'+'};

    public RedisCommandBuilder(RedisCodec<K, V> codec) {
        super(codec);
    }

    Command<K, V, Long> append(K key, V value) {
        notNullKey(key);

        return createCommand(APPEND, new IntegerOutput<>(codec), key, value);
    }

    Command<K, V, String> asking() {

        CommandArgs<K, V> args = new CommandArgs<>(codec);
        return createCommand(ASKING, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> auth(String password) {
        LettuceAssert.notNull(password, "Password " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(password, "Password " + MUST_NOT_BE_EMPTY);

        return auth(password.toCharArray());
    }

    Command<K, V, String> auth(char[] password) {
        LettuceAssert.notNull(password, "Password " + MUST_NOT_BE_NULL);
        LettuceAssert.isTrue(password.length > 0, "Password " + MUST_NOT_BE_EMPTY);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(password);
        return createCommand(AUTH, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> bgrewriteaof() {
        return createCommand(BGREWRITEAOF, new StatusOutput<>(codec));
    }

    Command<K, V, String> bgsave() {
        return createCommand(BGSAVE, new StatusOutput<>(codec));
    }

    Command<K, V, Long> bitcount(K key) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key);
        return createCommand(BITCOUNT, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> bitcount(K key, long start, long end) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec);
        args.addKey(key).add(start).add(end);
        return createCommand(BITCOUNT, new IntegerOutput<>(codec), args);
    }


    Command<K, V, Long> bitopAnd(K destination, K... keys) {
        LettuceAssert.notNull(destination, "Destination " + MUST_NOT_BE_NULL);
        notEmpty(keys);

        CommandArgs<K, V> args = new CommandArgs<>(codec);
        args.add(AND).addKey(destination).addKeys(keys);
        return createCommand(BITOP, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> bitopNot(K destination, K source) {
        LettuceAssert.notNull(destination, "Destination " + MUST_NOT_BE_NULL);
        LettuceAssert.notNull(source, "Source " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec);
        args.add(NOT).addKey(destination).addKey(source);
        return createCommand(BITOP, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> bitopOr(K destination, K... keys) {
        LettuceAssert.notNull(destination, "Destination " + MUST_NOT_BE_NULL);
        notEmpty(keys);

        CommandArgs<K, V> args = new CommandArgs<>(codec);
        args.add(OR).addKey(destination).addKeys(keys);
        return createCommand(BITOP, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> bitopXor(K destination, K... keys) {
        LettuceAssert.notNull(destination, "Destination " + MUST_NOT_BE_NULL);
        notEmpty(keys);

        CommandArgs<K, V> args = new CommandArgs<>(codec);
        args.add(XOR).addKey(destination).addKeys(keys);
        return createCommand(BITOP, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> bitpos(K key, boolean state) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec);
        args.addKey(key).add(state ? 1 : 0);
        return createCommand(BITPOS, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> bitpos(K key, boolean state, long start) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec);
        args.addKey(key).add(state ? 1 : 0).add(start);
        return createCommand(BITPOS, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> bitpos(K key, boolean state, long start, long end) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec);
        args.addKey(key).add(state ? 1 : 0).add(start).add(end);
        return createCommand(BITPOS, new IntegerOutput<>(codec), args);
    }


    Command<K, V, V> brpoplpush(long timeout, K source, K destination) {
        LettuceAssert.notNull(source, "Source " + MUST_NOT_BE_NULL);
        LettuceAssert.notNull(destination, "Destination " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec);
        args.addKey(source).addKey(destination).add(timeout);
        return createCommand(BRPOPLPUSH, new ValueOutput<>(codec), args);
    }


    Command<K, V, String> clientKill(String addr) {
        LettuceAssert.notNull(addr, "Addr " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(addr, "Addr " + MUST_NOT_BE_EMPTY);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(KILL).add(addr);
        return createCommand(CLIENT, new StatusOutput<>(codec), args);
    }


    Command<K, V, String> clientList() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(LIST);
        return createCommand(CLIENT, new StatusOutput<>(codec), args);
    }

    Command<K, V, Long> clientId() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(ID);
        return createCommand(CLIENT, new IntegerOutput<>(codec), args);
    }

    Command<K, V, String> clientPause(long timeout) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(PAUSE).add(timeout);
        return createCommand(CLIENT, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> clientSetname(K name) {
        LettuceAssert.notNull(name, "Name " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(SETNAME).addKey(name);
        return createCommand(CLIENT, new StatusOutput<>(codec), args);
    }


    Command<K, V, String> clusterAddslots(int[] slots) {
        notEmptySlots(slots);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(ADDSLOTS);

        for (int slot : slots) {
            args.add(slot);
        }
        return createCommand(CLUSTER, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> clusterBumpepoch() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(BUMPEPOCH);
        return createCommand(CLUSTER, new StatusOutput<>(codec), args);
    }

    Command<K, V, Long> clusterCountFailureReports(String nodeId) {
        assertNodeId(nodeId);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add("COUNT-FAILURE-REPORTS").add(nodeId);
        return createCommand(CLUSTER, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> clusterCountKeysInSlot(int slot) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(COUNTKEYSINSLOT).add(slot);
        return createCommand(CLUSTER, new IntegerOutput<>(codec), args);
    }

    Command<K, V, String> clusterDelslots(int[] slots) {
        notEmptySlots(slots);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(DELSLOTS);

        for (int slot : slots) {
            args.add(slot);
        }
        return createCommand(CLUSTER, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> clusterFailover(boolean force) {

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(FAILOVER);
        if (force) {
            args.add(FORCE);
        }
        return createCommand(CLUSTER, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> clusterFlushslots() {

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(FLUSHSLOTS);
        return createCommand(CLUSTER, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> clusterForget(String nodeId) {
        assertNodeId(nodeId);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(FORGET).add(nodeId);
        return createCommand(CLUSTER, new StatusOutput<>(codec), args);
    }


    Command<K, V, String> clusterInfo() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(INFO);

        return createCommand(CLUSTER, new StatusOutput<>(codec), args);
    }

    Command<K, V, Long> clusterKeyslot(K key) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(KEYSLOT).addKey(key);
        return createCommand(CLUSTER, new IntegerOutput<>(codec), args);
    }

    Command<K, V, String> clusterMeet(String ip, int port) {
        LettuceAssert.notNull(ip, "IP " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(ip, "IP " + MUST_NOT_BE_EMPTY);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(MEET).add(ip).add(port);
        return createCommand(CLUSTER, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> clusterMyId() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(MYID);

        return createCommand(CLUSTER, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> clusterNodes() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(NODES);

        return createCommand(CLUSTER, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> clusterReplicate(String nodeId) {
        assertNodeId(nodeId);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(REPLICATE).add(nodeId);
        return createCommand(CLUSTER, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> clusterReset(boolean hard) {

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(RESET);
        if (hard) {
            args.add(HARD);
        } else {
            args.add(SOFT);
        }
        return createCommand(CLUSTER, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> clusterSaveconfig() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(SAVECONFIG);
        return createCommand(CLUSTER, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> clusterSetConfigEpoch(long configEpoch) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add("SET-CONFIG-EPOCH").add(configEpoch);
        return createCommand(CLUSTER, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> clusterSetSlotImporting(int slot, String nodeId) {
        assertNodeId(nodeId);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(SETSLOT).add(slot).add(IMPORTING).add(nodeId);
        return createCommand(CLUSTER, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> clusterSetSlotMigrating(int slot, String nodeId) {
        assertNodeId(nodeId);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(SETSLOT).add(slot).add(MIGRATING).add(nodeId);
        return createCommand(CLUSTER, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> clusterSetSlotNode(int slot, String nodeId) {
        assertNodeId(nodeId);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(SETSLOT).add(slot).add(NODE).add(nodeId);
        return createCommand(CLUSTER, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> clusterSetSlotStable(int slot) {

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(SETSLOT).add(slot).add(STABLE);
        return createCommand(CLUSTER, new StatusOutput<>(codec), args);
    }


    Command<K, V, Long> commandCount() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(COUNT);
        return createCommand(COMMAND, new IntegerOutput<>(codec), args);
    }


    Command<K, V, String> configResetstat() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(RESETSTAT);
        return createCommand(CONFIG, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> configRewrite() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(REWRITE);
        return createCommand(CONFIG, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> configSet(String parameter, String value) {
        LettuceAssert.notNull(parameter, "Parameter " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(parameter, "Parameter " + MUST_NOT_BE_EMPTY);
        LettuceAssert.notNull(value, "Value " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(SET).add(parameter).add(value);
        return createCommand(CONFIG, new StatusOutput<>(codec), args);
    }

    Command<K, V, Long> dbsize() {
        return createCommand(DBSIZE, new IntegerOutput<>(codec));
    }

    Command<K, V, String> debugCrashAndRecover(Long delay) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add("CRASH-AND-RECOVER");
        if (delay != null) {
            args.add(delay);
        }
        return createCommand(DEBUG, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> debugHtstats(int db) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(HTSTATS).add(db);
        return createCommand(DEBUG, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> debugObject(K key) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(OBJECT).addKey(key);
        return createCommand(DEBUG, new StatusOutput<>(codec), args);
    }

    Command<K, V, Void> debugOom() {
        return createCommand(DEBUG, null, new CommandArgs<>(codec).add("OOM"));
    }

    Command<K, V, String> debugReload() {
        return createCommand(DEBUG, new StatusOutput<>(codec), new CommandArgs<>(codec).add(RELOAD));
    }

    Command<K, V, String> debugRestart(Long delay) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(RESTART);
        if (delay != null) {
            args.add(delay);
        }
        return createCommand(DEBUG, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> debugSdslen(K key) {
        notNullKey(key);

        return createCommand(DEBUG, new StatusOutput<>(codec), new CommandArgs<>(codec).add("SDSLEN").addKey(key));
    }

    Command<K, V, Void> debugSegfault() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(SEGFAULT);
        return createCommand(DEBUG, null, args);
    }

    Command<K, V, Long> decr(K key) {
        notNullKey(key);

        return createCommand(DECR, new IntegerOutput<>(codec), key);
    }

    Command<K, V, Long> decrby(K key, long amount) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(amount);
        return createCommand(DECRBY, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> del(K... keys) {
        notEmpty(keys);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(keys);
        return createCommand(DEL, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> del(Iterable<K> keys) {
        LettuceAssert.notNull(keys, "Keys " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(keys);
        return createCommand(DEL, new IntegerOutput<>(codec), args);
    }

    Command<K, V, String> discard() {
        return createCommand(DISCARD, new StatusOutput<>(codec));
    }


    Command<K, V, V> echo(V msg) {
        LettuceAssert.notNull(msg, "message " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addValue(msg);
        return createCommand(ECHO, new ValueOutput<>(codec), args);
    }

    <T> Command<K, V, T> eval(String script, ScriptOutputType type, K... keys) {
        LettuceAssert.notNull(script, "Script " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(script, "Script " + MUST_NOT_BE_EMPTY);
        LettuceAssert.notNull(type, "ScriptOutputType " + MUST_NOT_BE_NULL);
        LettuceAssert.notNull(keys, "Keys " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec);
        args.add(script.getBytes()).add(keys.length).addKeys(keys);
        CommandOutput<K, V, T> output = newScriptOutput(codec, type);
        return createCommand(EVAL, output, args);
    }

    <T> Command<K, V, T> eval(String script, ScriptOutputType type, K[] keys, V... values) {
        LettuceAssert.notNull(script, "Script " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(script, "Script " + MUST_NOT_BE_EMPTY);
        LettuceAssert.notNull(type, "ScriptOutputType " + MUST_NOT_BE_NULL);
        LettuceAssert.notNull(keys, "Keys " + MUST_NOT_BE_NULL);
        LettuceAssert.notNull(values, "Values " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec);
        args.add(script.getBytes()).add(keys.length).addKeys(keys).addValues(values);
        CommandOutput<K, V, T> output = newScriptOutput(codec, type);
        return createCommand(EVAL, output, args);
    }

    <T> Command<K, V, T> evalsha(String digest, ScriptOutputType type, K... keys) {
        LettuceAssert.notNull(digest, "Digest " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(digest, "Digest " + MUST_NOT_BE_EMPTY);
        LettuceAssert.notNull(type, "ScriptOutputType " + MUST_NOT_BE_NULL);
        LettuceAssert.notNull(keys, "Keys " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec);
        args.add(digest).add(keys.length).addKeys(keys);
        CommandOutput<K, V, T> output = newScriptOutput(codec, type);
        return createCommand(EVALSHA, output, args);
    }

    <T> Command<K, V, T> evalsha(String digest, ScriptOutputType type, K[] keys, V... values) {
        LettuceAssert.notNull(digest, "Digest " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(digest, "Digest " + MUST_NOT_BE_EMPTY);
        LettuceAssert.notNull(type, "ScriptOutputType " + MUST_NOT_BE_NULL);
        LettuceAssert.notNull(keys, "Keys " + MUST_NOT_BE_NULL);
        LettuceAssert.notNull(values, "Values " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec);
        args.add(digest).add(keys.length).addKeys(keys).addValues(values);
        CommandOutput<K, V, T> output = newScriptOutput(codec, type);
        return createCommand(EVALSHA, output, args);
    }

    Command<K, V, Boolean> exists(K key) {
        notNullKey(key);

        return createCommand(EXISTS, new BooleanOutput<>(codec), key);
    }

    Command<K, V, Long> exists(K... keys) {
        notEmpty(keys);

        return createCommand(EXISTS, new IntegerOutput<>(codec), new CommandArgs<>(codec).addKeys(keys));
    }

    Command<K, V, Long> exists(Iterable<K> keys) {
        LettuceAssert.notNull(keys, "Keys " + MUST_NOT_BE_NULL);

        return createCommand(EXISTS, new IntegerOutput<>(codec), new CommandArgs<>(codec).addKeys(keys));
    }

    Command<K, V, Boolean> expire(K key, long seconds) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(seconds);
        return createCommand(EXPIRE, new BooleanOutput<>(codec), args);
    }

    Command<K, V, Boolean> expireat(K key, long timestamp) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(timestamp);
        return createCommand(EXPIREAT, new BooleanOutput<>(codec), args);
    }

    Command<K, V, String> flushall() {
        return createCommand(FLUSHALL, new StatusOutput<>(codec));
    }

    Command<K, V, String> flushallAsync() {
        return createCommand(FLUSHALL, new StatusOutput<>(codec), new CommandArgs<>(codec).add(ASYNC));
    }

    Command<K, V, String> flushdb() {
        return createCommand(FLUSHDB, new StatusOutput<>(codec));
    }

    Command<K, V, String> flushdbAsync() {
        return createCommand(FLUSHDB, new StatusOutput<>(codec), new CommandArgs<>(codec).add(ASYNC));
    }

    Command<K, V, Long> geoadd(K key, double longitude, double latitude, V member) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(longitude).add(latitude).addValue(member);
        return createCommand(GEOADD, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> geoadd(K key, Object[] lngLatMember) {

        notNullKey(key);
        LettuceAssert.notNull(lngLatMember, "LngLatMember " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(lngLatMember, "LngLatMember " + MUST_NOT_BE_EMPTY);
        LettuceAssert.noNullElements(lngLatMember, "LngLatMember " + MUST_NOT_CONTAIN_NULL_ELEMENTS);
        LettuceAssert.isTrue(lngLatMember.length % 3 == 0, "LngLatMember.length must be a multiple of 3 and contain a "
                + "sequence of longitude1, latitude1, member1, longitude2, latitude2, member2, ... longitudeN, latitudeN, memberN");

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key);

        for (int i = 0; i < lngLatMember.length; i += 3) {
            args.add((Double) lngLatMember[i]);
            args.add((Double) lngLatMember[i + 1]);
            args.addValue((V) lngLatMember[i + 2]);
        }

        return createCommand(GEOADD, new IntegerOutput<>(codec), args);
    }


    public Command<K, V, V> get(K key) {
        notNullKey(key);

        return createCommand(GET, new ValueOutput<>(codec), key);
    }

    Command<K, V, Long> getbit(K key, long offset) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(offset);
        return createCommand(GETBIT, new IntegerOutput<>(codec), args);
    }

    Command<K, V, V> getrange(K key, long start, long end) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(start).add(end);
        return createCommand(GETRANGE, new ValueOutput<>(codec), args);
    }

    Command<K, V, V> getset(K key, V value) {
        notNullKey(key);

        return createCommand(GETSET, new ValueOutput<>(codec), key, value);
    }

    Command<K, V, Long> hdel(K key, K... fields) {
        notNullKey(key);
        LettuceAssert.notNull(fields, "Fields " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(fields, "Fields " + MUST_NOT_BE_EMPTY);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).addKeys(fields);
        return createCommand(HDEL, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Boolean> hexists(K key, K field) {
        notNullKey(key);
        LettuceAssert.notNull(field, "Field " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).addKey(field);
        return createCommand(HEXISTS, new BooleanOutput<>(codec), args);
    }

    Command<K, V, V> hget(K key, K field) {
        notNullKey(key);
        LettuceAssert.notNull(field, "Field " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).addKey(field);
        return createCommand(HGET, new ValueOutput<>(codec), args);
    }

    Command<K, V, Map<K, V>> hgetall(K key) {
        notNullKey(key);

        return createCommand(HGETALL, new MapOutput<>(codec), key);
    }


    Command<K, V, Long> hincrby(K key, K field, long amount) {
        notNullKey(key);
        LettuceAssert.notNull(field, "Field " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).addKey(field).add(amount);
        return createCommand(HINCRBY, new IntegerOutput<>(codec), args);
    }


    Command<K, V, Long> hlen(K key) {
        notNullKey(key);

        return createCommand(HLEN, new IntegerOutput<>(codec), key);
    }


    Command<K, V, Boolean> hset(K key, K field, V value) {
        notNullKey(key);
        LettuceAssert.notNull(field, "Field " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).addKey(field).addValue(value);
        return createCommand(HSET, new BooleanOutput<>(codec), args);
    }

    Command<K, V, Long> hset(K key, Map<K, V> map) {
        notNullKey(key);
        LettuceAssert.notNull(map, "Map " + MUST_NOT_BE_NULL);
        LettuceAssert.isTrue(!map.isEmpty(), "Map " + MUST_NOT_BE_EMPTY);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(map);
        return createCommand(HSET, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Boolean> hsetnx(K key, K field, V value) {
        notNullKey(key);
        LettuceAssert.notNull(field, "Field " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).addKey(field).addValue(value);
        return createCommand(HSETNX, new BooleanOutput<>(codec), args);
    }

    Command<K, V, Long> hstrlen(K key, K field) {
        notNullKey(key);
        LettuceAssert.notNull(field, "Field " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).addKey(field);
        return createCommand(HSTRLEN, new IntegerOutput<>(codec), args);
    }


    Command<K, V, Long> incr(K key) {
        notNullKey(key);

        return createCommand(INCR, new IntegerOutput<>(codec), key);
    }

    Command<K, V, Long> incrby(K key, long amount) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(amount);
        return createCommand(INCRBY, new IntegerOutput<>(codec), args);
    }

    Command<K, V, String> info() {
        return createCommand(INFO, new StatusOutput<>(codec));
    }

    Command<K, V, String> info(String section) {
        LettuceAssert.notNull(section, "Section " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(section);
        return createCommand(INFO, new StatusOutput<>(codec), args);
    }


    Command<K, V, V> lindex(K key, long index) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(index);
        return createCommand(LINDEX, new ValueOutput<>(codec), args);
    }

    Command<K, V, Long> linsert(K key, boolean before, V pivot, V value) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec);
        args.addKey(key).add(before ? BEFORE : AFTER).addValue(pivot).addValue(value);
        return createCommand(LINSERT, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> llen(K key) {
        notNullKey(key);

        return createCommand(LLEN, new IntegerOutput<>(codec), key);
    }

    Command<K, V, V> lpop(K key) {
        notNullKey(key);

        return createCommand(LPOP, new ValueOutput<>(codec), key);
    }


    Command<K, V, Long> lpush(K key, V... values) {
        notNullKey(key);
        notEmptyValues(values);

        return createCommand(LPUSH, new IntegerOutput<>(codec), key, values);
    }

    Command<K, V, Long> lpushx(K key, V... values) {
        notNullKey(key);
        notEmptyValues(values);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).addValues(values);

        return createCommand(LPUSHX, new IntegerOutput<>(codec), args);
    }


    Command<K, V, Long> lrem(K key, long count, V value) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(count).addValue(value);
        return createCommand(LREM, new IntegerOutput<>(codec), args);
    }

    Command<K, V, String> lset(K key, long index, V value) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(index).addValue(value);
        return createCommand(LSET, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> ltrim(K key, long start, long stop) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(start).add(stop);
        return createCommand(LTRIM, new StatusOutput<>(codec), args);
    }

    Command<K, V, Long> memoryUsage(K key) {
        return createCommand(MEMORY, new IntegerOutput<>(codec), new CommandArgs<>(codec).add(USAGE).add(key.toString()));
    }

    Command<K, V, Boolean> move(K key, int db) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(db);
        return createCommand(MOVE, new BooleanOutput<>(codec), args);
    }

    Command<K, V, String> mset(Map<K, V> map) {
        LettuceAssert.notNull(map, "Map " + MUST_NOT_BE_NULL);
        LettuceAssert.isTrue(!map.isEmpty(), "Map " + MUST_NOT_BE_EMPTY);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(map);
        return createCommand(MSET, new StatusOutput<>(codec), args);
    }

    Command<K, V, Boolean> msetnx(Map<K, V> map) {
        LettuceAssert.notNull(map, "Map " + MUST_NOT_BE_NULL);
        LettuceAssert.isTrue(!map.isEmpty(), "Map " + MUST_NOT_BE_EMPTY);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(map);
        return createCommand(MSETNX, new BooleanOutput<>(codec), args);
    }

    Command<K, V, String> multi() {
        return createCommand(MULTI, new StatusOutput<>(codec));
    }

    Command<K, V, String> objectEncoding(K key) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(ENCODING).addKey(key);
        return createCommand(OBJECT, new StatusOutput<>(codec), args);
    }

    Command<K, V, Long> objectIdletime(K key) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(IDLETIME).addKey(key);
        return createCommand(OBJECT, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> objectRefcount(K key) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(REFCOUNT).addKey(key);
        return createCommand(OBJECT, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Boolean> persist(K key) {
        notNullKey(key);

        return createCommand(PERSIST, new BooleanOutput<>(codec), key);
    }

    Command<K, V, Boolean> pexpire(K key, long milliseconds) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(milliseconds);
        return createCommand(PEXPIRE, new BooleanOutput<>(codec), args);
    }

    Command<K, V, Boolean> pexpireat(K key, long timestamp) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(timestamp);
        return createCommand(PEXPIREAT, new BooleanOutput<>(codec), args);
    }

    Command<K, V, Long> pfadd(K key, V value, V... moreValues) {
        notNullKey(key);
        LettuceAssert.notNull(value, "Value " + MUST_NOT_BE_NULL);
        LettuceAssert.notNull(moreValues, "MoreValues " + MUST_NOT_BE_NULL);
        LettuceAssert.noNullElements(moreValues, "MoreValues " + MUST_NOT_CONTAIN_NULL_ELEMENTS);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).addValue(value).addValues(moreValues);
        return createCommand(PFADD, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> pfadd(K key, V... values) {
        notNullKey(key);
        notEmptyValues(values);
        LettuceAssert.noNullElements(values, "Values " + MUST_NOT_CONTAIN_NULL_ELEMENTS);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).addValues(values);
        return createCommand(PFADD, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> pfcount(K key, K... moreKeys) {
        notNullKey(key);
        LettuceAssert.notNull(moreKeys, "MoreKeys " + MUST_NOT_BE_NULL);
        LettuceAssert.noNullElements(moreKeys, "MoreKeys " + MUST_NOT_CONTAIN_NULL_ELEMENTS);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).addKeys(moreKeys);
        return createCommand(PFCOUNT, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> pfcount(K... keys) {
        notEmpty(keys);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(keys);
        return createCommand(PFCOUNT, new IntegerOutput<>(codec), args);
    }

    @SuppressWarnings("unchecked")
    Command<K, V, String> pfmerge(K destkey, K sourcekey, K... moreSourceKeys) {
        LettuceAssert.notNull(destkey, "Destkey " + MUST_NOT_BE_NULL);
        LettuceAssert.notNull(sourcekey, "Sourcekey " + MUST_NOT_BE_NULL);
        LettuceAssert.notNull(moreSourceKeys, "MoreSourceKeys " + MUST_NOT_BE_NULL);
        LettuceAssert.noNullElements(moreSourceKeys, "MoreSourceKeys " + MUST_NOT_CONTAIN_NULL_ELEMENTS);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(destkey).addKey(sourcekey).addKeys(moreSourceKeys);
        return createCommand(PFMERGE, new StatusOutput<>(codec), args);
    }

    @SuppressWarnings("unchecked")
    Command<K, V, String> pfmerge(K destkey, K... sourcekeys) {
        LettuceAssert.notNull(destkey, "Destkey " + MUST_NOT_BE_NULL);
        LettuceAssert.notNull(sourcekeys, "Sourcekeys " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(sourcekeys, "Sourcekeys " + MUST_NOT_BE_EMPTY);
        LettuceAssert.noNullElements(sourcekeys, "Sourcekeys " + MUST_NOT_CONTAIN_NULL_ELEMENTS);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(destkey).addKeys(sourcekeys);
        return createCommand(PFMERGE, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> ping() {
        return createCommand(PING, new StatusOutput<>(codec));
    }

    Command<K, V, String> psetex(K key, long milliseconds, V value) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(milliseconds).addValue(value);
        return createCommand(PSETEX, new StatusOutput<>(codec), args);
    }

    Command<K, V, Long> pttl(K key) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key);
        return createCommand(PTTL, new IntegerOutput<>(codec), args);
    }


    Command<K, V, Long> pubsubNumpat() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(NUMPAT);
        return createCommand(PUBSUB, new IntegerOutput<>(codec), args);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    Command<K, V, Map<K, Long>> pubsubNumsub(K... pattern) {
        LettuceAssert.notNull(pattern, "Pattern " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(pattern, "Pattern " + MUST_NOT_BE_EMPTY);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(NUMSUB).addKeys(pattern);
        return createCommand(PUBSUB, (MapOutput) new MapOutput<K, Long>((RedisCodec) codec), args);
    }

    Command<K, V, String> quit() {
        return createCommand(QUIT, new StatusOutput<>(codec));
    }

    Command<K, V, String> readOnly() {
        return createCommand(READONLY, new StatusOutput<>(codec));
    }

    Command<K, V, String> readWrite() {
        return createCommand(READWRITE, new StatusOutput<>(codec));
    }

    Command<K, V, String> rename(K key, K newKey) {
        notNullKey(key);
        LettuceAssert.notNull(newKey, "NewKey " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).addKey(newKey);
        return createCommand(RENAME, new StatusOutput<>(codec), args);
    }

    Command<K, V, Boolean> renamenx(K key, K newKey) {
        notNullKey(key);
        LettuceAssert.notNull(newKey, "NewKey " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).addKey(newKey);
        return createCommand(RENAMENX, new BooleanOutput<>(codec), args);
    }


    Command<K, V, V> rpop(K key) {
        notNullKey(key);

        return createCommand(RPOP, new ValueOutput<>(codec), key);
    }

    Command<K, V, V> rpoplpush(K source, K destination) {
        LettuceAssert.notNull(source, "Source " + MUST_NOT_BE_NULL);
        LettuceAssert.notNull(destination, "Destination " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(source).addKey(destination);
        return createCommand(RPOPLPUSH, new ValueOutput<>(codec), args);
    }

    Command<K, V, Long> rpush(K key, V... values) {
        notNullKey(key);
        notEmptyValues(values);

        return createCommand(RPUSH, new IntegerOutput<>(codec), key, values);
    }

    Command<K, V, Long> rpushx(K key, V... values) {
        notNullKey(key);
        notEmptyValues(values);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).addValues(values);
        return createCommand(RPUSHX, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> sadd(K key, V... members) {
        notNullKey(key);
        LettuceAssert.notNull(members, "Members " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(members, "Members " + MUST_NOT_BE_EMPTY);

        return createCommand(SADD, new IntegerOutput<>(codec), key, members);
    }

    Command<K, V, String> save() {
        return createCommand(SAVE, new StatusOutput<>(codec));
    }


    public Command<K, V, String> set(K key, V value) {
        notNullKey(key);

        return createCommand(SET, new StatusOutput<>(codec), key, value);
    }

    Command<K, V, String> set(K key, V value, SetArgs setArgs) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).addValue(value);
        setArgs.build(args);
        return createCommand(SET, new StatusOutput<>(codec), args);
    }

    Command<K, V, Long> setbit(K key, long offset, int value) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(offset).add(value);
        return createCommand(SETBIT, new IntegerOutput<>(codec), args);
    }

    Command<K, V, String> setex(K key, long seconds, V value) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(seconds).addValue(value);
        return createCommand(SETEX, new StatusOutput<>(codec), args);
    }

    Command<K, V, Boolean> setnx(K key, V value) {
        notNullKey(key);
        return createCommand(SETNX, new BooleanOutput<>(codec), key, value);
    }

    Command<K, V, Long> setrange(K key, long offset, V value) {
        notNullKey(key);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).add(offset).addValue(value);
        return createCommand(SETRANGE, new IntegerOutput<>(codec), args);
    }

    Command<K, V, String> shutdown(boolean save) {
        CommandArgs<K, V> args = new CommandArgs<>(codec);
        return createCommand(SHUTDOWN, new StatusOutput<>(codec), save ? args.add(SAVE) : args.add(NOSAVE));
    }


    Command<K, V, Long> sinterstore(K destination, K... keys) {
        LettuceAssert.notNull(destination, "Destination " + MUST_NOT_BE_NULL);
        notEmpty(keys);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(destination).addKeys(keys);
        return createCommand(SINTERSTORE, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Boolean> sismember(K key, V member) {
        notNullKey(key);
        return createCommand(SISMEMBER, new BooleanOutput<>(codec), key, member);
    }

    Command<K, V, String> slaveof(String host, int port) {
        LettuceAssert.notNull(host, "Host " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(host, "Host " + MUST_NOT_BE_EMPTY);

        CommandArgs<K, V> args = new CommandArgs<>(codec).add(host).add(port);
        return createCommand(SLAVEOF, new StatusOutput<>(codec), args);
    }

    Command<K, V, String> slaveofNoOne() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(NO).add(ONE);
        return createCommand(SLAVEOF, new StatusOutput<>(codec), args);
    }

    Command<K, V, Long> slowlogLen() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(LEN);
        return createCommand(SLOWLOG, new IntegerOutput<>(codec), args);
    }

    Command<K, V, String> slowlogReset() {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(RESET);
        return createCommand(SLOWLOG, new StatusOutput<>(codec), args);
    }


    Command<K, V, Long> touch(K... keys) {
        notEmpty(keys);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(keys);
        return createCommand(TOUCH, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> touch(Iterable<K> keys) {
        LettuceAssert.notNull(keys, "Keys " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(keys);
        return createCommand(TOUCH, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> ttl(K key) {
        notNullKey(key);

        return createCommand(TTL, new IntegerOutput<>(codec), key);
    }

    Command<K, V, String> type(K key) {
        notNullKey(key);

        return createCommand(TYPE, new StatusOutput<>(codec), key);
    }

    Command<K, V, Long> unlink(K... keys) {
        notEmpty(keys);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(keys);
        return createCommand(UNLINK, new IntegerOutput<>(codec), args);
    }

    Command<K, V, Long> unlink(Iterable<K> keys) {
        LettuceAssert.notNull(keys, "Keys " + MUST_NOT_BE_NULL);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(keys);
        return createCommand(UNLINK, new IntegerOutput<>(codec), args);
    }

    Command<K, V, String> unwatch() {
        return createCommand(UNWATCH, new StatusOutput<>(codec));
    }

    Command<K, V, Long> wait(int replicas, long timeout) {
        CommandArgs<K, V> args = new CommandArgs<>(codec).add(replicas).add(timeout);

        return createCommand(WAIT, new IntegerOutput<>(codec), args);
    }

    Command<K, V, String> watch(K... keys) {
        notEmpty(keys);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKeys(keys);
        return createCommand(WATCH, new StatusOutput<>(codec), args);
    }

    public Command<K, V, Long> xack(K key, K group, String[] messageIds) {
        notNullKey(key);
        LettuceAssert.notNull(group, "Group " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(messageIds, "MessageIds " + MUST_NOT_BE_EMPTY);
        LettuceAssert.noNullElements(messageIds, "MessageIds " + MUST_NOT_CONTAIN_NULL_ELEMENTS);

        CommandArgs<K, V> args = new CommandArgs<>(codec).addKey(key).addKey(group);

        for (String messageId : messageIds) {
            args.add(messageId);
        }

        return createCommand(XACK, new IntegerOutput<>(codec), args);
    }


    private boolean allElementsInstanceOf(Object[] objects, Class<?> expectedAssignableType) {

        for (Object object : objects) {
            if (!expectedAssignableType.isAssignableFrom(object.getClass())) {
                return false;
            }
        }

        return true;
    }


    static void notNullMinMax(String min, String max) {
        LettuceAssert.notNull(min, "Min " + MUST_NOT_BE_NULL);
        LettuceAssert.notNull(max, "Max " + MUST_NOT_BE_NULL);
    }


    private static void assertNodeId(String nodeId) {
        LettuceAssert.notNull(nodeId, "NodeId " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(nodeId, "NodeId " + MUST_NOT_BE_EMPTY);
    }


    private static void notEmpty(Object[] keys) {
        LettuceAssert.notNull(keys, "Keys " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(keys, "Keys " + MUST_NOT_BE_EMPTY);
    }

    private static void notEmptySlots(int[] slots) {
        LettuceAssert.notNull(slots, "Slots " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(slots, "Slots " + MUST_NOT_BE_EMPTY);
    }

    private static void notEmptyValues(Object[] values) {
        LettuceAssert.notNull(values, "Values " + MUST_NOT_BE_NULL);
        LettuceAssert.notEmpty(values, "Values " + MUST_NOT_BE_EMPTY);
    }

    private static void notNullKey(Object key) {
        LettuceAssert.notNull(key, "Key " + MUST_NOT_BE_NULL);
    }

}
