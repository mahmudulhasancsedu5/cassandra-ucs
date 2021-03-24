/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.tries;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Stream;

import com.google.common.base.Throwables;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.junit.Assert.assertEquals;

public class ShardingMemtableTrieTest
{
    private static final int COUNT = 100000;
    private static final int KEY_CHOICE = 25;
    private static final int MIN_LENGTH = 10;
    private static final int MAX_LENGTH = 50;
    Random rand = new Random();

    static final ByteComparable.Version VERSION = MemtableTrie.BYTE_COMPARABLE_VERSION;
    final boolean usePut = true;

    @Test
    public void testDirect()
    {
        ByteComparable[] src = generateKeys(rand, COUNT);
        SortedMap<ByteComparable, ByteBuffer> content = new TreeMap<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));
        WritableTrie<ByteBuffer> trie = makeWritableTrie(src, content, usePut);
        int keysize = Arrays.stream(src)
                            .mapToInt(src1 -> ByteComparable.length(src1, VERSION))
                            .sum();
        long ts = ObjectSizes.measureDeep(content);
        long onh = ObjectSizes.measureDeep(trie);
        System.out.format("Trie size on heap %,d off heap %,d measured %,d keys %,d treemap %,d\n",
                          trie.sizeOnHeap(), trie.sizeOffHeap(), onh, keysize, ts);
        System.out.format("per entry on heap %.2f off heap %.2f measured %.2f keys %.2f treemap %.2f\n",
                          trie.sizeOnHeap() * 1.0 / COUNT, trie.sizeOffHeap() * 1.0 / COUNT, onh * 1.0 / COUNT, keysize * 1.0 / COUNT, ts * 1.0 / COUNT);
        // System.out.println("Trie " + trie.dump(ByteBufferUtil::bytesToHex).get());

        assertSameContent(trie.asTrie(), content);
        checkGet(trie, content);
    }

    static WritableTrie<ByteBuffer> makeWritableTrie(ByteComparable[] src,
                                                     Map<ByteComparable, ByteBuffer> content,
                                                     boolean usePut)

    {
        WritableTrie<ByteBuffer> trie = new ShardingMemtableTrie<>(BufferType.OFF_HEAP);
        addToWritableTrie(src, content, trie, usePut);
        return trie;
    }

    static void addToWritableTrie(ByteComparable[] src,
                                  Map<ByteComparable, ByteBuffer> content,
                                  WritableTrie<ByteBuffer> trie,
                                  boolean usePut)

    {
        for (ByteComparable b : src)
        {
            // Note: Because we don't ensure order when calling resolve, just use a hash of the key as payload
            // (so that all sources have the same value).
            int payload = asString(b).hashCode();
            ByteBuffer v = ByteBufferUtil.bytes(payload);
            content.put(b, v);
//             System.out.println("Adding " + asString(b) + ": " + ByteBufferUtil.bytesToHex(v));
            putSimpleResolve(trie, b, v, (x, y) -> y, usePut);
//             System.out.println(trie.dump(ByteBufferUtil::bytesToHex));
        }
    }

    static void checkGet(WritableTrie<ByteBuffer> trie, Map<ByteComparable, ByteBuffer> items)
    {
        for (Map.Entry<ByteComparable, ByteBuffer> en : items.entrySet())
        {
            assertEquals(en.getValue(), trie.get(en.getKey()));
        }
    }

    static void assertSameContent(Trie<ByteBuffer> trie, SortedMap<ByteComparable, ByteBuffer> map)
    {
        assertMapEquals(trie, map);
        assertValuesEqual(trie, map);
        assertUnorderedValuesEqual(trie, map);
    }

    private static void assertValuesEqual(Trie<ByteBuffer> trie, SortedMap<ByteComparable, ByteBuffer> map)
    {
        assertIterablesEqual(trie.values(), map.values());
    }

    private static void assertUnorderedValuesEqual(Trie<ByteBuffer> trie, SortedMap<ByteComparable, ByteBuffer> map)
    {
        Multiset<ByteBuffer> unordered = HashMultiset.create();
        StringBuilder errors = new StringBuilder();
        for (ByteBuffer b : trie.valuesUnordered())
            unordered.add(b);

        for (ByteBuffer b : map.values())
            if (!unordered.remove(b))
                errors.append("\nMissing value in valuesUnordered: " + ByteBufferUtil.bytesToHex(b));

        for (ByteBuffer b : unordered)
            errors.append("\nExtra value in valuesUnordered: " + ByteBufferUtil.bytesToHex(b));

        assertEquals("", errors.toString());
    }

    static void assertMapEquals(Trie<ByteBuffer> trie, SortedMap<ByteComparable, ByteBuffer> map)
    {
        assertMapEquals(trie.entrySet(), map.entrySet());
    }

    static void assertMapEquals(Iterable<Map.Entry<ByteComparable, ByteBuffer>> container1,
                                Iterable<Map.Entry<ByteComparable, ByteBuffer>> container2)
    {
        Iterator<Map.Entry<ByteComparable, ByteBuffer>> it1 = container1.iterator();
        Iterator<Map.Entry<ByteComparable, ByteBuffer>> it2 = container2.iterator();
        List<ByteComparable> failedAt = new ArrayList<>();
        StringBuilder b = new StringBuilder();
        while (it1.hasNext() && it2.hasNext())
        {
            Map.Entry<ByteComparable, ByteBuffer> en1 = it1.next();
            Map.Entry<ByteComparable, ByteBuffer> en2 = it2.next();
            b.append(String.format("TreeSet %s:%s\n", asString(en2.getKey()), ByteBufferUtil.bytesToHex(en2.getValue())));
            b.append(String.format("Trie    %s:%s\n", asString(en1.getKey()), ByteBufferUtil.bytesToHex(en1.getValue())));
            if (ByteComparable.compare(en1.getKey(), en2.getKey(), VERSION) != 0 || ByteBufferUtil.compareUnsigned(en1.getValue(), en2.getValue()) != 0)
                failedAt.add(en1.getKey());
        }
        while (it1.hasNext())
        {
            Map.Entry<ByteComparable, ByteBuffer> en1 = it1.next();
            b.append(String.format("Trie    %s:%s\n", asString(en1.getKey()), ByteBufferUtil.bytesToHex(en1.getValue())));
            failedAt.add(en1.getKey());
        }
        while (it2.hasNext())
        {
            Map.Entry<ByteComparable, ByteBuffer> en2 = it2.next();
            b.append(String.format("TreeSet %s:%s\n", asString(en2.getKey()), ByteBufferUtil.bytesToHex(en2.getValue())));
            failedAt.add(en2.getKey());
        }
        if (!failedAt.isEmpty())
        {
            String message = "Failed at " + Lists.transform(failedAt, ShardingMemtableTrieTest::asString);
            System.err.println(message);
            System.err.println(b);
            Assert.fail(message);
        }
    }

    static <E extends Comparable<E>> void assertIterablesEqual(Iterable<E> expectedIterable, Iterable<E> actualIterable)
    {
        Iterator<E> expected = expectedIterable.iterator();
        Iterator<E> actual = actualIterable.iterator();
        while (actual.hasNext() && expected.hasNext())
        {
            Assert.assertEquals(actual.next(), expected.next());
        }
        if (expected.hasNext())
            Assert.fail("Remaing values in expected, starting with " + expected.next());
        else if (actual.hasNext())
            Assert.fail("Remaing values in actual, starting with " + actual.next());
    }

    static ByteComparable[] generateKeys(Random rand, int count)
    {
        ByteComparable[] sources = new ByteComparable[count];
        TreeSet<ByteComparable> added = new TreeSet<>((bytes1, bytes2) -> ByteComparable.compare(bytes1, bytes2, VERSION));
        for (int i = 0; i < count; ++i)
        {
            sources[i] = generateKey(rand);
            if (!added.add(sources[i]))
                --i;
        }

        // note: not sorted!
        return sources;
    }

    static ByteComparable generateKey(Random rand)
    {
        return generateKey(rand, MIN_LENGTH, MAX_LENGTH);
    }

    static ByteComparable generateKey(Random rand, int minLength, int maxLength)
    {
        int len = rand.nextInt(maxLength - minLength + 1) + minLength;
        byte[] bytes = new byte[len];
        int p = 0;
        int length = bytes.length;
        while (p < length)
        {
            int seed = rand.nextInt(KEY_CHOICE);
            Random r2 = new Random(seed);
            int m = r2.nextInt(5) + 2 + p;
            if (m > length)
                m = length;
            while (p < m)
                bytes[p++] = (byte) r2.nextInt(256);
        }
        return ByteComparable.fixedLength(bytes);
    }

    static String asString(ByteComparable bc)
    {
        return bc != null ? bc.byteComparableAsString(VERSION) : "null";
    }

    <T, M> void putSimpleResolve(WritableTrie<T> trie,
                                 ByteComparable key,
                                 T value,
                                 Trie.MergeResolver<T> resolver)
    {
        putSimpleResolve(trie, key, value, resolver, usePut);
    }

    static <T, M> void putSimpleResolve(WritableTrie<T> trie,
                                        ByteComparable key,
                                        T value,
                                        Trie.MergeResolver<T> resolver,
                                        boolean usePut)
    {
        try
        {
            trie.putSingleton(key,
                              value,
                              (existing, update) -> existing != null ? resolver.resolve(existing, update) : update,
                              usePut);
        }
        catch (MemtableTrie.SpaceExhaustedException e)
        {
            throw Throwables.propagate(e);
        }
    }
}
