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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

import static org.apache.cassandra.db.tries.MemtableTrieTestBase.VERSION;
import static org.apache.cassandra.db.tries.MemtableTrieTestBase.generateKeys;

public class MemtableTrieConcurrentWriteTest
{
    private static final int COUNT = 1000000;
    private static final int OTHERS = COUNT / 10;
    private static final int PROGRESS_UPDATE = COUNT / 100;
    private static final int READERS = 8;
    private static final int WALKERS = 2;
    private static final int WRITERS = 4;
    Random rand = new Random();

    static String value(ByteComparable b)
    {
        return b.byteComparableAsString(VERSION);
    }

    AtomicIntegerArray writeProgress = new AtomicIntegerArray(WRITERS);
    AtomicIntegerArray writeCompleted = new AtomicIntegerArray(WRITERS);

    int getProgress()
    {
        int p = Integer.MAX_VALUE;
        for (int i = 0; i < WRITERS; ++i)
            p = Math.min(p, writeProgress.get(i));
        if (p == 0)
            return 0;
        return p - (p % WRITERS) + WRITERS - 1;
    }

    boolean writeCompleted()
    {
        int p = 1;
        for (int i = 0; i < WRITERS; ++i)
            p = Math.min(p, writeCompleted.get(i));
        return p > 0;
    }

    @Test
    public void testThreaded() throws InterruptedException
    {
        ByteComparable[] src = generateKeys(rand, COUNT + OTHERS);
        WritableTrie<String> trie = new ShardingMemtableTrie<>(BufferType.ON_HEAP);
//        WritableTrie<String> trie = new SynchronizedMemtableTrie<>(BufferType.ON_HEAP);
//        WritableTrie<String> trie = new MemtableTrie<>(BufferType.ON_HEAP);
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        List<Thread> threads = new ArrayList<Thread>();

        for (int i = 0; i < WALKERS; ++i)
            threads.add(new Thread()
            {
                public void run()
                {
                    try
                    {
                        Random r = ThreadLocalRandom.current();
                        while (!writeCompleted())
                        {
                            int min = getProgress();
                            int count = 0;
                            for (Map.Entry<ByteComparable, String> en : trie.asTrie().entrySet())
                            {
                                String v = value(en.getKey());
                                Assert.assertEquals(en.getKey()
                                                      .byteComparableAsString(
                                                      VERSION), v, en.getValue());
                                ++count;
                            }
                            Assert.assertTrue("Got only " + count + " while progress is at " + min, count >= min);
                        }
                    }
                    catch (Throwable t)
                    {
                        t.printStackTrace();
                        errors.add(t);
                    }
                }
            });

        for (int i = 0; i < READERS; ++i)
        {
            threads.add(new Thread()
            {
                public void run()
                {
                    try
                    {
                        Random r = ThreadLocalRandom.current();
                        while (!writeCompleted())
                        {
                            int min = getProgress();

                            for (int i = 0; i < PROGRESS_UPDATE; ++i)
                            {
                                int index = r.nextInt(COUNT + OTHERS);
                                ByteComparable b = src[index];
                                String v = value(b);
                                String result = trie.get(b);
                                if (result != null)
                                {
                                    Assert.assertTrue("Got not added " + index + " when COUNT is " + COUNT,
                                                      index < COUNT);
                                    Assert.assertEquals("Failed " + index, v, result);
                                }
                                else if (index < min)
                                    Assert.fail("Failed index " + index + " while progress is at " + min);
                            }
                        }
                    }
                    catch (Throwable t)
                    {
                        t.printStackTrace();
                        errors.add(t);
                    }
                }
            });
        }

        for (int i = 0; i < WRITERS; ++i)
        {
            final int base = i;
            threads.add(new Thread()
            {
                public void run()
                {
                    try
                    {
                        for (int i = base; i < COUNT; i += WRITERS)
                        {
                            ByteComparable b = src[i];

                            // Note: Because we don't ensure order when calling resolve, just use a hash of the key as payload
                            // (so that all sources have the same value).
                            String v = value(b);
//                            if (i % 2 == 0)
//                                trie.apply(Trie.singleton(b, v), (x, y) -> y);
//                            else
                                trie.putRecursive(b, v, (x, y) -> y);

                            if (i % PROGRESS_UPDATE == base)
                                writeProgress.set(base, i);
                        }
                        writeProgress.set(base, COUNT - 1);
                    }
                    catch (Throwable t)
                    {
                        t.printStackTrace();
                        errors.add(t);
                    }
                    finally
                    {
                        writeCompleted.set(base, 1);
                    }
                }
            });
        }

        for (Thread t : threads)
            t.start();

        while (!writeCompleted())
        {
            System.out.println("Progress at " + getProgress());
            Thread.sleep(100);
        }

        for (Thread t : threads)
            t.join();

        if (!errors.isEmpty())
            Assert.fail("Got errors:\n" + errors);
    }
}
