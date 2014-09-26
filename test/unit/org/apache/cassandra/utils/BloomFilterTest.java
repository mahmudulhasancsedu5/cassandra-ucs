/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;
import java.util.Set;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputStreamAndChannel;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.IFilter.FilterKey;
import org.apache.cassandra.utils.KeyGenerator.RandomStringGenerator;

public class BloomFilterTest
{
    public IFilter bf;

    public BloomFilterTest()
    {
        bf = FilterFactory.getFilter(10000L, FilterTestHelper.MAX_FAILURE_RATE, true);
    }

    public static IFilter testSerialize(IFilter f) throws IOException
    {
        f.add(FilterTestHelper.bytes("a"));
        DataOutputBuffer out = new DataOutputBuffer();
        FilterFactory.serialize(f, out);

        ByteArrayInputStream in = new ByteArrayInputStream(out.getData(), 0, out.getLength());
        IFilter f2 = FilterFactory.deserialize(new DataInputStream(in), true);

        assert f2.isPresent(FilterTestHelper.bytes("a"));
        assert !f2.isPresent(FilterTestHelper.bytes("b"));
        return f2;
    }


    @Before
    public void clear()
    {
        bf.clear();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testBloomLimits1()
    {
        int maxBuckets = BloomCalculations.probs.length - 1;
        int maxK = BloomCalculations.probs[maxBuckets].length - 1;

        // possible
        BloomCalculations.computeBloomSpec(maxBuckets, BloomCalculations.probs[maxBuckets][maxK]);

        // impossible, throws
        BloomCalculations.computeBloomSpec(maxBuckets, BloomCalculations.probs[maxBuckets][maxK] / 2);
    }

    @Test
    public void testOne()
    {
        bf.add(FilterTestHelper.bytes("a"));
        assert bf.isPresent(FilterTestHelper.bytes("a"));
        assert !bf.isPresent(FilterTestHelper.bytes("b"));
    }

    @Test
    public void testFalsePositivesInt()
    {
        FilterTestHelper.testFalsePositives(bf, FilterTestHelper.intKeys(), FilterTestHelper.randomKeys2());
    }

    @Test
    public void testFalsePositivesRandom()
    {
        FilterTestHelper.testFalsePositives(bf, FilterTestHelper.randomKeys(), FilterTestHelper.randomKeys2());
    }

    @Test
    public void testWords()
    {
        if (KeyGenerator.WordGenerator.WORDS == 0)
        {
            return;
        }
        IFilter bf2 = FilterFactory.getFilter(KeyGenerator.WordGenerator.WORDS / 2, FilterTestHelper.MAX_FAILURE_RATE, true);
        int skipEven = KeyGenerator.WordGenerator.WORDS % 2 == 0 ? 0 : 2;
        FilterTestHelper.testFalsePositives(bf2,
                                            new KeyGenerator.WordGenerator(skipEven, 2),
                                            new KeyGenerator.WordGenerator(1, 2));
    }

    @Test
    public void testSerialize() throws IOException
    {
        BloomFilterTest.testSerialize(bf);
    }

    public void testManyHashes(Iterator<ByteBuffer> keys)
    {
        int MAX_HASH_COUNT = 128;
        Set<Long> hashes = new HashSet<Long>();
        long collisions = 0;
        while (keys.hasNext())
        {
            hashes.clear();
            FilterKey buf = FilterTestHelper.wrap(keys.next());
            BloomFilter bf = (BloomFilter) FilterFactory.getFilter(10, 1, false);
            for (long hashIndex : bf.getHashBuckets(buf, MAX_HASH_COUNT, 1024 * 1024))
            {
                hashes.add(hashIndex);
            }
            collisions += (MAX_HASH_COUNT - hashes.size());
        }
        assert collisions <= 100;
    }

    @Test
    public void testManyRandom()
    {
        testManyHashes(FilterTestHelper.randomKeys());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testOffHeapException()
    {
        long numKeys = (Integer.MAX_VALUE * 64) + 1; // approx 128 Billion
        FilterFactory.getFilter(numKeys, 0.01d, true);
    }

    @Test
    public void compareCachedKey()
    {
        BloomFilter bf1 = (BloomFilter) FilterFactory.getFilter(FilterTestHelper.ELEMENTS / 2, FilterTestHelper.MAX_FAILURE_RATE, false);
        BloomFilter bf2 = (BloomFilter) FilterFactory.getFilter(FilterTestHelper.ELEMENTS / 2, FilterTestHelper.MAX_FAILURE_RATE, false);
        BloomFilter bf3 = (BloomFilter) FilterFactory.getFilter(FilterTestHelper.ELEMENTS / 2, FilterTestHelper.MAX_FAILURE_RATE, false);

        RandomStringGenerator gen1 = new KeyGenerator.RandomStringGenerator(new Random().nextInt(), FilterTestHelper.ELEMENTS);

        // make sure all bitsets are empty.
        BitSetTest.compare(bf1.bitset, bf2.bitset);
        BitSetTest.compare(bf1.bitset, bf3.bitset);

        while (gen1.hasNext())
        {
            ByteBuffer key = gen1.next();
            FilterKey cached = FilterTestHelper.wrapCached(key);
            bf1.add(FilterTestHelper.wrap(key));
            bf2.add(cached);
            bf3.add(cached);
        }

        BitSetTest.compare(bf1.bitset, bf2.bitset);
        BitSetTest.compare(bf1.bitset, bf3.bitset);
    }

    @Test
    @Ignore
    public void testHugeBFSerialization() throws IOException
    {
        ByteBuffer test = ByteBuffer.wrap(new byte[] {0, 1});

        File file = FileUtils.createTempFile("bloomFilterTest-", ".dat");
        BloomFilter filter = (BloomFilter) FilterFactory.getFilter(((long)Integer.MAX_VALUE / 8) + 1, 0.01d, true);
        filter.add(FilterTestHelper.wrap(test));
        DataOutputStreamAndChannel out = new DataOutputStreamAndChannel(new FileOutputStream(file));
        FilterFactory.serialize(filter, out);
        filter.bitset.serialize(out);
        out.close();
        
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        BloomFilter filter2 = (BloomFilter) FilterFactory.deserialize(in, true);
        Assert.assertTrue(filter2.isPresent(FilterTestHelper.wrap(test)));
        FileUtils.closeQuietly(in);
    }
    
    @Test
    public void testMurmur3FilterHash()
    {
        IPartitioner<?> partitioner = new Murmur3Partitioner();
        Iterator<ByteBuffer> gen = new KeyGenerator.RandomStringGenerator(new Random().nextInt(), FilterTestHelper.ELEMENTS);
        while (gen.hasNext())
        {
            ByteBuffer key = gen.next();
            FilterKey expectedKey = FilterTestHelper.wrapCached(key);
            FilterKey actualKey = partitioner.decorateKey(key);
            long[] expected = new long[2];
            long[] actual = new long[2];
            Assert.assertTrue(actualKey.retrieveCachedFilterHash(actual));
            Assert.assertTrue(expectedKey.retrieveCachedFilterHash(expected));
            Assert.assertArrayEquals(expected, actual);
        }
    }
}
