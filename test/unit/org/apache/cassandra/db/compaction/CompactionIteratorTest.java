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
package org.apache.cassandra.db.compaction;

import static org.junit.Assert.*;

import java.util.*;

import com.google.common.collect.*;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.partitions.AbstractUnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.schema.KeyspaceParams;

public class CompactionIteratorTest
{

    private static final int NOW = 1000;
    private static final int GC_BEFORE = 100;
    private static final String KSNAME = "CompactionIteratorTest";
    private static final String CFNAME = "Integer1";

    CFMetaData metadata;
//    = CFMetaData.Builder.create("CompactionIteratorTest", "Test").
//            addPartitionKey("key", AsciiType.instance).
//            addClusteringColumn("clustering", Int32Type.instance).
//            addRegularColumn("data", Int32Type.instance).
//            build();
    DecoratedKey kk = Util.dk("key");

    public CompactionIteratorTest()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KSNAME,
                                    KeyspaceParams.simple(1),
                                    metadata = SchemaLoader.standardCFMD(KSNAME,
                                                                         CFNAME,
                                                                         1,
                                                                         UTF8Type.instance,
                                                                         Int32Type.instance,
                                                                         Int32Type.instance));
    }

    @Test
    public void testGcCompaction()
    {
        testCompaction(new String[] {
            "5<=[140] 10[150] [140]<20 22<[130] [130]<25 30[150]"
        }, new String[] {
            "7<[160] 15[180] [160]<30 40[120]"
        },
        "5<=[140] [140]<=7 30[150]");
    }

    void testCompaction(String[] inputs, String[] tombstones, String expected)
    {
        UnfilteredRowsGenerator generator = new UnfilteredRowsGenerator(metadata.comparator, false);
        List<Unfiltered> result = compact(parse(inputs, generator), parse(tombstones, generator));
        generator.verifyValid(result);
        verifyEquivalent(parse(inputs, generator), result, parse(tombstones, generator), generator);
        List<Unfiltered> expectedResult = generator.parse(expected, NOW - 1);
        if (!expectedResult.equals(result))
            fail("Expected " + expected + ", got " + generator.str(result));
    }

    private void verifyEquivalent(List<List<Unfiltered>> sources, List<Unfiltered> result, List<List<Unfiltered>> tombstoneSources, UnfilteredRowsGenerator generator)
    {
        // sources + tombstoneSources must be the same as result + tombstoneSources
        List<Unfiltered> expected = compact(Iterables.concat(sources, tombstoneSources), Collections.emptyList());
        List<Unfiltered> actual = compact(Iterables.concat(ImmutableList.of(result), tombstoneSources), Collections.emptyList());
        if (!expected.equals(actual))
        {
            System.out.println("Equivalence test failure between sources:");
            for (List<Unfiltered> partition : sources)
                generator.dumpList(partition);
            System.out.println("and compacted " + generator.str(result));
            System.out.println("with tombstone sources:");
            for (List<Unfiltered> partition : tombstoneSources)
                generator.dumpList(partition);
            System.out.println("expected " + generator.str(expected));
            System.out.println("got " + generator.str(actual));
            fail("Failed equivalence test.");
        }
    }

    private List<List<Unfiltered>> parse(String[] inputs, UnfilteredRowsGenerator generator)
    {
        return Lists.transform(Arrays.asList(inputs), x -> generator.parse(x, NOW - 1));
    }

    private List<Unfiltered> compact(Iterable<List<Unfiltered>> sources, Iterable<List<Unfiltered>> tombstoneSources)
    {
        List<Iterable<UnfilteredRowIterator>> content = ImmutableList.copyOf(Iterables.transform(sources, list -> ImmutableList.of(listToIterator(list, kk))));
        Map<DecoratedKey, Iterable<UnfilteredRowIterator>> transformedSources = new TreeMap<>();
        transformedSources.put(kk, Iterables.transform(tombstoneSources, list -> listToIterator(list, kk)));
        try (CompactionController controller = new Controller(Keyspace.openAndGetStore(metadata), transformedSources, GC_BEFORE);
             CompactionIterator iter = new CompactionIterator(OperationType.COMPACTION,
                                                              Lists.transform(content, x -> new Scanner(x)),
                                                              controller, NOW, null))
        {
            List<Unfiltered> result = new ArrayList<>();
            assertTrue(iter.hasNext());
            try (UnfilteredRowIterator partition = iter.next())
            {
                Iterators.addAll(result, partition);
            }
            assertFalse(iter.hasNext());
            return result;
        }
    }
    
    private UnfilteredRowIterator listToIterator(List<Unfiltered> list, DecoratedKey key)
    {
        return UnfilteredRowsGenerator.source(list, metadata, key);
    }
    
//    NavigableMap<DecoratedKey, UnfilteredRowIterator> generateContent(Random rand, List<DecoratedKey> keys, int pcount, int rcount)
//    {
//        NavigableMap<DecoratedKey, UnfilteredRowIterator> map = new TreeMap<>();
//        for (int i = 0; i < pcount; ++i)
//        {
//            DecoratedKey key = keys.get(rand.nextInt(keys.size()));
//            map.put(key, generateContent(rand, rcount));
//        }
//        return map;
//    }
//
//    UnfilteredRowIterator generateContent(Random rand, int rcount)
//    {
//        // TODO Auto-generated method stub
//        return null;
//    }

    class Controller extends CompactionController
    {
        private final Map<DecoratedKey, Iterable<UnfilteredRowIterator>> tombstoneSources;

        public Controller(ColumnFamilyStore cfs, Map<DecoratedKey, Iterable<UnfilteredRowIterator>> tombstoneSources, int gcBefore)
        {
            super(cfs, Collections.emptySet(), gcBefore);
            this.tombstoneSources = tombstoneSources;
        }

        @Override
        public Iterable<UnfilteredRowIterator> tombstoneSources(DecoratedKey key)
        {
            return tombstoneSources.get(key);
        }
    }

    class Scanner extends AbstractUnfilteredPartitionIterator implements ISSTableScanner
    {
        Iterator<UnfilteredRowIterator> iter;

        Scanner(Iterable<UnfilteredRowIterator> content)
        {
            iter = content.iterator();
        }

        @Override
        public boolean isForThrift()
        {
            return false;
        }

        @Override
        public CFMetaData metadata()
        {
            return metadata;
        }

        @Override
        public boolean hasNext()
        {
            return iter.hasNext();
        }

        @Override
        public UnfilteredRowIterator next()
        {
            return iter.next();
        }

        @Override
        public long getLengthInBytes()
        {
            return 0;
        }

        @Override
        public long getCurrentPosition()
        {
            return 0;
        }

        @Override
        public String getBackingFiles()
        {
            return null;
        }
    }
}
