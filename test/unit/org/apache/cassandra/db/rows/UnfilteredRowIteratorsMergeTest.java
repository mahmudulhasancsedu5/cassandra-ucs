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
package org.apache.cassandra.db.rows;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Unfiltered.Kind;
import org.apache.cassandra.utils.FBUtilities;

public class UnfilteredRowIteratorsMergeTest
{
    static DecoratedKey partitionKey = Util.dk("key");
    static CFMetaData metadata = CFMetaData.Builder.create("UnfilteredRowIteratorsMergeTest", "Test").
            addPartitionKey("key", AsciiType.instance).
            addClusteringColumn("clustering", Int32Type.instance).
            addRegularColumn("data", Int32Type.instance).
            build();
    static Comparator<Clusterable> comparator = new ClusteringComparator(Int32Type.instance);
    static int nowInSec = FBUtilities.nowInSeconds();

    static final int RANGE = 3000;
    static final int DEL_RANGE = 100;
    static final int ITERATORS = 15;
    static final int ITEMS = 300;

    boolean reversed;

    public UnfilteredRowIteratorsMergeTest()
    {
    }

    @Test
    public void testTombstoneMerge()
    {
        testTombstoneMerge(false, false);
    }

    @Test
    public void testTombstoneMergeReversed()
    {
        testTombstoneMerge(true, false);
    }

    @Test
    public void testTombstoneMergeIterative()
    {
        testTombstoneMerge(false, true);
    }

    @Test
    public void testTombstoneMergeReversedIterative()
    {
        testTombstoneMerge(true, true);
    }

    @Test
    public void testDuplicateRangeCase()
    {
        testForInput("67<=[98] [98]<=67",
                     "66<=[11] [11]<71",
                     "66<[13] [13]<67");
    }

    @SuppressWarnings("unused")
    public void testTombstoneMerge(boolean reversed, boolean iterations)
    {
        this.reversed = reversed;
        UnfilteredRowsGenerator generator = new UnfilteredRowsGenerator(comparator, reversed);

        for (int seed = 1; seed <= 100; ++seed)
        {
            if (ITEMS <= 20)
                System.out.println("\nSeed " + seed);

            Random r = new Random(seed);
            List<Function<Integer, Integer>> timeGenerators = ImmutableList.of(
                    x -> -1,
                    x -> DEL_RANGE,
                    x -> r.nextInt(DEL_RANGE)
                );
            List<List<Unfiltered>> sources = new ArrayList<>(ITERATORS);
            if (ITEMS <= 20)
                System.out.println("Merging");
            for (int i=0; i<ITERATORS; ++i)
                sources.add(generator.generateSource(r, ITEMS, RANGE, DEL_RANGE, timeGenerators.get(r.nextInt(timeGenerators.size()))));
            List<Unfiltered> merged = merge(sources, iterations);
    
            if (ITEMS <= 20)
                System.out.println("results in");
            if (ITEMS <= 20)
                generator.dumpList(merged);
            verifyEquivalent(sources, merged, generator);
            generator.verifyValid(merged);
            if (reversed)
            {
                Collections.reverse(merged);
                generator.verifyValid(merged, false);
            }
        }
    }

    private List<Unfiltered> merge(List<List<Unfiltered>> sources, boolean iterations)
    {
        List<UnfilteredRowIterator> us = sources.
                stream().
                map(l -> new UnfilteredRowsGenerator.Source(l.iterator(), metadata, partitionKey, DeletionTime.LIVE, reversed)).
                collect(Collectors.toList());
        List<Unfiltered> merged = new ArrayList<>();
        Iterators.addAll(merged, mergeIterators(us, iterations));
        return merged;
    }

    public UnfilteredRowIterator mergeIterators(List<UnfilteredRowIterator> us, boolean iterations)
    {
        int now = FBUtilities.nowInSeconds();
        if (iterations)
        {
            UnfilteredRowIterator mi = us.get(0);
            int i;
            for (i = 1; i + 2 <= ITERATORS; i += 2)
                mi = UnfilteredRowIterators.merge(ImmutableList.of(mi, us.get(i), us.get(i+1)), now);
            if (i + 1 <= ITERATORS)
                mi = UnfilteredRowIterators.merge(ImmutableList.of(mi, us.get(i)), now);
            return mi;
        }
        else
        {
            return UnfilteredRowIterators.merge(us, now);
        }
    }

    void verifyEquivalent(List<List<Unfiltered>> sources, List<Unfiltered> merged, UnfilteredRowsGenerator generator)
    {
        try
        {
            for (int i=0; i<RANGE; ++i)
            {
                Clusterable c = UnfilteredRowsGenerator.clusteringFor(i);
                DeletionTime dt = DeletionTime.LIVE;
                for (List<Unfiltered> source : sources)
                {
                    dt = deletionFor(c, source, dt);
                }
                Assert.assertEquals("Deletion time mismatch for position " + i, dt, deletionFor(c, merged));
                if (dt == DeletionTime.LIVE)
                {
                    Optional<Unfiltered> sourceOpt = sources.stream().map(source -> rowFor(c, source)).filter(x -> x != null).findAny();
                    Unfiltered mergedRow = rowFor(c, merged);
                    Assert.assertEquals("Content mismatch for position " + i, clustering(sourceOpt.orElse(null)), clustering(mergedRow));
                }
            }
        }
        catch (AssertionError e)
        {
            System.out.println(e);
            for (List<Unfiltered> list : sources)
                generator.dumpList(list);
            System.out.println("merged");
            generator.dumpList(merged);
            throw e;
        }
    }

    String clustering(Clusterable curr)
    {
        if (curr == null)
            return "null";
        return Int32Type.instance.getString(curr.clustering().get(0));
    }

    private Unfiltered rowFor(Clusterable pointer, List<Unfiltered> list)
    {
        int index = Collections.binarySearch(list, pointer, reversed ? comparator.reversed() : comparator);
        return index >= 0 ? list.get(index) : null;
    }

    DeletionTime deletionFor(Clusterable pointer, List<Unfiltered> list)
    {
        return deletionFor(pointer, list, DeletionTime.LIVE);
    }

    DeletionTime deletionFor(Clusterable pointer, List<Unfiltered> list, DeletionTime def)
    {
        if (list.isEmpty())
            return def;

        int index = Collections.binarySearch(list, pointer, reversed ? comparator.reversed() : comparator);
        if (index < 0)
            index = -1 - index;
        else
        {
            Row row = (Row) list.get(index);
            if (row.deletion().supersedes(def))
                def = row.deletion().time();
        }

        if (index >= list.size())
            return def;

        while (--index >= 0)
        {
            Unfiltered unfiltered = list.get(index);
            if (unfiltered.kind() == Kind.ROW)
                continue;
            RangeTombstoneMarker lower = (RangeTombstoneMarker) unfiltered;
            if (!lower.isOpen(reversed))
                return def;
            return lower.openDeletionTime(reversed).supersedes(def) ? lower.openDeletionTime(reversed) : def;
        }
        return def;
    }

    public void testForInput(String... inputs)
    {
        reversed = false;
        UnfilteredRowsGenerator generator = new UnfilteredRowsGenerator(comparator, false);

        List<List<Unfiltered>> sources = new ArrayList<>();
        for (String input : inputs)
        {
            List<Unfiltered> source = generator.parse(input, DEL_RANGE);
            generator.dumpList(source);
            generator.verifyValid(source);
            sources.add(source);
        }

        List<Unfiltered> merged = merge(sources, false);
        System.out.println("Merge to:");
        generator.dumpList(merged);
        verifyEquivalent(sources, merged, generator);
        generator.verifyValid(merged);
        System.out.println();
    }
}
