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

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.Slice.Bound;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.rows.Unfiltered.Kind;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SearchIterator;

public class UnfilteredRowIteratorsMergeTest
{
    static DecoratedKey partitionKey = Util.dk("key");
    static DeletionTime partitionLevelDeletion = DeletionTime.LIVE;
    static CFMetaData metadata = CFMetaData.Builder.create("UnfilteredRowIteratorsMergeTest", "Test").
            addPartitionKey("key", AsciiType.instance).
            addClusteringColumn("clustering", Int32Type.instance).
            addRegularColumn("data", Int32Type.instance).
            build();
    static Comparator<Clusterable> comparator = new ClusteringComparator(Int32Type.instance);
    static int nowInSec = FBUtilities.nowInSeconds();

    static final int RANGE = 100;
    static final int DEL_RANGE = 100;
    static final int ITERATORS = 3;
    static final int ITEMS = 10;

    public UnfilteredRowIteratorsMergeTest()
    {
    }

    @Test
    public void testTombstoneMerge()
    {
        testTombstoneMerge(false, false);
    }

//    @Test
//    public void testTombstoneMergeReversed()
//    {
//        testTombstoneMerge(true, false);
//    }
//
//    @Test
//    public void testTombstoneMergeIterative()
//    {
//        testTombstoneMerge(false, true);
//    }
//
//    @Test
//    public void testTombstoneMergeReversedIterative()
//    {
//        testTombstoneMerge(true, true);
//    }

    public void testTombstoneMerge(boolean reversed, boolean iterations)
    {
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
                sources.add(generateSource(r, reversed, timeGenerators.get(r.nextInt(timeGenerators.size()))));
            List<UnfilteredRowIterator> us = sources.stream().map(l -> new Source(l.iterator(), reversed)).collect(Collectors.toList());
            List<Unfiltered> merged = new ArrayList<>();
            Iterators.addAll(merged, safeIterator(merge(us, iterations)));
    
            if (ITEMS <= 20)
                System.out.println("results in");
            if (ITEMS <= 20)
                dumpList(merged);
            verifyEquivalent(sources, merged, reversed);
            if (reversed)
                Collections.reverse(merged);

            verifyValid(merged);
        }
    }
    
    public UnfilteredRowIterator merge(List<UnfilteredRowIterator> us, boolean iterations)
    {
        if (iterations)
        {
            UnfilteredRowIterator mi = us.get(0);
            int i;
            for (i = 1; i + 2 <= ITERATORS; i += 2)
                mi = UnfilteredRowIterators.merge(ImmutableList.of(mi, us.get(i), us.get(i+1)));
            if (i + 1 <= ITERATORS)
                mi = UnfilteredRowIterators.merge(ImmutableList.of(mi, us.get(i)));
            return mi;
        }
        else
        {
            return UnfilteredRowIterators.merge(us);
        }
    }

    private static List<Unfiltered> generateSource(Random r, boolean reversed, Function<Integer, Integer> timeGenerator)
    {
        int[] positions = new int[ITEMS + 1];
        for (int i=0; i<ITEMS; ++i)
            positions[i] = r.nextInt(RANGE);
        positions[ITEMS] = RANGE;
        Arrays.sort(positions);

        List<Unfiltered> content = new ArrayList<>(ITEMS);
        int prev = -1;
        for (int i=0; i<ITEMS; ++i)
        {
            int pos = positions[i];
            int sz = positions[i + 1] - pos;
            if (sz > 0 && r.nextBoolean())
            {
                int span = pos > prev ? r.nextInt(sz + 1) : 1 + r.nextInt(sz);
                int deltime = r.nextInt(DEL_RANGE);
                DeletionTime dt = new SimpleDeletionTime(deltime, deltime);
                content.add(new SimpleRangeTombstoneMarker(boundFor(pos, true, pos > prev ? (span > 0 ? r.nextBoolean() : true) : false), dt));
                content.add(new SimpleRangeTombstoneMarker(boundFor(pos + span, false, span < sz ? (span > 0 ? r.nextBoolean() : true) : false), dt));
            }
            else
            {
                content.add(emptyRowAt(pos, timeGenerator));
            }
            prev = pos;
        }

        verifyValid(content);
        if (reversed)
            Collections.reverse(content);
        if (ITEMS <= 20)
            dumpList(content);
        return content;
    }

    static void verifyValid(List<Unfiltered> list)
    {
        try {
            RangeTombstoneMarker prev = null;
            Unfiltered prevUnfiltered = null;
            for (Unfiltered unfiltered : list)
            {
                if (prevUnfiltered != null)
                    Assert.assertTrue(str(prevUnfiltered) + " not ordered before " + str(unfiltered), comparator.compare(prevUnfiltered, unfiltered) <= 0);
                prevUnfiltered = unfiltered;

                if (unfiltered.kind() == Kind.RANGE_TOMBSTONE_MARKER)
                {
                    RangeTombstoneMarker curr = (RangeTombstoneMarker) unfiltered;
                    if (prev != null)
                    {
                        Bound currBound = curr.clustering();
                        Bound prevBound = prev.clustering();

                        if (currBound.isEnd())
                        {
                            Assert.assertTrue(str(unfiltered) + " follows another close marker " + str(prev), prevBound.isStart());
                            Assert.assertEquals("Deletion time mismatch for open " + str(prev) + " and close " + str(unfiltered),
                                                prev.deletionTime(),
                                                curr.deletionTime());
                        }
                        else
                            Assert.assertTrue(str(curr) + " follows another open marker " + str(prev), prevBound.isEnd());
                    }

                    prev = curr;
                }
            }
            Assert.assertFalse("Cannot end in open marker " + str(prev), prev != null && prev.clustering().isStart());

        } catch (AssertionError e) {
            System.out.println(e);
            dumpList(list);
            throw e;
        }
    }

    static void verifyEquivalent(List<List<Unfiltered>> sources, List<Unfiltered> merged, boolean reversed)
    {
        try
        {
            for (int i=0; i<RANGE; ++i)
            {
                Clusterable c = clusteringFor(i);
                DeletionTime dt = DeletionTime.LIVE;
                for (List<Unfiltered> source : sources)
                {
                    dt = deletionFor(c, source, dt, reversed);
                }
                Assert.assertEquals("Deletion time mismatch for position " + str(c), dt, deletionFor(c, merged, reversed));
                if (dt == DeletionTime.LIVE)
                {
                    Optional<Unfiltered> sourceOpt = sources.stream().map(source -> rowFor(c, source, reversed)).filter(x -> x != null).findAny();
                    Unfiltered mergedRow = rowFor(c, merged, reversed);
                    Assert.assertEquals("Content mismatch for position " + str(c), str(sourceOpt.orElse(null)), str(mergedRow));
                }
            }
        }
        catch (AssertionError e)
        {
            System.out.println(e);
            for (List<Unfiltered> list : sources)
                dumpList(list);
            System.out.println("merged");
            dumpList(merged);
            throw e;
        }
    }

    private static Unfiltered rowFor(Clusterable pointer, List<Unfiltered> list, boolean reversed)
    {
        int index = Collections.binarySearch(list, pointer, reversed ? comparator.reversed() : comparator);
        return index >= 0 ? list.get(index) : null;
    }

    static DeletionTime deletionFor(Clusterable pointer, List<Unfiltered> list, boolean reversed)
    {
        return deletionFor(pointer, list, DeletionTime.LIVE, reversed);
    }

    static DeletionTime deletionFor(Clusterable pointer, List<Unfiltered> list, DeletionTime def, boolean reversed)
    {
        if (list.isEmpty())
            return def;

        int index = Collections.binarySearch(list, pointer, reversed ? comparator.reversed() : comparator);
        if (index < 0)
            index = -1 - index;
        else
        {
            Row row = (Row) list.get(index);
            if (row.deletion() != null && row.deletion().supersedes(def))
                def = row.deletion();
        }

        if (index >= list.size())
            return def;

        while (--index >= 0)
        {
            Unfiltered unfiltered = list.get(index);
            if (unfiltered.kind() == Kind.ROW)
                continue;
            RangeTombstoneMarker lower = (RangeTombstoneMarker) unfiltered;
            if (lower.clustering().isEnd() != reversed)
                return def;
            return lower.deletionTime().supersedes(def) ? lower.deletionTime() : def;
        }
        return def;
    }

    private static Bound boundFor(int pos, boolean start, boolean inclusive)
    {
        return Bound.create(Bound.boundKind(start, inclusive), new ByteBuffer[] {Int32Type.instance.decompose(pos)});
    }

    private static SimpleClustering clusteringFor(int i)
    {
        return new SimpleClustering(Int32Type.instance.decompose(i));
    }
    
    static Row emptyRowAt(int pos, Function<Integer, Integer> timeGenerator)
    {
        final Clustering clustering = clusteringFor(pos);
        final LivenessInfo live = SimpleLivenessInfo.forUpdate(timeGenerator.apply(pos), 0, nowInSec, metadata);
        return emptyRowAt(clustering, live, DeletionTime.LIVE);
    }

    public static class TestCell extends AbstractCell
    {
        private final ColumnDefinition column;
        private final ByteBuffer value;
        private final LivenessInfo info;

        public TestCell(ColumnDefinition column, ByteBuffer value, LivenessInfo info)
        {
            this.column = column;
            this.value = value;
            this.info = info.takeAlias();
        }

        public ColumnDefinition column()
        {
            return column;
        }

        public boolean isCounterCell()
        {
            return false;
        }

        public ByteBuffer value()
        {
            return value;
        }

        public LivenessInfo livenessInfo()
        {
            return info;
        }

        public CellPath path()
        {
            return null;
        }
    }
    
    static Row emptyRowAt(final Clustering clustering, final LivenessInfo live, final DeletionTime deletion)
    {
        final ColumnDefinition columnDef = metadata.getColumnDefinition(new ColumnIdentifier("data", true));
        final Cell cell = new TestCell(columnDef, clustering.get(0), live);
        
        return new AbstractRow()
        {
            public Columns columns()
            {
                return Columns.of(columnDef);
            }

            public LivenessInfo primaryKeyLivenessInfo()
            {
                return live;
            }

            public long maxLiveTimestamp()
            {
                return live.timestamp();
            }

            public int nowInSec()
            {
                return nowInSec;
            }

            public DeletionTime deletion()
            {
                return deletion;
            }

            public boolean isEmpty()
            {
                return true;
            }

            public boolean hasComplexDeletion()
            {
                return false;
            }

            public Clustering clustering()
            {
                return clustering;
            }

            public Cell getCell(ColumnDefinition c)
            {
                return c == columnDef ? cell : null;
            }

            public Cell getCell(ColumnDefinition c, CellPath path)
            {
                return null;
            }

            public Iterator<Cell> getCells(ColumnDefinition c)
            {
                return Iterators.singletonIterator(cell);
            }

            public DeletionTime getDeletion(ColumnDefinition c)
            {
                return DeletionTime.LIVE;
            }

            public Iterator<Cell> iterator()
            {
                return Iterators.<Cell>emptyIterator();
            }

            public SearchIterator<ColumnDefinition, ColumnData> searchIterator()
            {
                return new SearchIterator<ColumnDefinition, ColumnData>()
                {
                    public boolean hasNext()
                    {
                        return false;
                    }

                    public ColumnData next(ColumnDefinition column)
                    {
                        return null;
                    }
                };
            }

            public Kind kind()
            {
                return Unfiltered.Kind.ROW;
            }

            public Row takeAlias()
            {
                return this;
            }

            public String toString()
            {
                return Int32Type.instance.getString(clustering.get(0));
            }
        };

    }

    private static void dumpList(List<Unfiltered> list)
    {
        for (Unfiltered u : list)
            System.out.print(str(u) + " ");
        System.out.println();
    }

    private static String str(Clusterable prev)
    {
        if (prev == null)
            return "null";
        String val = Int32Type.instance.getString(prev.clustering().get(0));
        if (prev instanceof RangeTombstoneMarker)
        {
            Bound b = (Bound) prev.clustering();
            if (b.isStart()) 
                val = val + (b.isInclusive() ? "<=" : "<") + "[" + ((RangeTombstoneMarker) prev).deletionTime().markedForDeleteAt() + "]";
            else
                val = (b.isInclusive() ? "<=" : "<") + val + "[" + ((RangeTombstoneMarker) prev).deletionTime().markedForDeleteAt() + "]";
        }
        return val;
    }

    static class Source extends AbstractUnfilteredRowIterator implements UnfilteredRowIterator
    {
        Iterator<Unfiltered> content;

        protected Source(Iterator<Unfiltered> content, boolean reversed)
        {
            super(UnfilteredRowIteratorsMergeTest.metadata,
                  UnfilteredRowIteratorsMergeTest.partitionKey,
                  UnfilteredRowIteratorsMergeTest.partitionLevelDeletion,
                  UnfilteredRowIteratorsMergeTest.metadata.partitionColumns(),
                  null,
                  reversed,
                  RowStats.NO_STATS,
                  UnfilteredRowIteratorsMergeTest.nowInSec);
            this.content = content;
        }

        @Override
        protected Unfiltered computeNext()
        {
            return content.hasNext() ? content.next() : endOfData();
        }
    }

    static DeletionTime safeDeletionTime(RangeTombstoneMarker m)
    {
        return new SimpleDeletionTime(m.deletionTime().markedForDeleteAt(), m.deletionTime().localDeletionTime());
    }

    static private Bound safeBound(Bound clustering)
    {
        return Bound.create(clustering.kind(), new ByteBuffer[] {clustering.get(0)});
    }
    
    private static Row safeRow(Row row)
    {
        return emptyRowAt(new SimpleClustering(row.clustering().get(0)), SimpleLivenessInfo.forUpdate(row.maxLiveTimestamp(), 0, nowInSec, metadata), row.deletion());
    }
    
    public static UnfilteredRowIterator safeIterator(UnfilteredRowIterator iterator)
    {
        return new WrappingUnfilteredRowIterator(iterator)
        {
            public Unfiltered next()
            {
                Unfiltered next = super.next();
                return next.kind() == Unfiltered.Kind.ROW
                     ? safeRow((Row) next)
                     : new SimpleRangeTombstoneMarker(safeBound((Bound) next.clustering()), safeDeletionTime((RangeTombstoneMarker)next));
            }
        };
    }


}
