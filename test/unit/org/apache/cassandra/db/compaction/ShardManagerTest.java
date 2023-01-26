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

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.db.SortedLocalRanges;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

public class ShardManagerTest
{
    final IPartitioner partitioner = Murmur3Partitioner.instance;
    final Token minimumToken = partitioner.getMinimumToken();

    SortedLocalRanges localRanges;
    List<Splitter.WeightedRange> weightedRanges;

    static final double delta = 1e-15;

    @Before
    public void setUp()
    {
        weightedRanges = new ArrayList<>();
        localRanges = Mockito.mock(SortedLocalRanges.class, Mockito.withSettings().defaultAnswer(Mockito.CALLS_REAL_METHODS));
        Mockito.when(localRanges.getRanges()).thenAnswer(invocation -> weightedRanges);
    }

    @Test
    public void testRangeSpannedFullOwnership()
    {
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(minimumToken, minimumToken)));
        ShardManager shardManager = new ShardManagerNoDisks(localRanges);

        // sanity check
        assertEquals(0.4, tokenAt(0.1).size(tokenAt(0.5)), delta);

        assertEquals(0.5, shardManager.rangeSpanned(range(0.2, 0.7)), delta);
        assertEquals(0.2, shardManager.rangeSpanned(range(0.3, 0.5)), delta);

        assertEquals(0.2, shardManager.rangeSpanned(mockedTable(0.5, 0.7, Double.NaN)), delta);
        // single-partition correction
        assertEquals(1.0, shardManager.rangeSpanned(mockedTable(0.3, 0.3, Double.NaN)), delta);

        // reported coverage
        assertEquals(0.1, shardManager.rangeSpanned(mockedTable(0.5, 0.7, 0.1)), delta);
        // bad coverage
        assertEquals(0.2, shardManager.rangeSpanned(mockedTable(0.5, 0.7, 0.0)), delta);
        assertEquals(0.2, shardManager.rangeSpanned(mockedTable(0.5, 0.7, -1)), delta);

        // correction over coverage
        assertEquals(1.0, shardManager.rangeSpanned(mockedTable(0.3, 0.5, 1e-50)), delta);
    }

    @Test
    public void testRangeSpannedPartialOwnership()
    {
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.05), tokenAt(0.15))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.3), tokenAt(0.4))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.45), tokenAt(0.5))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.7), tokenAt(0.75))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.75), tokenAt(0.85))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.90), tokenAt(0.91))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.92), tokenAt(0.94))));
        weightedRanges.add(new Splitter.WeightedRange(1.0, new Range<>(tokenAt(0.98), tokenAt(1.0))));
        double total = weightedRanges.stream().mapToDouble(wr -> wr.range().left.size(wr.range().right)).sum();

        ShardManager shardManager = new ShardManagerNoDisks(localRanges);

        // sanity check
        assertEquals(0.4, tokenAt(0.1).size(tokenAt(0.5)), delta);

        assertEquals(0.15, shardManager.rangeSpanned(range(0.2, 0.7)), delta);
        assertEquals(0.15, shardManager.rangeSpanned(range(0.3, 0.5)), delta);
        assertEquals(0.0, shardManager.rangeSpanned(range(0.5, 0.7)), delta);
        assertEquals(total, shardManager.rangeSpanned(range(0.0, 1.0)), delta);


        assertEquals(0.1, shardManager.rangeSpanned(mockedTable(0.5, 0.8, Double.NaN)), delta);

        // single-partition correction
        assertEquals(1.0, shardManager.rangeSpanned(mockedTable(0.3, 0.3, Double.NaN)), delta);
        // out-of-local-range correction
        assertEquals(1.0, shardManager.rangeSpanned(mockedTable(0.6, 0.7, Double.NaN)), delta);
        assertEquals(0.001, shardManager.rangeSpanned(mockedTable(0.6, 0.701, Double.NaN)), delta);

        // reported coverage
        assertEquals(0.1, shardManager.rangeSpanned(mockedTable(0.5, 0.7, 0.1)), delta);
        // bad coverage
        assertEquals(0.1, shardManager.rangeSpanned(mockedTable(0.5, 0.8, 0.0)), delta);
        assertEquals(0.1, shardManager.rangeSpanned(mockedTable(0.5, 0.8, -1)), delta);

        // correction over coverage, no recalculation
        assertEquals(1.0, shardManager.rangeSpanned(mockedTable(0.5, 0.8, 1e-50)), delta);
    }

    Token tokenAt(double pos)
    {
        return partitioner.split(minimumToken, minimumToken, pos);
    }

    Range<Token> range(double start, double end)
    {
        return new Range<>(tokenAt(start), tokenAt(end));
    }

    CompactionSSTable mockedTable(double start, double end, double reportedCoverage)
    {
        CompactionSSTable mock = Mockito.mock(CompactionSSTable.class);
        Mockito.when(mock.getFirst()).thenReturn(tokenAt(start).minKeyBound());
        Mockito.when(mock.getLast()).thenReturn(tokenAt(end).minKeyBound());
        Mockito.when(mock.tokenSpaceCoverage()).thenReturn(reportedCoverage);
        return mock;
    }

    private Token[] shardTokens()
    {
        Token[] shards = new Token[]
                         {
                         tokenAt(0.1),
                         tokenAt(0.25),
                         tokenAt(0.3),
                         tokenAt(0.33),
                         tokenAt(0.34),
                         tokenAt(0.4),
                         tokenAt(0.7),
                         tokenAt(0.9),
                         minimumToken
                         };
        return shards;
    }
}
