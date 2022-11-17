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

import java.util.Arrays;
import java.util.Collection;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SortedLocalRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;

public class ShardManager
{
    /**
     * Single-partition, and generally sstables with very few partitions, can cover very small sections of the token
     * space, resulting in very high densities.
     * Additionally, sstables that have completely fallen outside of the local token ranges will end up with a zero
     * coverage.
     * To avoid problems with both we check if coverage is below the minimum, and replace it with 1.
     */
    static final double MINIMUM_TOKEN_COVERAGE = Math.scalb(1.0, -48);

    private final Token[] shardBoundaries;
    final SortedLocalRanges localRanges;

    public ShardManager(Collection<Token> shardBoundaries, SortedLocalRanges localRanges)
    {
        this(shardBoundaries.toArray(new Token[shardBoundaries.size()]), localRanges);
    }

    public ShardManager(Token[] shardBoundaries, SortedLocalRanges localRanges)
    {
        this.shardBoundaries = shardBoundaries;
        this.localRanges = localRanges;
    }

    public int shardFor(Token token)
    {
        int pos = Arrays.binarySearch(shardBoundaries, token);
        return pos >= 0 ? pos : -pos - 1;   // boundaries are end-inclusive
    }

    /**
     * Returns the shard where this key belongs. Shards are given by their end boundaries (i.e. shard 0 covers the space
     * between minimum and shardBoundaries[0], shard 1 is between shardBoundaries[0] and shardBoundaries[1]), thus
     * finding the index of the first bigger boundary gives the index of the covering shard.
     */
    public int shardFor(PartitionPosition key)
    {
        return shardFor(key.getToken());
    }

    /**
     * Return the token space share that the given SSTable spans, excluding any non-locally owned space.
     * Returns a positive floating-point number between 0 and 1.
     */
    public double rangeSpanned(CompactionSSTable rdr)
    {
        double reported = rdr.tokenSpaceCoverage();
        double span;
        if (reported > 0)   // also false for NaN
            span = reported;
        else
            span = rangeSpanned(rdr.getFirst(), rdr.getLast());

        if (span >= MINIMUM_TOKEN_COVERAGE)
            return span;

        // Too small ranges are expected to be the result of either a single-partition sstable or falling outside
        // of the local token ranges. In these cases we substitute it with 1 because for them sharding and density
        // tiering does not make sense.
        return 1.0;  // This will be chosen if span is NaN too.
    }

    public double rangeSpanned(PartitionPosition first, PartitionPosition last)
    {
        return rangeSpanned(coveringRange(first, last));
    }

    public double rangeSpanned(Range<Token> tableRange)
    {
        assert !tableRange.isTrulyWrapAround();
        return rangeSizeNonWrapping(tableRange);
    }

    public double rangeSpannedInShard(CompactionSSTable rdr, int shardIdx)
    {
        Range<Token> shardSpan = shardSpan(shardIdx);
        Range<Token> tableRange = coveringRange(rdr.getFirst(), rdr.getLast());
        final Range<Token> tableInShardRange = shardSpan.intersectionNonWrapping(tableRange);
        if (tableInShardRange == null)
            return 0;
        return rangeSizeNonWrapping(tableInShardRange);
    }

    public Range<Token> shardSpan(int shardIdx)
    {
        if (shardIdx == 0)
            return new Range<>(shardBoundaries[0].minValue(), shardBoundaries[0]);
        else
            return new Range<>(shardBoundaries[shardIdx - 1], shardBoundaries[shardIdx]);
    }

    public double rangeSizeNonWrapping(Range<Token> tableRange)
    {
        double size = 0;
        for (Splitter.WeightedRange range : localRanges.getRanges())
        {
            Range<Token> ix = range.range().intersectionNonWrapping(tableRange);
            if (ix == null)
                continue;
            size += ix.left.size(ix.right);
        }
        return size;
    }

    private static Range<Token> coveringRange(PartitionPosition first, PartitionPosition last)
    {
        // To include the token of last, the range's upper bound must be increased.
        return new Range<>(first.getToken(), last.getToken().nextValidToken());
    }

    /**
     * Return the density of an SSTable, i.e. its size divided by the covered token space share.
     * This is an improved measure of the compaction age of an SSTable that grows both with STCS-like full-SSTable
     * compactions (where size grows, share is constant), LCS-like size-threshold splitting (where size is constant
     * but share shrinks), UCS-like compactions (where size may grow and covered shards i.e. share may decrease)
     * and can reproduce levelling structure that corresponds to all, including their mixtures.
     */
    public double density(CompactionSSTable rdr)
    {
        return rdr.onDiskLength() / rangeSpanned(rdr);
    }

    public int compareByDensity(CompactionSSTable a, CompactionSSTable b)
    {
        return Double.compare(density(a), density(b));
    }

    public int size()
    {
        return shardBoundaries.length;
    }

    public Token get(int index)
    {
        return shardBoundaries[index];
    }
}
