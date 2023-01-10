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

import java.util.List;
import java.util.Set;

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

    final SortedLocalRanges localRanges;

    // Staring positions for the local token ranges, in covered token range. The last number defines the total token
    // shared owned by the node.
    double[] localRangePositions;

    public ShardManager(SortedLocalRanges localRanges)
    {
        this.localRanges = localRanges;
        double position = 0;
        final List<Splitter.WeightedRange> ranges = localRanges.getRanges();
        localRangePositions = new double[ranges.size()];
        for (int i = 0; i < localRangePositions.length; ++i)
        {
            Range<Token> range = ranges.get(i).range();
            double span = range.left.size(range.right);
            position += span;
            localRangePositions[i] = position;
        }
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

    public static Range<Token> coveringRange(CompactionSSTable sstable)
    {
        return coveringRange(sstable.getFirst(), sstable.getLast());
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

    /**
     * Estimate the density of the sstable that will be the result of compacting the given sources.
     */
    public double calculateCombinedDensity(Set<? extends CompactionSSTable> sstables)
    {
        if (sstables.isEmpty())
            return 0;
        long onDiskLength = 0;
        PartitionPosition min = null;
        PartitionPosition max = null;
        for (CompactionSSTable sstable : sstables)
        {
            onDiskLength += sstable.onDiskLength();
            min = min == null || min.compareTo(sstable.getFirst()) > 0 ? sstable.getFirst() : min;
            max = max == null || max.compareTo(sstable.getLast()) < 0 ? sstable.getLast() : max;
        }
        double span = rangeSpanned(min, max);
        if (span >= MINIMUM_TOKEN_COVERAGE)
            return onDiskLength / span;
        else
            return onDiskLength;
    }

    public double localSpaceCoverage()
    {
        return localRangePositions[localRangePositions.length - 1];
    }

    /**
     * Construct a boundary/shard iterator for the given number of shards.
     */
    public BoundaryIterator boundaries(int count)
    {
        return new BoundaryIterator(count);
    }

    public class BoundaryIterator
    {
        private final double rangeStep;
        private final int count;
        private int pos;
        private int currentRange;
        private Token currentStart;
        private Token currentEnd;   // null for the last shard

        public BoundaryIterator(int count)
        {
            this.count = count;
            rangeStep = localSpaceCoverage() / count;
            currentStart = localRanges.getRanges().get(0).left();
            currentRange = 0;
            pos = 1;
            if (pos == count)
                currentEnd = null;
            else
                currentEnd = getEndToken(rangeStep * pos);
        }

        private Token getEndToken(double toPos)
        {
            double left = currentRange > 0 ? localRangePositions[currentRange - 1] : 0;
            double right = localRangePositions[currentRange];
            while (toPos > right)
            {
                left = right;
                right = localRangePositions[++currentRange];
            }

            final Range<Token> range = localRanges.getRanges().get(currentRange).range();
            return currentStart.getPartitioner().split(range.left, range.right, (toPos - left) / (right - left));
        }

        public Token shardStart()
        {
            return currentStart;
        }

        public Token shardEnd()
        {
            return currentEnd;
        }

        public Range<Token> shardSpan()
        {
            return new Range<>(currentStart, currentEnd != null ? currentEnd : currentStart.minValue());
        }

        public double shardSpanSize()
        {
            return rangeStep;
        }

        /**
         * Advance to the given token (e.g. before writing a key). Returns true if this resulted in advancing to a new
         * shard, and false otherwise.
         */
        public boolean advanceTo(Token nextToken)
        {
            if (currentEnd == null || nextToken.compareTo(currentEnd) < 0)
                return false;
            do
            {
                currentStart = currentEnd;
                if (++pos == count)
                    currentEnd = null;
                else
                    currentEnd = getEndToken(rangeStep * pos);
            }
            while (!(currentEnd == null || nextToken.compareTo(currentEnd) < 0));
            return true;
        }

        public int count()
        {
            return count;
        }

        /**
         * Returns the fraction of the given token range's coverage that falls within this shard.
         * E.g. if the span covers two shards exactly and the current shard is one of them, it will return 0.5.
         */
        public double fractionInShard(Range<Token> targetSpan)
        {
            Range<Token> shardSpan = shardSpan();
            Range<Token> covered = targetSpan.intersectionNonWrapping(shardSpan);
            if (covered == null)
                return 0;
            if (covered == targetSpan)
                return 1;
            double inShardSize = covered == shardSpan ? shardSpanSize() : ShardManager.this.rangeSpanned(covered);
            double totalSize = ShardManager.this.rangeSpanned(targetSpan);
            return inShardSize / totalSize;
        }

        public double rangeSpanned(PartitionPosition first, PartitionPosition last)
        {
            return ShardManager.this.rangeSpanned(first, last);
        }

        public int shardIndex()
        {
            return pos - 1;
        }
    }
}
