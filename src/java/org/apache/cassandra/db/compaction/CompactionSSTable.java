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

import java.util.Comparator;
import java.util.UUID;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.CorruptSSTableException;

public interface CompactionSSTable
{
    Comparator<CompactionSSTable> maxTimestampDescending = (o1, o2) -> Long.compare(o2.getMaxTimestamp(), o1.getMaxTimestamp());
    Comparator<CompactionSSTable> maxTimestampAscending = (o1, o2) -> Long.compare(o1.getMaxTimestamp(), o2.getMaxTimestamp());
    Comparator<CompactionSSTable> firstKeyComparator = (o1, o2) -> o1.getFirst().compareTo(o2.getFirst());
    Ordering<CompactionSSTable> firstKeyOrdering = Ordering.from(firstKeyComparator);
    Comparator<? super CompactionSSTable> sizeComparator = (o1, o2) -> Longs.compare(o1.onDiskLength(), o2.onDiskLength());

    /**
     * @return the position of the first partition in the sstable
     */
    DecoratedKey getFirst();

    /**
     * @return the position of the last partition in the sstable
     */
    DecoratedKey getLast();

    /**
     * @return the bounds spanned by this sstable, from first to last keys.
     */
    AbstractBounds<Token> getBounds();

    /**
     * @return The length in bytes of the on disk size for this SSTable. For compressed files, this is not the same
     * thing as the data length (see length())
     */
    long onDiskLength();

    /**
     * @return The length in bytes of the data for this SSTable. For compressed files, this is not the same thing as the
     * on disk size (see onDiskLength())
     */
    long uncompressedLength();

    static long getTotalBytes(Iterable<? extends CompactionSSTable> sstables)
    {
        long sum = 0;
        for (CompactionSSTable sstable : sstables)
            sum += sstable.onDiskLength();
        return sum;
    }

    static long getTotalUncompressedBytes(Iterable<? extends CompactionSSTable> sstables)
    {
        long sum = 0;
        for (CompactionSSTable sstable : sstables)
            sum += sstable.uncompressedLength();

        return sum;
    }

    /**
      * @return the smallest timestamp of all cells contained in this sstable.
      */
    long getMinTimestamp();

    /**
      * @return the largest timestamp of all cells contained in this sstable.
      */
    long getMaxTimestamp();

    /**
      * @return the smallest deletion time of all deletions contained in this sstable.
      */
    int getMinLocalDeletionTime();

    /**
      * @return the larget deletion time of all deletions contained in this sstable.
      */
    int getMaxLocalDeletionTime();

    /**
     * Called by {@link org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy} and other compaction strategies
     * to determine the read hotness of this sstables, this method returna a "read hotness" which is calculated by
     * looking at the last two hours read rate and dividing this number by the estimated number of keys.
     * <p/>
     * Note that some system tables do not have read meters, in which case this method will return zero.
     *
     * @return the last two hours read rate per estimated key
     */
    double hotness();

    /**
      * @return true if this sstable was repaired by a repair service, false otherwise.
      */
    boolean isRepaired();

    /**
     * @return the time of repair when isRepaired is true, otherwise UNREPAIRED_SSTABLE.
     */
    long getRepairedAt();

    /**
      * @return true if this sstable is pending repair, false otherwise.
      */
    boolean isPendingRepair();

    /**
     * @return the id of the repair session when isPendingRepair is true, otherwise null.
     */
    UUID getPendingRepair();

    /**
     * @return An estimate of the number of keys in this SSTable based on the index summary.
     */
    long estimatedKeys();

    /**
      * @return the level of this sstable according to {@link LeveledCompactionStrategy}, zero for other strategies.
      */
    int getSSTableLevel();

    /**
      * @return true if this sstable can take part into a compaction.
      */
    boolean isSuitableForCompaction();

    /**
      * @return true if this sstable was marked for obsoletion by a compaction.
      */
    boolean isMarkedCompacted();

    /**
      * @return true if this sstable is suspect, that is it was involved in an operation that failed, such
      *         as a write or read that resulted in {@link CorruptSSTableException}.
      */
    boolean isMarkedSuspect();

    /**
     * Whether the sstable may contain tombstones or if it is guaranteed to not contain any.
     * <p>
     * Note that having that method return {@code false} guarantees the sstable has no tombstones whatsoever (so no cell
     * tombstone, no range tombstone maker and no expiring columns), but having it return {@code true} doesn't guarantee
     * it contains any as it may simply have non-expired cells.
     */
    boolean mayHaveTombstones();

    /**
     * @return true if it is possible that the given key is contained in this sstable.
     */
    boolean couldContain(DecoratedKey key);

    /**
     * @return the index of the disk storing this sstable, according to the boundaries passed in.
     */
    int getDiskIndex(DiskBoundaries diskBoundaries);

    /**
      * @return the exact position of the given key in this sstable, or null if this is not supported or available.
      */
//    @Nullable
//    RowIndexEntry getExactPosition(DecoratedKey key);
}
