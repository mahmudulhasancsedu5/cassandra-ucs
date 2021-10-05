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

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.UUID;

import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableUniqueIdentifier;

/**
 * An SSTable abstraction used by compaction. Implemented by {@link SSTableReader} and provided by
 * {@link CompactionRealm} instances.
 *
 * This abstraction is used to select the sstables to compact. When a compaction is initiated using
 * {@link CompactionRealm#tryModify}, the compaction operation receives the SSTableReaders corresponding to the passed
 * CompactionSSTables.
 */
public interface CompactionSSTable
{
    // Note: please do not replace with Comparator.comparing, this code can be on a hot path.
    Comparator<CompactionSSTable> maxTimestampDescending = (o1, o2) -> Long.compare(o2.getMaxTimestamp(), o1.getMaxTimestamp());
    Comparator<CompactionSSTable> maxTimestampAscending = (o1, o2) -> Long.compare(o1.getMaxTimestamp(), o2.getMaxTimestamp());
    Comparator<CompactionSSTable> firstKeyComparator = (o1, o2) -> o1.getFirst().compareTo(o2.getFirst());
    Ordering<CompactionSSTable> firstKeyOrdering = Ordering.from(firstKeyComparator);
    Comparator<CompactionSSTable> sizeComparator = (o1, o2) -> Longs.compare(o1.onDiskLength(), o2.onDiskLength());
    Comparator<CompactionSSTable> generationReverseComparator = Comparator.comparing(CompactionSSTable::getGeneration).reversed();

    /**
     * Returns the position of the first partition in the sstable
     */
    DecoratedKey getFirst();

    /**
     * Returns the position of the last partition in the sstable
     */
    DecoratedKey getLast();

    /**
     * Returns the bounds spanned by this sstable, from first to last keys.
     */
    AbstractBounds<Token> getBounds();

    /**
     * Returns the length in bytes of the on disk size for this SSTable. For compressed files, this is not the same
     * thing as the data length (see {@link #uncompressedLength})
     */
    long onDiskLength();

    /**
     * Returns the length in bytes of the data for this SSTable. For compressed files, this is not the same thing as the
     * on disk size (see {@link #onDiskLength})
     */
    long uncompressedLength();

    /**
     * Returns the sum of the on-disk size of the given sstables.
     */
    static long getTotalBytes(Iterable<? extends CompactionSSTable> sstables)
    {
        long sum = 0;
        for (CompactionSSTable sstable : sstables)
            sum += sstable.onDiskLength();
        return sum;
    }

    /**
     * Returns the sum of the uncompressed size of the given sstables.
     */
    static long getTotalUncompressedBytes(Iterable<? extends CompactionSSTable> sstables)
    {
        long sum = 0;
        for (CompactionSSTable sstable : sstables)
            sum += sstable.uncompressedLength();

        return sum;
    }

    /**
      * Returns the smallest timestamp of all cells contained in this sstable.
      */
    long getMinTimestamp();

    /**
      * Returns the largest timestamp of all cells contained in this sstable.
      */
    long getMaxTimestamp();

    /**
      * Returns the smallest deletion time of all deletions contained in this sstable.
      */
    int getMinLocalDeletionTime();

    /**
      * Returns the larget deletion time of all deletions contained in this sstable.
      */
    int getMaxLocalDeletionTime();

    /**
     * Called by {@link org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy} and other compaction strategies
     * to determine the read hotness of this sstables, this method returna a "read hotness" which is calculated by
     * looking at the last two hours read rate and dividing this number by the estimated number of keys.
     * <p/>
     * Note that some system tables do not have read meters, in which case this method will return zero.
     *
     * Returns the last two hours read rate per estimated key
     */
    double hotness();

    /**
      * Returns true if this sstable was repaired by a repair service, false otherwise.
      */
    boolean isRepaired();

    /**
     * Returns the time of repair when isRepaired is true, otherwise UNREPAIRED_SSTABLE.
     */
    long getRepairedAt();

    /**
      * Returns true if this sstable is pending repair, false otherwise.
      */
    boolean isPendingRepair();

    /**
     * Returns the id of the repair session when isPendingRepair is true, otherwise null.
     */
    UUID getPendingRepair();

    /**
     * Returns true if this sstable is transient.
     */
    boolean isTransient();

    /**
     * Returns an estimate of the number of keys in this SSTable based on the index summary.
     */
    long estimatedKeys();

    /**
      * Returns the level of this sstable according to {@link LeveledCompactionStrategy}, zero for other strategies.
      */
    int getSSTableLevel();

    /**
      * Returns true if this sstable can take part into a compaction.
      */
    boolean isSuitableForCompaction();

    /**
      * Returns true if this sstable was marked for obsoletion by a compaction.
      */
    boolean isMarkedCompacted();

    /**
      * Returns true if this sstable is suspect, that is it was involved in an operation that failed, such
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
     * Returns true if it is possible that the given key is contained in this sstable.
     */
    boolean couldContain(DecoratedKey key);

    Descriptor getDescriptor();
    default String getColumnFamilyName()
    {
        return getDescriptor().cfname;
    }
    default String getKeyspaceName()
    {
        return getDescriptor().ksname;
    }
    default SSTableUniqueIdentifier getGeneration()
    {
        return getDescriptor().generation;
    }

    /**
     * @param component component to get timestamp.
     * Returns last modified time for given component. 0 if given component does not exist or IO error occurs.
     */
    default long getCreationTimeFor(Component component)
    {
        return new File(getDescriptor().filenameFor(component)).lastModified();
    }

    /**
     * Returns an estimate of the ratio of the tombstones present in the sstable that could be dropped for the given
     * garbage collection threshold.
     */
    double getEstimatedDroppableTombstoneRatio(int gcBefore);

    /**
     * Changes the SSTable level as used by {@link LeveledCompactionStrategy}.
     * @throws IOException
     */
    void mutateSSTableLevelAndReload(int newLevel) throws IOException;
}
