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

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.PurgeFunction;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Transformation;
import org.apache.cassandra.index.transactions.CompactionTransaction;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.metrics.CompactionMetrics;

/**
 * Merge multiple iterators over the content of sstable into a "compacted" iterator.
 * <p>
 * On top of the actual merging the source iterators, this class:
 * <ul>
 *   <li>purge gc-able tombstones if possible (see PurgeIterator below).</li>
 *   <li>update 2ndary indexes if necessary (as we don't read-before-write on index updates, index entries are
 *       not deleted on deletion of the base table data, which is ok because we'll fix index inconsistency
 *       on reads. This however mean that potentially obsolete index entries could be kept a long time for
 *       data that is not read often, so compaction "pro-actively" fix such index entries. This is mainly
 *       an optimization).</li>
 *   <li>invalidate cached partitions that are empty post-compaction. This avoids keeping partitions with
 *       only purgable tombstones in the row cache.</li>
 *   <li>keep tracks of the compaction progress.</li>
 * </ul>
 */
public class CompactionIterator extends CompactionInfo.Holder implements UnfilteredPartitionIterator
{
    private static final Logger logger = LoggerFactory.getLogger(CompactionIterator.class);
    private static final long UNFILTERED_TO_UPDATE_PROGRESS = 100;

    private final OperationType type;
    private final CompactionController controller;
    private final List<ISSTableScanner> scanners;
    private final int nowInSec;
    private final UUID compactionId;

    private final long totalBytes;
    private long bytesRead;

    /*
     * counters for merged rows.
     * array index represents (number of merged rows - 1), so index 0 is counter for no merge (1 row),
     * index 1 is counter for 2 rows merged, and so on.
     */
    private final long[] mergeCounters;

    private final UnfilteredPartitionIterator compacted;
    private final CompactionMetrics metrics;

    public CompactionIterator(OperationType type, List<ISSTableScanner> scanners, CompactionController controller, int nowInSec, UUID compactionId)
    {
        this(type, scanners, controller, nowInSec, compactionId, null);
    }

    @SuppressWarnings("resource") // We make sure to close mergedIterator in close() and CompactionIterator is itself an AutoCloseable
    public CompactionIterator(OperationType type, List<ISSTableScanner> scanners, CompactionController controller, int nowInSec, UUID compactionId, CompactionMetrics metrics)
    {
        this.controller = controller;
        this.type = type;
        this.scanners = scanners;
        this.nowInSec = nowInSec;
        this.compactionId = compactionId;
        this.bytesRead = 0;

        long bytes = 0;
        for (ISSTableScanner scanner : scanners)
            bytes += scanner.getLengthInBytes();
        this.totalBytes = bytes;
        this.mergeCounters = new long[scanners.size()];
        this.metrics = metrics;

        if (metrics != null)
            metrics.beginCompaction(this);

        UnfilteredPartitionIterator merged = scanners.isEmpty()
                                             ? EmptyIterators.unfilteredPartition(controller.cfs.metadata, false)
                                             : UnfilteredPartitionIterators.merge(scanners, nowInSec, listener());
        boolean isForThrift = merged.isForThrift(); // to stop capture of iterator in Purger, which is confusing for debug
        merged = Transformation.apply(merged, new Deleter(controller, nowInSec));
        this.compacted = Transformation.apply(merged, new Purger(isForThrift, controller));
    }

    public boolean isForThrift()
    {
        return false;
    }

    public CFMetaData metadata()
    {
        return controller.cfs.metadata;
    }

    public CompactionInfo getCompactionInfo()
    {
        return new CompactionInfo(controller.cfs.metadata,
                                  type,
                                  bytesRead,
                                  totalBytes,
                                  compactionId);
    }

    private void updateCounterFor(int rows)
    {
        assert rows > 0 && rows - 1 < mergeCounters.length;
        mergeCounters[rows - 1] += 1;
    }

    public long[] getMergedRowCounts()
    {
        return mergeCounters;
    }

    private UnfilteredPartitionIterators.MergeListener listener()
    {
        return new UnfilteredPartitionIterators.MergeListener()
        {
            public UnfilteredRowIterators.MergeListener getRowMergeListener(DecoratedKey partitionKey, List<UnfilteredRowIterator> versions)
            {
                int merged = 0;
                for (UnfilteredRowIterator iter : versions)
                {
                    if (iter != null)
                        merged++;
                }

                assert merged > 0;

                CompactionIterator.this.updateCounterFor(merged);

                if (type != OperationType.COMPACTION || !controller.cfs.indexManager.hasIndexes())
                    return null;

                Columns statics = Columns.NONE;
                Columns regulars = Columns.NONE;
                for (UnfilteredRowIterator iter : versions)
                {
                    if (iter != null)
                    {
                        statics = statics.mergeTo(iter.columns().statics);
                        regulars = regulars.mergeTo(iter.columns().regulars);
                    }
                }
                final PartitionColumns partitionColumns = new PartitionColumns(statics, regulars);

                // If we have a 2ndary index, we must update it with deleted/shadowed cells.
                // we can reuse a single CleanupTransaction for the duration of a partition.
                // Currently, it doesn't do any batching of row updates, so every merge event
                // for a single partition results in a fresh cycle of:
                // * Get new Indexer instances
                // * Indexer::start
                // * Indexer::onRowMerge (for every row being merged by the compaction)
                // * Indexer::commit
                // A new OpOrder.Group is opened in an ARM block wrapping the commits
                // TODO: this should probably be done asynchronously and batched.
                final CompactionTransaction indexTransaction =
                    controller.cfs.indexManager.newCompactionTransaction(partitionKey,
                                                                         partitionColumns,
                                                                         versions.size(),
                                                                         nowInSec);

                return new UnfilteredRowIterators.MergeListener()
                {
                    public void onMergedPartitionLevelDeletion(DeletionTime mergedDeletion, DeletionTime[] versions)
                    {
                    }

                    public void onMergedRows(Row merged, Row[] versions)
                    {
                        indexTransaction.start();
                        indexTransaction.onRowMerge(merged, versions);
                        indexTransaction.commit();
                    }

                    public void onMergedRangeTombstoneMarkers(RangeTombstoneMarker mergedMarker, RangeTombstoneMarker[] versions)
                    {
                    }

                    public void close()
                    {
                    }
                };
            }

            public void close()
            {
            }
        };
    }

    private void updateBytesRead()
    {
        long n = 0;
        for (ISSTableScanner scanner : scanners)
            n += scanner.getCurrentPosition();
        bytesRead = n;
    }

    public boolean hasNext()
    {
        return compacted.hasNext();
    }

    public UnfilteredRowIterator next()
    {
        return compacted.next();
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }

    public void close()
    {
        try
        {
            compacted.close();
        }
        finally
        {
            if (metrics != null)
                metrics.finishCompaction(this);
        }
    }

    public String toString()
    {
        return this.getCompactionInfo().toString();
    }

    private class Purger extends PurgeFunction
    {
        private final CompactionController controller;

        private DecoratedKey currentKey;
        private long maxPurgeableTimestamp;
        private boolean hasCalculatedMaxPurgeableTimestamp;

        private long compactedUnfiltered;

        private Purger(boolean isForThrift, CompactionController controller)
        {
            super(isForThrift, controller.gcBefore, controller.compactingRepaired() ? Integer.MIN_VALUE : Integer.MAX_VALUE, controller.cfs.getCompactionStrategyManager().onlyPurgeRepairedTombstones());
            this.controller = controller;
        }

        @Override
        protected void onEmptyPartitionPostPurge(DecoratedKey key)
        {
            if (type == OperationType.COMPACTION)
                controller.cfs.invalidateCachedPartition(key);
        }

        @Override
        protected void onNewPartition(DecoratedKey key)
        {
            currentKey = key;
            hasCalculatedMaxPurgeableTimestamp = false;
        }

        @Override
        protected void updateProgress()
        {
            if ((++compactedUnfiltered) % UNFILTERED_TO_UPDATE_PROGRESS == 0)
                updateBytesRead();
        }

        /*
         * Tombstones with a localDeletionTime before this can be purged. This is the minimum timestamp for any sstable
         * containing `currentKey` outside of the set of sstables involved in this compaction. This is computed lazily
         * on demand as we only need this if there is tombstones and this a bit expensive (see #8914).
         */
        protected long getMaxPurgeableTimestamp()
        {
            if (!hasCalculatedMaxPurgeableTimestamp)
            {
                hasCalculatedMaxPurgeableTimestamp = true;
                maxPurgeableTimestamp = controller.maxPurgeableTimestamp(currentKey);
            }
            return maxPurgeableTimestamp;
        }
    }
    
    private static class Deleter extends Transformation<UnfilteredRowIterator>
    {
        final int nowInSec;
        final TombstoneOnly tombstoneOnly;
        UnfilteredRowIterator partitionSource;
        DeletionTime openDeletionTime = DeletionTime.LIVE;
        Unfiltered next;
        final ClusteringComparator comparator;
        final ColumnFilter cf;
        final CFMetaData metadata;
        final CompactionController controller;

        private Deleter(CompactionController controller, int nowInSec)
        {
            this.controller = controller;
            this.tombstoneOnly = new TombstoneOnly(nowInSec);
            this.nowInSec = nowInSec;
            metadata = controller.cfs.metadata;
            comparator = metadata.comparator;
            cf = ColumnFilter.all(metadata);
        }

        protected void onPartitionClose()
        {
            partitionSource.close();
        }

        @Override
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            Iterable<UnfilteredRowIterator> sources = controller.tombstoneSources(partition.partitionKey());
            if (sources == null)
                return partition;
            List<UnfilteredRowIterator> iters = new ArrayList<>();
            for (UnfilteredRowIterator iter : sources)
            {
                iter = Transformation.apply(iter, tombstoneOnly);
                if (!iter.isEmpty())
                    iters.add(iter);
                else
                    iter.close();
            }
            if (iters.isEmpty())
                return partition;

            partitionSource = UnfilteredRowIterators.merge(iters, nowInSec);
            next = partitionSource.next();
            return Transformation.apply(partition, this);
        }

        protected DeletionTime applyToDeletion(DeletionTime deletionTime)
        {
            return partitionSource.partitionLevelDeletion().supersedes(deletionTime) ? partitionSource.partitionLevelDeletion() : deletionTime;
        }

        protected Row applyToRow(Row row)
        {
            DeletionTime activeDeletionTime = proceedTo(row.clustering());
            return row.filter(cf, activeDeletionTime, false, metadata);
        }

        protected Row applyToStatic(Row row)
        {
            return applyToRow(row);
        }

        protected RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            DeletionTime activeDeletionTime = proceedTo(marker.clustering());
            assert activeDeletionTime == openDeletionTime;

            if (marker.isOpen(false))
            {
                if (activeDeletionTime.supersedes(marker.openDeletionTime(false)))
                    // remove open
                    if (!marker.isClose(false) || activeDeletionTime.supersedes(marker.closeDeletionTime(false)))
                        return null;
                    else
                        return new RangeTombstoneBoundMarker(marker.closeBound(false), marker.closeDeletionTime(false));
                else
                    // open stays
                    if (marker.isClose(false) && activeDeletionTime.supersedes(marker.closeDeletionTime(false)))
                        return new RangeTombstoneBoundMarker(marker.openBound(false), marker.openDeletionTime(false));
                    else
                        return marker;
            }
            assert marker.isClose(false);
            if (activeDeletionTime.supersedes(marker.closeDeletionTime(false)))
                return null;
            else
                return marker;
        }

        private DeletionTime proceedTo(Clusterable clustering)
        {
            while (true)
            {
                if (next == null)
                {
                    assert openDeletionTime == DeletionTime.LIVE;
                    return DeletionTime.LIVE;
                }

                int cmp = comparator.compare(next, clustering);
                if (cmp > 0)
                    return openDeletionTime;

                if (next.isRow())
                {
                    Row deletion = (Row) next;
                    if (cmp == 0)
                        return deletion.deletion().time();
                }
                else
                {
                    RangeTombstoneMarker marker = (RangeTombstoneMarker) next;
                    assert marker.isClose(false) == (openDeletionTime != DeletionTime.LIVE);
                    assert !marker.isClose(false) || openDeletionTime.equals(marker.closeDeletionTime(false));
                    openDeletionTime = marker.isOpen(false) ? marker.openDeletionTime(false) : DeletionTime.LIVE;
                }
                next = partitionSource.hasNext() ? partitionSource.next() : null;
            }
        }
    }

    UnfilteredPartitionIterator tombstonesOnly(UnfilteredPartitionIterator iter)
    {
        return Transformation.apply(iter, new TombstoneOnly(nowInSec));
    }

    public static class TombstoneOnly extends Transformation<UnfilteredRowIterator>
    {
        private final int nowInSec;

        public TombstoneOnly(int nowInSec)
        {
            super();
            this.nowInSec = nowInSec;
        }

        @Override
        protected UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            return Transformation.apply(partition, this);
        }

        @Override
        protected Row applyToRow(Row row)
        {
            if (!row.hasDeletion(nowInSec))
                return null;

            return BTreeRow.emptyDeletedRow(row.clustering(), row.deletion());
        }

        @Override
        protected Row applyToStatic(Row row)
        {
            if (!row.hasDeletion(nowInSec))
                return Rows.EMPTY_STATIC_ROW;

            return BTreeRow.emptyDeletedRow(row.clustering(), row.deletion());
        }
    }
}
