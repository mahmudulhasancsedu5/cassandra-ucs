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
package org.apache.cassandra.db.partitions;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.transform.Consumer;
import org.apache.cassandra.db.transform.Transformation;

public abstract class PurgeFunction extends Transformation<Unfiltered, UnfilteredRowIterator>
{
    private final boolean isForThrift;
    private final DeletionPurger purger;
    private final int gcBefore;
    private boolean isReverseOrder;

    public PurgeFunction(boolean isForThrift, int gcBefore, int oldestUnrepairedTombstone, boolean onlyPurgeRepairedTombstones)
    {
        this.isForThrift = isForThrift;
        this.gcBefore = gcBefore;
        this.purger = (timestamp, localDeletionTime) ->
                      !(onlyPurgeRepairedTombstones && localDeletionTime >= oldestUnrepairedTombstone)
                      && localDeletionTime < gcBefore
                      && timestamp < getMaxPurgeableTimestamp();
    }

    protected abstract long getMaxPurgeableTimestamp();

    // Called at the beginning of each new partition
    protected void onNewPartition(DecoratedKey partitionKey)
    {
    }

    // Called for each partition that had only purged infos and are empty post-purge.
    protected void onEmptyPartitionPostPurge(DecoratedKey partitionKey)
    {
    }

    // Called for every unfiltered. Meant for CompactionIterator to update progress
    protected void updateProgress()
    {
    }

    @Override
    public Consumer<UnfilteredRowIterator> applyAsPartitionConsumer(Consumer<UnfilteredRowIterator> nextConsumer)
    {
        return partition ->
        {
            onNewPartition(partition.partitionKey());

            isReverseOrder = partition.isReverseOrder();
            UnfilteredRowIterator purged = Transformation.apply(partition, this);
            if (!isForThrift && purged.isEmpty())
            {
                onEmptyPartitionPostPurge(purged.partitionKey());
                purged.close();
                return true;
            }

            return nextConsumer.accept(purged);
        };
    }

    @Override
    public DeletionTime applyToDeletion(DeletionTime deletionTime)
    {
        return purger.shouldPurge(deletionTime) ? DeletionTime.LIVE : deletionTime;
    }

    @Override
    public Row applyToStatic(Row row)
    {
        updateProgress();
        return row.purge(purger, gcBefore);
    }

    @Override
    public Consumer<Unfiltered> applyAsRowConsumer(Consumer<Unfiltered> nextConsumer)
    {
        return value ->
        {
            updateProgress();
            if (value instanceof Row)
                value = ((Row) value).purge(purger, gcBefore);
            else
                value = purgeMarker((RangeTombstoneMarker) value);
            return value == null || nextConsumer.accept(value);
        };
    }

    public RangeTombstoneMarker purgeMarker(RangeTombstoneMarker marker)
    {
        boolean reversed = isReverseOrder;
        if (marker.isBoundary())
        {
            // We can only skip the whole marker if both deletion time are purgeable.
            // If only one of them is, filterTombstoneMarker will deal with it.
            RangeTombstoneBoundaryMarker boundary = (RangeTombstoneBoundaryMarker)marker;
            boolean shouldPurgeClose = purger.shouldPurge(boundary.closeDeletionTime(reversed));
            boolean shouldPurgeOpen = purger.shouldPurge(boundary.openDeletionTime(reversed));

            if (shouldPurgeClose)
            {
                if (shouldPurgeOpen)
                    return null;

                return boundary.createCorrespondingOpenMarker(reversed);
            }

            return shouldPurgeOpen
                   ? boundary.createCorrespondingCloseMarker(reversed)
                   : marker;
        }
        else
        {
            return purger.shouldPurge(((RangeTombstoneBoundMarker)marker).deletionTime()) ? null : marker;
        }
    }
}