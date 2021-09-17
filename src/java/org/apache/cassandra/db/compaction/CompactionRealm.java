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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.ScannerList;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.utils.concurrent.Refs;


/**
 * An interface for supplying table data, or {@link CompactionSSTable}, to modules that only
 * need this information, and create instances of {@link SSTableReader} lazily via {@link CompactionRealm#tryModify}.
 * <p/>
 * It is currently only used by {@link UnifiedCompactionStrategy}, and {@link CompactionController}.
 *
 * The idea would be that this would eventually manage all input dependencies to a compaction module, and other modules,
 * reducing the scope of CFS and readers. However, at the moment {@link ColumnFamilyStore} is still used in some places
 * even in the compaction code.
 */
public interface CompactionRealm
{
    /**
     * @return the {@link Directories} backing this table.
     */
    Directories getDirectories();

    /**
     * @return the {@DiskBoundaries} that are currently applied to the directories backing table.
     */
    DiskBoundaries getDiskBoundaries();

    /**
     * @return the schema metadata of this table as a reference, used for long-living objects to keep up-to-date with
     *         changes.
     */
    TableMetadataRef metadataRef();

    /**
     * @return the schema metadata of this table.
     */
    default TableMetadata metadata()
    {
        return metadataRef().get();
    }

    default String getTableName()
    {
        return metadata().name;
    }

    default String getKeyspaceName()
    {
        return metadata().keyspace;
    }

    /**
     * @return the partitioner used by this table.
     */
    default IPartitioner getPartitioner()
    {
        return metadata().partitioner;
    }

    /**
     * @return metrics object for the realm, if available.
     */
    TableMetrics metrics();

    /**
     * @return the secondary index manager, which is responsible for all secondary indexes.
     */
    SecondaryIndexManager getIndexManager();

    /**
     * @return true if this sstable is backed by remote storage.
     */
    default boolean isRemote()
    {
        return false;
    }

    /**
     * @return true if tombstones should be purged only from repaired sstables.
     */
    boolean onlyPurgeRepairedTombstones();

    /**
     * Inserts the live sstables that overlap with the given sstables into the collection in the first argument.
     */
    void addOverlappingLiveSSTables(Set<? super SSTableReader> overlaps,
                                    Iterable<? extends CompactionSSTable> sstables);

    default Set<CompactionSSTable> getOverlappingLiveSSTables(Iterable<? extends CompactionSSTable> sstables)
    {
        Set<CompactionSSTable> overlaps = new HashSet<>();
        addOverlappingLiveSSTables(overlaps, sstables);
        return overlaps;
    }

    /**
     * Invalidate the given key from local caches.
     */
    void invalidateCachedPartition(DecoratedKey key);

    LifecycleTransaction tryModify(Iterable<? extends CompactionSSTable> sstables, OperationType operationType);
    Refs<SSTableReader> getAndReferenceOverlappingLiveSSTables(Iterable<SSTableReader> sstables);
    ScannerList getScanners(Set<SSTableReader> actuallyCompact);

    boolean isCompactionActive();
    CompactionParams getCompactionParams();
    boolean getNeverPurgeTombstones();
    int getMinimumCompactionThreshold();
    int getMaximumCompactionThreshold();
    int getLevelFanoutSize();
    boolean supportsEarlyOpen();
    long getExpectedCompactedFileSize(Iterable<SSTableReader> sstables, OperationType operationType);
    boolean isCompactionDiskSpaceCheckEnabled();
    long getMaxSSTableBytes();

    /**
     * @return all live memtables, or empty if no memtables are available.
     */
    Iterable<Memtable> getAllMemtables();
    Set<SSTableReader> getCompactingSSTables();
    Iterable<SSTableReader> getNoncompactingSSTables();
    Iterable<SSTableReader> getNoncompactingSSTables(Iterable<SSTableReader> sstables);
    Set<SSTableReader> getLiveSSTables();

    Descriptor newSSTableDescriptor(File locationForDisk);

    void notifySSTableMetadataChanged(SSTableReader sstable, StatsMetadata metadataBefore);

    void snapshotWithoutMemtable(String snapshotId);
}
