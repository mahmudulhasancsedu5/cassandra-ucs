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

package org.apache.cassandra.db;

import java.util.Set;

import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.UnifiedCompactionStrategy;
import org.apache.cassandra.db.compaction.unified.Environment;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.index.SecondaryIndexManager;
import org.apache.cassandra.io.sstable.format.SSTableBasicStats;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;


/**
 * An interface for supplying table data, or {@link SSTableBasicStats}, to modules that only
 * need this information, and create instances of {@link SSTableReader} lazily via {@link CompactionRealm#toSSTableReaders}.
 * <p/>
 * It is currently only used by {@link UnifiedCompactionStrategy}, and {@link CompactionController}.
 *
 * The idea would be that this would eventually manage all input dependencies to a compaction module, and other modules,
 * reducing the scope of CFS and readers. However, at the moment {@link ColumnFamilyStore} is still used in some places
 * even in the compaction code.
 */
public interface CompactionRealm<SSTABLE extends SSTableBasicStats>
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
     * @return the partitioner used by this table.
     */
    IPartitioner getPartitioner();

    /**
     * @return the schema metadata of this table.
     */
    TableMetadataRef metadataRef();

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
     * @return all live sstables, whether they are compacting or not.
     */
    Set<SSTABLE> getLiveSSTables();

    /**
     * @return all live sstables that are also taking part into a compaction.
     */
    Set<SSTABLE> getCompactingSSTables();

    /**
     * @return all live sstables that are not yet taking part into a compaction.
     */
    Iterable<SSTABLE> getNoncompactingSSTables();

    /**
     * @return the live sstables that overlap with the given sstables passed in.
     */
    Iterable<SSTABLE> getOverlappingLiveSSTables(Iterable<SSTABLE> sstables);

    /**
     * @return all live memtables, or empty if no memtables are available.
     */
    Iterable<Memtable> getAllMemtables();

    /**
     * Invalidate the given key from local caches.
     */
    void invalidateCachedPartition(DecoratedKey key);

    /**
     * Convert an abstract sstable data into a concrete reader. This is just a cast or a lookup
     * if the abstract sstable is already a full reader, but in some cases it may involve opening
     * a reader.
     */
    SSTableReader toSSTableReader(SSTABLE sstable);

    /**
     * Convert a set of abstract sstables into concrete readers. This is just a set of casts or lookups if
     * the abstract sstables are already full readers, but in some cases it may involve opening readers.
     */
    Iterable<SSTableReader> toSSTableReaders(Iterable<SSTABLE> sstables);
}
