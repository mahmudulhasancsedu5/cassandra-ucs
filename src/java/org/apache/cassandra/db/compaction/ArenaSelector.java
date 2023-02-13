/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.db.compaction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SortedLocalRanges;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;

/**
 * Arena selector, used by UnifiedCompactionStrategy to distribute SSTables to separate compaction arenas.
 *
 * This is used to:
 * - ensure that sstables that should not be compacted together (e.g. repaired with unrepaired) are separated
 * - ensure that each disk's sstables are compacted separately
 * - implement compaction shards, subsections of the token space which compact separately for improved parallelism
 *   and compaction overheads.
 */
public class ArenaSelector implements Comparator<CompactionSSTable>
{
    private final EquivClassSplitter[] classSplitters;
    final Controller controller;
    final DiskBoundaries diskBoundaries;

    final ShardManager shardManager;

    public ArenaSelector(Controller controller, DiskBoundaries diskBoundaries, List<Token> shardBoundaries, SortedLocalRanges localRanges)
    {
        shardManager = new ShardManager(shardBoundaries.toArray(new Token[shardBoundaries.size()]), localRanges);
        this.controller = controller;
        this.diskBoundaries = diskBoundaries;

        ArrayList<EquivClassSplitter> ret = new ArrayList<>(2);

        ret.add(RepairEquivClassSplitter.INSTANCE);

        if (diskBoundaries.getPositions() != null)
        {
            // The shard boundaries must also split on disks. Verify it.
            assert new HashSet<>(shardBoundaries).containsAll(diskBoundaries.getPositions());
        }
        else if (diskBoundaries.getNumBoundaries() > 1)
        {
            // We end up here if there are multiple disks, but not assigned according to token range.
            ret.add(new DiskIndexEquivClassSplitter());
        }

        if (shardBoundaries.size() > 1)
            ret.add(new ShardEquivClassSplitter());

        classSplitters = ret.toArray(new EquivClassSplitter[0]);
    }

    public ArenaSelector nonShardingSelector()
    {
        return new ArenaSelector(controller,
                                 diskBoundaries,
                                 diskBoundaries.getPositions() == null ? Collections.EMPTY_LIST
                                                                       : diskBoundaries.getPositions(),
                                 shardManager.localRanges);
    }

    @Override
    public int compare(CompactionSSTable o1, CompactionSSTable o2)
    {
        int res = 0;
        for (int i = 0; res == 0 && i < classSplitters.length; i++)
            res = classSplitters[i].compare(o1, o2);
        return res;
    }

    public String name(CompactionSSTable t)
    {
        return Arrays.stream(classSplitters)
                     .map(e -> e.name(t))
                     .collect(Collectors.joining("-"));
    }

    public int shardFor(PartitionPosition pos)
    {
        return shardManager.shardFor(pos);
    }

    /**
     * An equivalence class is a function that compares two sstables and returns 0 when they fall in the same class.
     * For example, the repair status or disk index may define equivalence classes. See the concrete equivalence classes below.
     */
    private interface EquivClassSplitter extends Comparator<CompactionSSTable> {

        @Override
        int compare(CompactionSSTable a, CompactionSSTable b);

        /** Return a name that describes the equivalence class */
        String name(CompactionSSTable ssTableReader);
    }

    /**
     * Split sstables by their repair state: repaired, unrepaired, pending repair with a specific UUID (one group per pending repair).
     */
    private static final class RepairEquivClassSplitter implements EquivClassSplitter
    {
        public static final EquivClassSplitter INSTANCE = new RepairEquivClassSplitter();

        @Override
        public int compare(CompactionSSTable a, CompactionSSTable b)
        {
            // This is the same as name(a).compareTo(name(b))
            int af = a.isRepaired() ? 1 : !a.isPendingRepair() ? 2 : 0;
            int bf = b.isRepaired() ? 1 : !b.isPendingRepair() ? 2 : 0;
            if (af != 0 || bf != 0)
                return Integer.compare(af, bf);
            return a.getPendingRepair().compareTo(b.getPendingRepair());
        }

        @Override
        public String name(CompactionSSTable ssTableReader)
        {
            if (ssTableReader.isRepaired())
                return "repaired";
            else if (!ssTableReader.isPendingRepair())
                return "unrepaired";
            else
                return "pending_repair_" + ssTableReader.getPendingRepair();
        }
    }

    /**
     * Return the shard for an sstable, which may be fixed to zero for L0 sstables if shards are disabled on L0
     */
    public int shardFor(CompactionSSTable ssTableReader)
    {
        // If shards on L0 are disabled and the size of the sstable is less than the max L0 size, always pick the fist shard
        if (!controller.areL0ShardsEnabled() && shardManager.density(ssTableReader) < controller.getMaxL0Density())
            return 0;

        return shardManager.shardFor(ssTableReader.getFirst());
    }

    /**
     * Split sstables by their shard. If the data set size is larger than the shard size in the compaction options,
     * then we create an equivalence class based by shard. Each sstable ends up in a shard based on their first
     * key. Each shard is calculated by splitting the local token ranges into a number of shards, where the number
     * of shards is calculated as ceil(data_size / shard size);
     *
     * Shard boundaries also split the sstables that reside on different disks.
     */
    private final class ShardEquivClassSplitter implements EquivClassSplitter
    {
        @Override
        public int compare(CompactionSSTable a, CompactionSSTable b)
        {
            return Integer.compare(shardFor(a), shardFor(b));
        }

        @Override
        public String name(CompactionSSTable ssTableReader)
        {
            return "shard_" + shardFor(ssTableReader);
        }
    }

    /**
     * Group sstables by their disk index.
     */
    private final class DiskIndexEquivClassSplitter implements EquivClassSplitter
    {
        @Override
        public int compare(CompactionSSTable a, CompactionSSTable b)
        {
            return Integer.compare(diskBoundaries.getDiskIndexFromKey(a), diskBoundaries.getDiskIndexFromKey(b));
        }

        @Override
        public String name(CompactionSSTable ssTableReader)
        {
            return "disk_" + diskBoundaries.getDiskIndexFromKey(ssTableReader);
        }
    }


    // TODO - missing equivalence classes:

    // - by time window to emulate TWCS, in this case only the latest shard will use size based buckets, the older
    //   shards will get major compactions
}
