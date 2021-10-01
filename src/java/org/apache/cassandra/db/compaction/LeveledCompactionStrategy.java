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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.primitives.Doubles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.ScannerList;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.TableMetadata;

public class LeveledCompactionStrategy extends LegacyAbstractCompactionStrategy.WithAggregates
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledCompactionStrategy.class);
    static final String SSTABLE_SIZE_OPTION = "sstable_size_in_mb";
    private static final boolean tolerateSstableSize = Boolean.getBoolean(Config.PROPERTY_PREFIX + "tolerate_sstable_size");
    static final String LEVEL_FANOUT_SIZE_OPTION = "fanout_size";
    private static final String SINGLE_SSTABLE_UPLEVEL_OPTION = "single_sstable_uplevel";
    public static final int DEFAULT_LEVEL_FANOUT_SIZE = 10;

    @VisibleForTesting
    final LeveledManifest manifest;
    private final int maxSSTableSizeInMB;
    private final int levelFanoutSize;
    private final boolean singleSSTableUplevel;

    public LeveledCompactionStrategy(CompactionStrategyFactory factory, Map<String, String> options)
    {
        super(factory, options);
        int configuredMaxSSTableSize = 160;
        int configuredLevelFanoutSize = DEFAULT_LEVEL_FANOUT_SIZE;
        boolean configuredSingleSSTableUplevel = false;
        SizeTieredCompactionStrategyOptions localOptions = new SizeTieredCompactionStrategyOptions(options);
        if (options != null)
        {
            if (options.containsKey(SSTABLE_SIZE_OPTION))
            {
                configuredMaxSSTableSize = Integer.parseInt(options.get(SSTABLE_SIZE_OPTION));
                if (!tolerateSstableSize)
                {
                    if (configuredMaxSSTableSize >= 1000)
                        logger.warn("Max sstable size of {}MB is configured for {}.{}; having a unit of compaction this large is probably a bad idea",
                                    configuredMaxSSTableSize, realm.getKeyspaceName(), realm.getTableName());
                    if (configuredMaxSSTableSize < 50)
                        logger.warn("Max sstable size of {}MB is configured for {}.{}.  Testing done for CASSANDRA-5727 indicates that performance improves up to 160MB",
                                    configuredMaxSSTableSize, realm.getKeyspaceName(), realm.getTableName());
                }
            }

            if (options.containsKey(LEVEL_FANOUT_SIZE_OPTION))
            {
                configuredLevelFanoutSize = Integer.parseInt(options.get(LEVEL_FANOUT_SIZE_OPTION));
            }

            if (options.containsKey(SINGLE_SSTABLE_UPLEVEL_OPTION))
            {
                configuredSingleSSTableUplevel = Boolean.parseBoolean(options.get(SINGLE_SSTABLE_UPLEVEL_OPTION));
            }
        }
        maxSSTableSizeInMB = configuredMaxSSTableSize;
        levelFanoutSize = configuredLevelFanoutSize;
        singleSSTableUplevel = configuredSingleSSTableUplevel;

        manifest = new LeveledManifest(realm, this.maxSSTableSizeInMB, this.levelFanoutSize, localOptions);
        logger.trace("Created {}", manifest);
    }

    int getLevelSize(int i)
    {
        return manifest.getLevelSize(i);
    }

    public int[] getAllLevelSize()
    {
        return manifest.getAllLevelSize();
    }

    @Override
    public int[] getSSTableCountPerLevel()
    {
        return manifest.getSSTableCountPerLevel();
    }

    @Override
    public void startup()
    {
        manifest.calculateLastCompactedKeys();
        super.startup();
    }

    @Override
    protected CompactionAggregate getNextBackgroundAggregate(int gcBefore)
    {
        CompactionAggregate.Leveled candidate = manifest.getCompactionCandidate();
        backgroundCompactions.setPending(this, manifest.getEstimatedTasks(candidate));

        if (candidate != null)
            return candidate;

        return findDroppableSSTable(gcBefore);
    }

    @Override
    protected AbstractCompactionTask createCompactionTask(final int gcBefore, LifecycleTransaction txn, CompactionAggregate compaction)
    {
        long maxxSSTableBytes;
        int nextLevel;
        OperationType op;

        if (compaction instanceof CompactionAggregate.TombstoneAggregate)
        {
            op = OperationType.TOMBSTONE_COMPACTION;
            nextLevel = Iterables.getOnlyElement(compaction.selected.sstables).getSSTableLevel();
            maxxSSTableBytes = getMaxSSTableBytes();    // TODO: verify this is expected as it can split L0 tables
        }
        else
        {
            CompactionAggregate.Leveled candidate = (CompactionAggregate.Leveled) compaction;
            op = OperationType.COMPACTION;
            nextLevel = candidate.nextLevel;
            maxxSSTableBytes = candidate.maxSSTableBytes;
        }


        AbstractCompactionTask newTask;
        if (!singleSSTableUplevel || op == OperationType.TOMBSTONE_COMPACTION || txn.originals().size() > 1)
            newTask = new LeveledCompactionTask(this, txn, nextLevel, gcBefore, maxxSSTableBytes, false);
        else
            newTask = new SingleSSTableLCSTask(this, txn, nextLevel);

        newTask.setCompactionType(op);
        return newTask;
    }


    @Override
    protected AbstractCompactionTask createCompactionTask(final int gcBefore, LifecycleTransaction txn, boolean isMaximal, boolean splitOutput)
    {
        Collection<SSTableReader> sstables = txn.originals();
        int level = sstables.size() > 1 ? 0 : sstables.iterator().next().getSSTableLevel();
        long maxSSTableBytes = (level == 0 && !isMaximal) ? Long.MAX_VALUE : getMaxSSTableBytes();
        return new LeveledCompactionTask(this, txn, level, gcBefore, maxSSTableBytes, isMaximal);
    }

    @Override
    public AbstractCompactionTask createCompactionTask(LifecycleTransaction txn, int gcBefore, long maxSSTableBytes)
    {
        assert txn.originals().size() > 0;
        int level = -1;
        // if all sstables are in the same level, we can set that level:
        for (SSTableReader sstable : txn.originals())
        {
            if (level == -1)
                level = sstable.getSSTableLevel();
            if (level != sstable.getSSTableLevel())
                level = 0;
        }
        return new LeveledCompactionTask(this, txn, level, gcBefore, maxSSTableBytes, false);
    }

    /**
     * Leveled compaction strategy has guarantees on the data contained within each level so we
     * have to make sure we only create groups of SSTables with members from the same level.
     * This way we won't end up creating invalid sstables during anti-compaction.
     * @param ssTablesToGroup
     * @return Groups of sstables from the same level
     */
    @Override
    public Collection<Collection<CompactionSSTable>> groupSSTablesForAntiCompaction(Collection<? extends CompactionSSTable> ssTablesToGroup)
    {
        int groupSize = 2;
        Map<Integer, Collection<CompactionSSTable>> sstablesByLevel = new HashMap<>();
        for (CompactionSSTable sstable : ssTablesToGroup)
        {
            Integer level = sstable.getSSTableLevel();
            Collection<CompactionSSTable> sstablesForLevel = sstablesByLevel.get(level);
            if (sstablesForLevel == null)
            {
                sstablesForLevel = new ArrayList<CompactionSSTable>();
                sstablesByLevel.put(level, sstablesForLevel);
            }
            sstablesForLevel.add(sstable);
        }

        Collection<Collection<CompactionSSTable>> groupedSSTables = new ArrayList<>();

        for (Collection<CompactionSSTable> levelOfSSTables : sstablesByLevel.values())
        {
            Collection<CompactionSSTable> currGroup = new ArrayList<>(groupSize);
            for (CompactionSSTable sstable : levelOfSSTables)
            {
                currGroup.add(sstable);
                if (currGroup.size() == groupSize)
                {
                    groupedSSTables.add(currGroup);
                    currGroup = new ArrayList<>(groupSize);
                }
            }

            if (currGroup.size() != 0)
                groupedSSTables.add(currGroup);
        }
        return groupedSSTables;

    }

    public long getMaxSSTableBytes()
    {
        return maxSSTableSizeInMB * 1024L * 1024L;
    }

    public int getLevelFanoutSize()
    {
        return levelFanoutSize;
    }

    public ScannerList getScanners(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
    {
        Set<CompactionSSTable>[] sstablesPerLevel = manifest.getSStablesPerLevelSnapshot();

        Multimap<Integer, SSTableReader> byLevel = ArrayListMultimap.create();
        for (SSTableReader sstable : sstables)
        {
            int level = sstable.getSSTableLevel();
            // if an sstable is not on the manifest, it was recently added or removed
            // so we add it to level -1 and create exclusive scanners for it - see below (#9935)
            if (level >= sstablesPerLevel.length || !sstablesPerLevel[level].contains(sstable))
            {
                logger.warn("Live sstable {} from level {} is not on corresponding level in the leveled manifest." +
                            " This is not a problem per se, but may indicate an orphaned sstable due to a failed" +
                            " compaction not cleaned up properly.",
                             sstable.getFilename(), level);
                level = -1;
            }
            byLevel.get(level).add(sstable);
        }

        List<ISSTableScanner> scanners = new ArrayList<ISSTableScanner>(sstables.size());
        try
        {
            for (Integer level : byLevel.keySet())
            {
                // level can be -1 when sstables are added to Tracker but not to LeveledManifest
                // since we don't know which level those sstable belong yet, we simply do the same as L0 sstables.
                if (level <= 0)
                {
                    // L0 makes no guarantees about overlapping-ness.  Just create a direct scanner for each
                    for (SSTableReader sstable : byLevel.get(level))
                        scanners.add(sstable.getScanner(ranges));
                }
                else
                {
                    // Create a LeveledScanner that only opens one sstable at a time, in sorted order
                    Collection<SSTableReader> intersecting = LeveledScanner.intersecting(byLevel.get(level), ranges);
                    if (!intersecting.isEmpty())
                    {
                        @SuppressWarnings("resource") // The ScannerList will be in charge of closing (and we close properly on errors)
                        ISSTableScanner scanner = new LeveledScanner(realm.metadata(), intersecting, ranges, level);
                        scanners.add(scanner);
                    }
                }
            }
        }
        catch (Throwable t)
        {
            ISSTableScanner.closeAllAndPropagate(scanners, t);
        }

        return new ScannerList(scanners);
    }

    @Override
    public void replaceSSTables(Collection<CompactionSSTable> removed, Collection<CompactionSSTable> added)
    {
        manifest.replace(removed, added);
    }

    public void metadataChanged(StatsMetadata oldMetadata, CompactionSSTable sstable)
    {
        if (sstable.getSSTableLevel() != oldMetadata.sstableLevel)
            manifest.newLevel(sstable, oldMetadata.sstableLevel);
    }

    @Override
    public void addSSTables(Iterable<CompactionSSTable> sstables)
    {
        manifest.addSSTables(sstables);
    }

    @Override
    void removeDeadSSTables()
    {
        manifest.removeDeadSSTables();
    }

    @Override
    public void addSSTable(CompactionSSTable added)
    {
        manifest.addSSTables(Collections.singleton(added));
    }

    @Override
    public void removeSSTable(CompactionSSTable sstable)
    {
        manifest.remove(sstable);
    }

    @Override
    public Set<CompactionSSTable> getSSTables()
    {
        return manifest.getSSTables();
    }

    // Lazily creates SSTableBoundedScanner for sstable that are assumed to be from the
    // same level (e.g. non overlapping) - see #4142
    private static class LeveledScanner extends AbstractIterator<UnfilteredRowIterator> implements ISSTableScanner
    {
        private final TableMetadata metadata;
        private final Collection<Range<Token>> ranges;
        private final List<SSTableReader> sstables;
        private final int level;
        private final Iterator<SSTableReader> sstableIterator;
        private final long totalLength;
        private final long compressedLength;

        private ISSTableScanner currentScanner;
        private long positionOffset;
        private long totalBytesScanned = 0;

        public LeveledScanner(TableMetadata metadata, Collection<SSTableReader> sstables, Collection<Range<Token>> ranges, int level)
        {
            this.metadata = metadata;
            this.ranges = ranges;

            // add only sstables that intersect our range, and estimate how much data that involves
            this.sstables = new ArrayList<>(sstables.size());
            this.level = level;
            long length = 0;
            long cLength = 0;
            for (SSTableReader sstable : sstables)
            {
                this.sstables.add(sstable);
                long estimatedKeys = sstable.estimatedKeys();
                double estKeysInRangeRatio = 1.0;

                if (estimatedKeys > 0 && ranges != null)
                    estKeysInRangeRatio = ((double) sstable.estimatedKeysForRanges(ranges)) / estimatedKeys;

                length += sstable.uncompressedLength() * estKeysInRangeRatio;
                cLength += sstable.onDiskLength() * estKeysInRangeRatio;
            }

            totalLength = length;
            compressedLength = cLength;
            Collections.sort(this.sstables, SSTableReader.firstKeyComparator);
            sstableIterator = this.sstables.iterator();
            assert sstableIterator.hasNext(); // caller should check intersecting first
            SSTableReader currentSSTable = sstableIterator.next();
            currentScanner = currentSSTable.getScanner(ranges);

        }

        public static Collection<SSTableReader> intersecting(Collection<SSTableReader> sstables, Collection<Range<Token>> ranges)
        {
            if (ranges == null)
                return Lists.newArrayList(sstables);

            Set<SSTableReader> filtered = new HashSet<>();
            for (Range<Token> range : ranges)
            {
                for (SSTableReader sstable : sstables)
                {
                    Range<Token> sstableRange = new Range<>(sstable.first.getToken(), sstable.last.getToken());
                    if (range == null || sstableRange.intersects(range))
                        filtered.add(sstable);
                }
            }
            return filtered;
        }

        public TableMetadata metadata()
        {
            return metadata;
        }

        protected UnfilteredRowIterator computeNext()
        {
            if (currentScanner == null)
                return endOfData();

            while (true)
            {
                if (currentScanner.hasNext())
                    return currentScanner.next();

                positionOffset += currentScanner.getLengthInBytes();
                totalBytesScanned += currentScanner.getBytesScanned();

                currentScanner.close();
                if (!sstableIterator.hasNext())
                {
                    // reset to null so getCurrentPosition does not return wrong value
                    currentScanner = null;
                    return endOfData();
                }
                SSTableReader currentSSTable = sstableIterator.next();
                currentScanner = currentSSTable.getScanner(ranges);
            }
        }

        public void close()
        {
            if (currentScanner != null)
                currentScanner.close();
        }

        public long getLengthInBytes()
        {
            return totalLength;
        }

        public long getCurrentPosition()
        {
            return positionOffset + (currentScanner == null ? 0L : currentScanner.getCurrentPosition());
        }

        public long getCompressedLengthInBytes()
        {
            return compressedLength;
        }

        public long getBytesScanned()
        {
            return currentScanner == null ? totalBytesScanned : totalBytesScanned + currentScanner.getBytesScanned();
        }

        public Set<SSTableReader> getBackingSSTables()
        {
            return ImmutableSet.copyOf(sstables);
        }

        public int level()
        {
            return level;
        }
    }

    @Override
    public String toString()
    {
        return String.format("LCS@%d(%s)", hashCode(), realm.getTableName());
    }

    private CompactionAggregate findDroppableSSTable(final int gcBefore)
    {
        Comparator<CompactionSSTable> comparator = (o1, o2) -> {
            double r1 = o1.getEstimatedDroppableTombstoneRatio(gcBefore);
            double r2 = o2.getEstimatedDroppableTombstoneRatio(gcBefore);
            return -1 * Doubles.compare(r1, r2);
        };
        Function<Collection<CompactionSSTable>, CompactionSSTable> selector = list -> Collections.max(list, comparator);
        Set<? extends CompactionSSTable> compacting = realm.getCompactingSSTables();

        for (int i = manifest.getLevelCount(); i >= 0; i--)
        {
            CompactionAggregate tombstoneAggregate = makeTombstoneCompaction(gcBefore,
                                                                             nonSuspectAndNotIn(manifest.getLevel(i), compacting),
                                                                             selector);
            if (tombstoneAggregate != null)
                return tombstoneAggregate;
        }
        return null;
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        Map<String, String> uncheckedOptions = CompactionStrategyOptions.validateOptions(options);

        String size = options.containsKey(SSTABLE_SIZE_OPTION) ? options.get(SSTABLE_SIZE_OPTION) : "1";
        try
        {
            int ssSize = Integer.parseInt(size);
            if (ssSize < 1)
            {
                throw new ConfigurationException(String.format("%s must be larger than 0, but was %s", SSTABLE_SIZE_OPTION, ssSize));
            }
        }
        catch (NumberFormatException ex)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", size, SSTABLE_SIZE_OPTION), ex);
        }

        uncheckedOptions.remove(SSTABLE_SIZE_OPTION);

        // Validate the fanout_size option
        String levelFanoutSize = options.containsKey(LEVEL_FANOUT_SIZE_OPTION) ? options.get(LEVEL_FANOUT_SIZE_OPTION) : String.valueOf(DEFAULT_LEVEL_FANOUT_SIZE);
        try
        {
            int fanoutSize = Integer.parseInt(levelFanoutSize);
            if (fanoutSize < 1)
            {
                throw new ConfigurationException(String.format("%s must be larger than 0, but was %s", LEVEL_FANOUT_SIZE_OPTION, fanoutSize));
            }
        }
        catch (NumberFormatException ex)
        {
            throw new ConfigurationException(String.format("%s is not a parsable int (base10) for %s", size, LEVEL_FANOUT_SIZE_OPTION), ex);
        }

        uncheckedOptions.remove(LEVEL_FANOUT_SIZE_OPTION);
        uncheckedOptions.remove(SINGLE_SSTABLE_UPLEVEL_OPTION);

        uncheckedOptions.remove(CompactionParams.Option.MIN_THRESHOLD.toString());
        uncheckedOptions.remove(CompactionParams.Option.MAX_THRESHOLD.toString());

        uncheckedOptions = SizeTieredCompactionStrategyOptions.validateOptions(options, uncheckedOptions);

        return uncheckedOptions;
    }
}
