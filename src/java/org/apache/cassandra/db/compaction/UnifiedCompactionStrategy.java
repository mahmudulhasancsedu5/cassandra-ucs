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
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.BiPredicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.agrona.collections.IntArrayList;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.SortedLocalRanges;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.IntervalSet;
import org.apache.cassandra.db.compaction.unified.Controller;
import org.apache.cassandra.db.compaction.unified.ShardedMultiWriter;
import org.apache.cassandra.db.compaction.unified.UnifiedCompactionTask;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Splitter;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.utils.Throwables.perform;

/**
 * The unified compaction strategy is described in this design document:
 *
 * See CEP-26: https://cwiki.apache.org/confluence/display/CASSANDRA/CEP-26%3A+Unified+Compaction+Strategy
 */
public class UnifiedCompactionStrategy extends AbstractCompactionStrategy
{
    public static final Class<? extends CompactionStrategyContainer> CONTAINER_CLASS = UnifiedCompactionContainer.class;

    private static final Logger logger = LoggerFactory.getLogger(UnifiedCompactionStrategy.class);

    static final int MAX_LEVELS = 32;   // This is enough for a few petabytes of data (with the worst case fan factor
                                        // at W=0 this leaves room for 2^32 sstables, presumably of at least 1MB each).

    private final Controller controller;

    private volatile ArenaSelector arenaSelector;
    private volatile ShardManager shardManager;

    private long lastExpiredCheck;

    public UnifiedCompactionStrategy(CompactionStrategyFactory factory, BackgroundCompactions backgroundCompactions, Map<String, String> options)
    {
        this(factory, backgroundCompactions, options, Controller.fromOptions(factory.getRealm(), options));
    }

    public UnifiedCompactionStrategy(CompactionStrategyFactory factory, BackgroundCompactions backgroundCompactions, Controller controller)
    {
        this(factory, backgroundCompactions, new HashMap<>(), controller);
    }

    public UnifiedCompactionStrategy(CompactionStrategyFactory factory, BackgroundCompactions backgroundCompactions, Map<String, String> options, Controller controller)
    {
        super(factory, backgroundCompactions, options);
        this.controller = controller;
    }

    @VisibleForTesting
    public UnifiedCompactionStrategy(CompactionStrategyFactory factory, Controller controller)
    {
        this(factory, new BackgroundCompactions(factory.getRealm()), new HashMap<>(), controller);
    }

    public static Map<String, String> validateOptions(Map<String, String> options) throws ConfigurationException
    {
        return Controller.validateOptions(CompactionStrategyOptions.validateOptions(options));
    }

    @Override
    public Collection<Collection<CompactionSSTable>> groupSSTablesForAntiCompaction(Collection<? extends CompactionSSTable> sstablesToGroup)
    {
        Collection<Collection<CompactionSSTable>> groups = new ArrayList<>();
        for (Arena shard : getCompactionArenas(sstablesToGroup, false))
        {
            groups.addAll(super.groupSSTablesForAntiCompaction(shard.sstables));
        }

        return groups;
    }

    @Override
    public synchronized CompactionTasks getUserDefinedTasks(Collection<? extends CompactionSSTable> sstables, int gcBefore)
    {
        // The tasks need to be split by repair status and disk, but we permit cross-shard user compactions.
        // See also getMaximalTasks below.
        List<AbstractCompactionTask> tasks = new ArrayList<>();
        for (Arena shard : getCompactionArenas(sstables, arenaSelector, true))
            tasks.addAll(super.getUserDefinedTasks(shard.sstables, gcBefore));
        return CompactionTasks.create(tasks);
    }

    @Override
    public synchronized CompactionTasks getMaximalTasks(int gcBefore, boolean splitOutput)
    {
        maybeUpdateSelector();
        // The tasks need to be split by repair status and disk, but we perform the maximal compaction across all shards.
        // The result will be split across shards, but the operation cannot be parallelized and does require 100% extra
        // space to complete.
        // The reason for this is to ensure that shard-spanning sstables get compacted with everything they overlap with.
        // For example, if an sstable on L0 covers shards 0 to 3, a sharded maximal compaction will compact that with
        // higher-level sstables in shard 0 and will leave a low-level sstable containing the non-shard-0 data from that
        // L0 sstable. This is a non-expected and non-desirable outcome.
        List<AbstractCompactionTask> tasks = new ArrayList<>();
        for (Arena shard : getCompactionArenas(realm.getLiveSSTables(), arenaSelector, true))
        {
            LifecycleTransaction txn = realm.tryModify(shard.sstables, OperationType.COMPACTION);
            if (txn != null)
                tasks.add(createCompactionTask(gcBefore, txn, true, splitOutput));
        }
        return CompactionTasks.create(tasks);
    }

    @Override
    public void startup()
    {
        perform(super::startup,
                () -> controller.startup(this, ScheduledExecutors.scheduledTasks));
    }

    @Override
    public void shutdown()
    {
        perform(super::shutdown,
                controller::shutdown);
    }

    /**
     * Returns a collections of compaction tasks.
     *
     * This method is synchornized because task creation is significantly more expensive in UCS; the strategy is
     * stateless, therefore it has to compute the shard/bucket structure on each call.
     *
     * @param gcBefore throw away tombstones older than this
     * @return collection of AbstractCompactionTask, which could be either a CompactionTask or an UnifiedCompactionTask
     */
    @Override
    public synchronized Collection<AbstractCompactionTask> getNextBackgroundTasks(int gcBefore)
    {
        // TODO - we should perhaps consider executing this code less frequently than legacy strategies
        // since it's more expensive, and we should therefore prevent a second concurrent thread from executing at all

        return getNextBackgroundTasks(getNextCompactionAggregates(gcBefore), gcBefore);
    }

    /**
     * Used by CNDB where compaction aggregates come from etcd rather than the strategy
     * @return collection of AbstractCompactionTask, which could be either a CompactionTask or an UnifiedCompactionTask
     */
    public synchronized Collection<AbstractCompactionTask> getNextBackgroundTasks(Collection<CompactionAggregate> aggregates, int gcBefore)
    {
        controller.onStrategyBackgroundTaskRequest();
        return createCompactionTasks(aggregates, gcBefore);
    }

    private synchronized Collection<AbstractCompactionTask> createCompactionTasks(Collection<CompactionAggregate> aggregates, int gcBefore)
    {
        Collection<AbstractCompactionTask> tasks = new ArrayList<>(aggregates.size());
        for (CompactionAggregate aggregate : aggregates)
        {
            CompactionPick selected = aggregate.getSelected();
            Preconditions.checkNotNull(selected);
            Preconditions.checkArgument(!selected.isEmpty());

            LifecycleTransaction transaction = realm.tryModify(selected.sstables(),
                                                               OperationType.COMPACTION,
                                                               selected.id());
            if (transaction != null)
            {
                backgroundCompactions.setSubmitted(this, transaction.opId(), aggregate);
                tasks.add(createCompactionTask(transaction, gcBefore));
            }
            else
            {
                // This can happen e.g. due to a race with upgrade tasks
                logger.error("Failed to submit compaction {} because a transaction could not be created. If this happens frequently, it should be reported", aggregate);
            }
        }

        return tasks;
    }

    /**
     * Create the sstable writer used for flushing.
     *
     * @return either a normal sstable writer, if there are no shards, or a sharded sstable writer that will
     *         create multiple sstables if a shard has a sufficiently large sstable.
     */
    @Override
    public SSTableMultiWriter createSSTableMultiWriter(Descriptor descriptor,
                                                       long keyCount,
                                                       long repairedAt,
                                                       UUID pendingRepair,
                                                       boolean isTransient,
                                                       IntervalSet<CommitLogPosition> commitLogPositions,
                                                       int sstableLevel,
                                                       SerializationHeader header,
                                                       Collection<Index.Group> indexGroups,
                                                       LifecycleNewTracker lifecycleNewTracker)
    {
        return new ShardedMultiWriter(realm,
                                      descriptor,
                                      keyCount,
                                      repairedAt,
                                      pendingRepair,
                                      isTransient,
                                      commitLogPositions,
                                      header,
                                      indexGroups,
                                      lifecycleNewTracker,
                                      controller.getMinSstableSizeBytes(),
                                      getShardManager());
    }

    /**
     * Create the task that in turns creates the sstable writer used for compaction.
     *
     * @return either a normal compaction task, if there are no shards, or a sharded compaction task that in turn will
     * create a sharded compaction writer.
     */
    private CompactionTask createCompactionTask(LifecycleTransaction transaction, int gcBefore)
    {
        if (controller.getNumShards() <= 1)
            return new CompactionTask(realm, transaction, gcBefore, false, this);

        return new UnifiedCompactionTask(realm, this, transaction, gcBefore, controller.getMinSstableSizeBytes(), getShardManager());
    }

    private void maybeUpdateSelector()
    {
        if (arenaSelector != null && !arenaSelector.diskBoundaries.isOutOfDate())
            return; // the disk boundaries (and thus the local ranges too) have not changed since the last time we calculated

        synchronized (this)
        {
            if (arenaSelector != null && !arenaSelector.diskBoundaries.isOutOfDate())
                return; // another thread beat us to the update

            DiskBoundaries currentBoundaries = realm.getDiskBoundaries();
            SortedLocalRanges localRanges = currentBoundaries.getLocalRanges();
            List<Token> diskBoundaries = currentBoundaries.getPositions();
            shardManager = new ShardManager(computeShardBoundaries(localRanges,
                                                                   diskBoundaries,
                                                                   controller.getNumShards(),
                                                                   realm.getPartitioner()),
                                            localRanges);
            arenaSelector = new ArenaSelector(controller, currentBoundaries);
            // Note: this can just as well be done without the synchronization (races would be benign, just doing some
            // redundant work). For the current usages of this blocking is fine and expected to perform no worse.
        }
    }

    /**
     * We want to split the local token range in shards, aiming for close to equal share for each shard.
     * If there are no disk boundaries, we just split the token space equally, but if multiple disks have been defined
     * (each with its own share of the local range), we can't have shards spanning disk boundaries. This means that
     * shards need to be selected within the disk's portion of the local ranges.
     *
     * As an example of what this means, consider a 3-disk node and 10 shards. The range is split equally between
     * disks, but we can only split shards within a disk range, thus we end up with 6 shards taking 1/3*1/3=1/9 of the
     * token range, and 4 smaller shards taking 1/3*1/4=1/12 of the token range.
     */
    @VisibleForTesting
    static List<Token> computeShardBoundaries(SortedLocalRanges localRanges,
                                              List<Token> diskBoundaries,
                                              int numShards,
                                              IPartitioner partitioner)
    {
        Optional<Splitter> splitter = partitioner.splitter();
        if (diskBoundaries != null && !splitter.isPresent())
            return diskBoundaries;
        else if (!splitter.isPresent()) // C* 2i case, just return 1 boundary at min token
            return ImmutableList.of(partitioner.getMinimumToken());

        // this should only happen in tests that change partitioners, but we don't want UCS to throw
        // where other strategies work even if the situations are unrealistic.
        if (localRanges.getRanges().isEmpty() || !localRanges.getRanges()
                                                             .get(0)
                                                             .range()
                                                             .left
                                                             .getPartitioner()
                                                             .equals(partitioner))
            localRanges = new SortedLocalRanges(localRanges.getRealm(),
                                                localRanges.getRingVersion(),
                                                null);

        if (diskBoundaries == null || diskBoundaries.size() <= 1)
            return localRanges.split(numShards);

        if (numShards <= diskBoundaries.size())
            return diskBoundaries;

        return splitPerDiskRanges(localRanges,
                                  diskBoundaries,
                                  getRangesTotalSize(localRanges.getRanges()),
                                  numShards,
                                  splitter.get());
    }

    /**
     * Split the per-disk ranges and generate the required number of shard boundaries.
     * This works by accumulating the size after each disk's share, multiplying by shardNum/totalSize and rounding to
     * produce an integer number of total shards needed by the disk boundary, which in turns defines how many need to be
     * added for this disk.
     *
     * For example, for a total size of 1, 2 disks (each of 0.5 share) and 3 shards, this will:
     * -process disk 1:
     * -- calculate 1/2 as the accumulated size
     * -- map this to 3/2 and round to 2 shards
     * -- split the disk's ranges into two equally-sized shards
     * -process disk 2:
     * -- calculate 1 as the accumulated size
     * -- map it to 3 and round to 3 shards
     * -- assign the disk's ranges to one shard
     *
     * The resulting shards will not be of equal size and this works best if the disk shares are distributed evenly
     * (which the current code always ensures).
     */
    private static List<Token> splitPerDiskRanges(SortedLocalRanges localRanges,
                                                  List<Token> diskBoundaries,
                                                  double totalSize,
                                                  int numArenas,
                                                  Splitter splitter)
    {
        double perArena = totalSize / numArenas;
        List<Token> shardBoundaries = new ArrayList<>(numArenas);
        double processedSize = 0;
        Token left = diskBoundaries.get(0).getPartitioner().getMinimumToken();
        for (Token boundary : diskBoundaries)
        {
            Token right = boundary.getToken();
            List<Splitter.WeightedRange> disk = localRanges.subrange(new Range<>(left, right));

            processedSize += getRangesTotalSize(disk);
            int targetCount = (int) Math.round(processedSize / perArena);
            List<Token> splits = splitter.splitOwnedRanges(Math.max(targetCount - shardBoundaries.size(), 1), disk, Splitter.SplitType.ALWAYS_SPLIT).boundaries;
            shardBoundaries.addAll(splits);
            // The splitting always results in maxToken as the last boundary. Replace it with the disk's upper bound.
            shardBoundaries.set(shardBoundaries.size() - 1, boundary);

            left = right;
        }
        assert shardBoundaries.size() == numArenas;
        return shardBoundaries;
    }

    private static double getRangesTotalSize(List<Splitter.WeightedRange> ranges)
    {
        double totalSize = 0;
        for (Splitter.WeightedRange range : ranges)
            totalSize += range.left().size(range.right());
        return totalSize;
    }

    @VisibleForTesting
    ShardManager getShardManager()
    {
        maybeUpdateSelector();
        return shardManager;
    }

    private CompactionLimits getCurrentLimits(int maxConcurrentCompactions)
    {
        // Calculate the running compaction limits, i.e. the overall number of compactions permitted, which is either
        // the compaction thread count, or the compaction throughput divided by the compaction rate (to prevent slowing
        // down individual compaction progress).
        String rateLimitLog = "";

        // identify space limit
        long spaceOverheadLimit = controller.maxCompactionSpaceBytes();

        // identify throughput limit
        double throughputLimit = controller.maxThroughput();
        int maxCompactions;
        if (throughputLimit < Double.MAX_VALUE)
        {
            int maxCompactionsForThroughput;

            double compactionRate = backgroundCompactions.compactionRate.get();
            if (compactionRate > 0)
            {
                // Start as many as can saturate the limit, making sure to also account for compactions that have
                // already been started but don't have progress yet.

                // Note: the throughput limit is adjusted here because the limiter won't let compaction proceed at more
                // than the given rate, and small hiccups or rounding errors could cause this to go above the current
                // running count when we are already at capacity.
                // Allow up to 5% variability, or if we are permitted more than 20 concurrent compactions, one/maxcount
                // so that we don't issue less tasks than we should.
                double adjustment = Math.min(0.05, 1.0 / maxConcurrentCompactions);
                maxCompactionsForThroughput = (int) Math.ceil(throughputLimit * (1 - adjustment) / compactionRate);
            }
            else
            {
                // If we don't have running compactions we don't know the effective rate.
                // Allow only one compaction; this will be called again soon enough to recheck.
                maxCompactionsForThroughput = 1;
            }

            rateLimitLog = String.format(" rate-based limit %d (rate %s/%s)",
                                         maxCompactionsForThroughput,
                                         FBUtilities.prettyPrintMemoryPerSecond((long) compactionRate),
                                         FBUtilities.prettyPrintMemoryPerSecond((long) throughputLimit));
            maxCompactions = Math.min(maxConcurrentCompactions, maxCompactionsForThroughput);
        }
        else
            maxCompactions = maxConcurrentCompactions;

        // Now that we have a count, make sure it is spread close to equally among levels. In other words, reserve
        // floor(permitted / levels) compactions for each level and don't permit more than ceil(permitted / levels) on
        // any, to make sure that no level hogs all threads and thus lowest-level ops (which need to run more often but
        // complete quickest) have a chance to run frequently. Also, running compactions can't go above the specified
        // space overhead limit.
        // To do this we count the number and size of already running compactions on each level and make sure any new
        // ones we select satisfy these constraints.
        int[] perLevel = new int[MAX_LEVELS];
        int levelCount = 1; // Start at 1 to avoid division by zero if the aggregates list is empty.
        int runningCompactions = 0;
        long spaceAvailable = spaceOverheadLimit;
        int runningAdaptiveCompactions = 0;
        int maxAdaptiveCompactions = controller.getMaxAdaptiveCompactions(); //limit for number of compactions triggered by new W value
        for (CompactionPick compaction : backgroundCompactions.getCompactionsInProgress())
        {
            final int level = levelOf(compaction);
            ++perLevel[level];
            ++runningCompactions;
            levelCount = Math.max(levelCount, level + 1);
            spaceAvailable -= controller.getOverheadSizeInBytes(compaction);
            if (compaction.isAdaptive(controller.getThreshold(level), controller.getPreviousThreshold(level)))
                runningAdaptiveCompactions++;
        }

        CompactionLimits limits = new CompactionLimits(runningCompactions, 
                                                       maxCompactions,
                                                       maxConcurrentCompactions,
                                                       perLevel,
                                                       levelCount,
                                                       spaceAvailable,
                                                       rateLimitLog,
                                                       runningAdaptiveCompactions,
                                                       maxAdaptiveCompactions);
        logger.debug("Selecting up to {} new compactions of up to {}, concurrency limit {}{}",
                     Math.max(0, limits.maxCompactions - limits.runningCompactions),
                     FBUtilities.prettyPrintMemory(limits.spaceAvailable),
                     limits.maxConcurrentCompactions,
                     limits.rateLimitLog);
        return limits;
    }

    private Collection<CompactionAggregate> updateLevelCountWithParentAndGetSelection(final CompactionLimits limits,
                                                                                      List<CompactionAggregate.UnifiedAggregate> pending)
    {
        for (CompactionAggregate.UnifiedAggregate aggregate : pending)
        {
            warnIfSizeAbove(aggregate, limits.spaceAvailable);

            CompactionPick selected = aggregate.getSelected();
            if (selected != null)
                limits.levelCount = Math.max(limits.levelCount, (int) selected.parent());
        }

        final List<CompactionAggregate> selection = getSelection(pending,
                                                                 limits.maxCompactions,
                                                                 limits.levelCount,
                                                                 limits.perLevel,
                                                                 limits.spaceAvailable,
                                                                 limits.runningAdaptiveCompactions,
                                                                 limits.maxAdaptiveCompactions);
        logger.debug("Starting {} compactions (out of {})", selection.size(), pending.stream().filter(agg -> !agg.getSelected().isEmpty()).count());
        return selection;
    }

    /**
     * Selects compactions to run next.
     * 
     * @param gcBefore
     * @return a subset of compaction aggregates to run next
     */
    private Collection<CompactionAggregate> getNextCompactionAggregates(int gcBefore)
    {
        final CompactionLimits limits = getCurrentLimits(controller.maxConcurrentCompactions());

        List<CompactionAggregate.UnifiedAggregate> pending = getPendingCompactionAggregates(limits.spaceAvailable, gcBefore);
        setPendingCompactionAggregates(pending);

        for (CompactionAggregate.UnifiedAggregate aggregate : pending)
        {
            // Make sure the level count includes all levels for which we have sstables (to be ready to compact
            // as soon as the threshold is crossed)...
            limits.levelCount = Math.max(limits.levelCount, aggregate.bucketIndex() + 1);
            CompactionPick selected = aggregate.getSelected();
            if (selected != null)
            {
                // ... and also the levels that a layout-preserving selection would create.
                limits.levelCount = Math.max(limits.levelCount, levelOf(selected) + 1);
            }
        }

        return updateLevelCountWithParentAndGetSelection(limits, pending);
    }
    
    /**
     * Selects compactions to run next from the passed aggregates.
     *
     * The intention here is to use this method directly from outside processes, to run compactions from a set
     * of pre-existing aggregates, that have been generated out of process.
     *
     * @param aggregates a collection of aggregates from which to select the next compactions
     * @param maxConcurrentCompactions the maximum number of concurrent compactions
     * @return a subset of compaction aggregates to run next
     */
    public Collection<CompactionAggregate> getNextCompactionAggregates(Collection<CompactionAggregate.UnifiedAggregate> aggregates, 
                                                                       int maxConcurrentCompactions)
    {
        final CompactionLimits limits = getCurrentLimits(maxConcurrentCompactions);
        maybeUpdateSelector();
        return updateLevelCountWithParentAndGetSelection(limits, new ArrayList<>(aggregates));
    }

    /**
     * Returns all pending compaction aggregates.
     *
     * This method is used by CNDB to find all pending compactions and put them to etcd.
     *
     * @param gcBefore
     * @return all pending compaction aggregates
     **/
    public Collection<CompactionAggregate.UnifiedAggregate> getPendingCompactionAggregates(int gcBefore)
    {
        return getPendingCompactionAggregates(controller.maxCompactionSpaceBytes(), gcBefore);
    }

    /**
     * Set the compaction aggregates passed in as pending in {@link BackgroundCompactions}. This ensures
     * that the compaction statistics will be accurate.
     * <p/>
     * This is called by {@link UnifiedCompactionStrategy#getNextCompactionAggregates(int)}
     * and externally after calling {@link UnifiedCompactionStrategy#getPendingCompactionAggregates(int)} 
     * or before submitting tasks.
     *
     * Also, note that skipping the call to {@link BackgroundCompactions#setPending(CompactionStrategy, Collection)}
     * would result in memory leaks: the aggregates added in {@link BackgroundCompactions#setSubmitted(CompactionStrategy, UUID, CompactionAggregate)}
     * would never be removed, and the aggregates hold references to the compaction tasks, so they retain a significant
     * size of heap memory.
     *
     * @param pending the aggregates that should be set as pending compactions
     */
    public void setPendingCompactionAggregates(Collection<? extends CompactionAggregate> pending)
    {
        backgroundCompactions.setPending(this, pending);
    }

    private List<CompactionAggregate.UnifiedAggregate> getPendingCompactionAggregates(long spaceAvailable, int gcBefore)
    {
        maybeUpdateSelector();

        List<CompactionAggregate.UnifiedAggregate> pending = new ArrayList<>();
        long ts = System.currentTimeMillis();
        boolean expiredCheck = ts - lastExpiredCheck > controller.getExpiredSSTableCheckFrequency();
        if (expiredCheck)
            lastExpiredCheck = ts;

        for (Map.Entry<Arena, List<Level>> entry : getLevels().entrySet())
        {
            Arena arena = entry.getKey();
            Set<CompactionSSTable> expired;
            if (expiredCheck)
            {
                expired = arena.getExpiredSSTables(gcBefore, controller.getIgnoreOverlapsInExpirationCheck());
                if (logger.isTraceEnabled() && expired.size() > 0)
                    logger.trace("Expiration check for arena {} found {} fully expired SSTables", arena.name(), expired.size());
            }
            else
                expired = Collections.emptySet();

            for (Level level : entry.getValue())
            {
                CompactionAggregate.UnifiedAggregate aggregate = level.getCompactionAggregate(arena, expired, controller, spaceAvailable);
                // Note: We allow empty aggregates into the list of pending compactions. The pending compactions list
                // is for progress tracking only, and it is helpful to see empty levels there.
                pending.add(aggregate);
            }
        }

        return pending;
    }

    /**
     * This method logs a warning related to the fact that the space overhead limit also applies when a
     * single compaction is above that limit. This should prevent running out of space at the expense of ending up
     * with several extra sstables at the highest-level (compared to the number of sstables that we should have
     * as per config of the strategy), i.e. slightly higher read amplification. This is a sensible tradeoff but
     * the operators must be warned if this happens, and that's the purpose of this warning.
     */
    private void warnIfSizeAbove(CompactionAggregate.UnifiedAggregate aggregate, long spaceOverheadLimit)
    {
        if (controller.getOverheadSizeInBytes(aggregate.selected) > spaceOverheadLimit)
            logger.warn("Compaction needs to perform an operation that is bigger than the current space overhead " +
                        "limit - size {} (compacting {} sstables in shard {}/bucket {}); limit {} = {}% of dataset size {}. " +
                        "To honor the limit, this operation will not be performed, which may result in degraded performance.\n" +
                        "Please verify the compaction parameters, specifically {} and {}.",
                        FBUtilities.prettyPrintMemory(controller.getOverheadSizeInBytes(aggregate.selected)),
                        aggregate.selected.sstables().size(),
                        aggregate.getArena().name(),
                        aggregate.bucketIndex(),
                        FBUtilities.prettyPrintMemory(spaceOverheadLimit),
                        controller.getMaxSpaceOverhead() * 100,
                        FBUtilities.prettyPrintMemory(controller.getDataSetSizeBytes()),
                        Controller.DATASET_SIZE_OPTION_GB,
                        Controller.MAX_SPACE_OVERHEAD_OPTION);
    }

    /**
     * Returns a random selection of the compactions to be submitted. The selection will be chosen so that the total
     * number of compactions is at most totalCount, where each level gets a share that is the whole part of the ratio
     * between the the total permitted number of compactions, and the remainder gets distributed randomly among the
     * levels. Note that if a level does not have tasks to fill its share, its quota will remain unused in this
     * allocation.
     *
     * The selection also limits the size of the newly scheduled compactions to be below spaceAvailable by not
     * scheduling compactions if they would push the combined size above that limit.
     *
     * @param pending list of all current aggregates with possible selection for each bucket
     * @param totalCount maximum number of compactions permitted to run
     * @param levelCount number of levels in use
     * @param perLevel int array with the number of in-progress compactions per level
     * @param spaceAvailable amount of space in bytes available for the new compactions
     */
    List<CompactionAggregate> getSelection(List<CompactionAggregate.UnifiedAggregate> pending,
                                           int totalCount,
                                           int levelCount,
                                           int[] perLevel,
                                           long spaceAvailable,
                                           int runningAdaptiveCompactions,
                                           int maxAdaptiveCompactions)
    {
        pending = controller.maybeSort(pending);

        int perLevelCount = totalCount / levelCount;   // each level has this number of tasks reserved for it
        int remainder = totalCount % levelCount;       // and the remainder is distributed randomly, up to 1 per level

        // List the indexes of all compaction picks, adding several entries for compactions that span multiple shards.
        IntArrayList list = new IntArrayList(pending.size(), -1);
        IntArrayList expired = new IntArrayList(pending.size(), -1);
        for (int aggregateIndex = 0; aggregateIndex < pending.size(); ++aggregateIndex)
        {
            CompactionAggregate.UnifiedAggregate aggregate = pending.get(aggregateIndex);
            final CompactionPick pick = aggregate.getSelected();
            if (pick.isEmpty())
                continue;
            if (pick.hasExpiredOnly())
            {
                expired.add(aggregateIndex);
                continue;
            }
            if (controller.getOverheadSizeInBytes(pick) > spaceAvailable)
                continue;
            if (perLevel[levelOf(pick)] > perLevelCount)
                continue;  // this level is already using up all its share + one, we can ignore candidate altogether
            int currentLevel = levelOf(pick);
            if (maxAdaptiveCompactions == -1)
                maxAdaptiveCompactions = Integer.MAX_VALUE;
            if (pick.isAdaptive(controller.getThreshold(currentLevel), controller.getPreviousThreshold(currentLevel)))
            {
                if (runningAdaptiveCompactions >= maxAdaptiveCompactions)
                    continue; //do not allow more than maxAdaptiveCompactions to limit latency spikes upon changing W
                runningAdaptiveCompactions++;
            }


            int shardsSpanned = shardsSpanned(pick);
            for (int i = 0; i < shardsSpanned; ++i)  // put an entry for each spanned shard
                list.addInt(aggregateIndex);
        }
        if (list.isEmpty() && expired.isEmpty())
            return ImmutableList.of();

        BitSet selection = new BitSet(pending.size());

        // Always include expire-only aggregates
        for (int i = 0; i < expired.size(); i++)
            selection.set(expired.get(i));

        int selectedSize = 0;
        if (!list.isEmpty())
        {
            // Randomize the list.
            list = controller.maybeRandomize(list);

            // Calculate how many new ones we can add in each level, and how many we can assign randomly.
            int remaining = totalCount;
            for (int i = 0; i < levelCount; ++i)
            {
                remaining -= perLevel[i];
                if (perLevel[i] > perLevelCount)
                    remainder -= perLevel[i] - perLevelCount;
            }
            int toAdd = remaining;
            // Note: if we are in the middle of changes in the parameters or level count, remainder might become negative.
            // This is okay, some buckets will temporarily not get their rightful share until these tasks complete.

            // Select the first ones, skipping over duplicates and permitting only the specified number per level.
            for (int i = 0; remaining > 0 && i < list.size(); ++i)
            {
                final int aggregateIndex = list.getInt(i);
                if (selection.get(aggregateIndex))
                    continue; // this is a repeat
                CompactionAggregate.UnifiedAggregate aggregate = pending.get(aggregateIndex);
                if (controller.getOverheadSizeInBytes(aggregate.selected) > spaceAvailable)
                    continue; // compaction is too large for current cycle
                int level = levelOf(aggregate.getSelected());

                if (perLevel[level] > perLevelCount)
                    continue;   // share + one already used
                else if (perLevel[level] == perLevelCount)
                {
                    if (remainder <= 0)
                        continue;   // share used up, no remainder to distribute
                    --remainder;
                }

                --remaining;
                ++perLevel[level];
                spaceAvailable -= controller.getOverheadSizeInBytes(aggregate.selected);
                selection.set(aggregateIndex);
            }

            selectedSize = toAdd - remaining;
        }

        // Return in the order of the pending aggregates to satisfy tests.
        List<CompactionAggregate> aggregates = new ArrayList<>(selectedSize + expired.size());
        for (int i = selection.nextSetBit(0); i >= 0; i = selection.nextSetBit(i+1))
            aggregates.add(pending.get(i));

        return aggregates;
    }

    private int shardsSpanned(CompactionPick pick)
    {
        PartitionPosition min = pick.sstables().stream().map(CompactionSSTable::getFirst).min(Ordering.natural()).get();
        PartitionPosition max = pick.sstables().stream().map(CompactionSSTable::getLast).max(Ordering.natural()).get();
        return shardManager.shardFor(max) - shardManager.shardFor(min) + 1;
    }

    @Override
    public int getEstimatedRemainingTasks()
    {
        return backgroundCompactions.getEstimatedRemainingTasks();
    }

    @Override
    public long getMaxSSTableBytes()
    {
        return Long.MAX_VALUE;
    }

    @Override
    public Set<? extends CompactionSSTable> getSSTables()
    {
        return realm.getLiveSSTables();
    }

    @VisibleForTesting
    public int getW(int index)
    {
        return controller.getScalingParameter(index);
    }

    @VisibleForTesting
    public Controller getController()
    {
        return controller;
    }

    /**
     * Group candidate sstables into compaction shards.
     * Each compaction shard is obtained by comparing using a compound comparator for the equivalence classes
     * configured in the arena selector of this strategy.
     *
     * @param sstables a collection of the sstables to be assigned to shards
     * @param compactionFilter a filter to exclude CompactionSSTables,
     *                         e.g., {@link CompactionSSTable#isSuitableForCompaction()}
     * @return a list of shards, where each shard contains sstables that belong to that shard
     */
    public Collection<Arena> getCompactionArenas(Collection<? extends CompactionSSTable> sstables,
                                                 Predicate<CompactionSSTable> compactionFilter)
    {
        return getCompactionArenas(sstables, compactionFilter, this.arenaSelector,true);
    }

    Collection<Arena> getCompactionArenas(Collection<? extends CompactionSSTable> sstables, boolean filterUnsuitable)
    {
        return getCompactionArenas(sstables,
                                   CompactionSSTable::isSuitableForCompaction,
                                   this.arenaSelector,
                                   filterUnsuitable);
    }

    Collection<Arena> getCompactionArenas(Collection<? extends CompactionSSTable> sstables,
                                          ArenaSelector arenaSelector,
                                          boolean filterUnsuitable)
    {
        return getCompactionArenas(sstables,
                                   CompactionSSTable::isSuitableForCompaction,
                                   arenaSelector,
                                   filterUnsuitable);
    }

    Collection<Arena> getCompactionArenas(Collection<? extends CompactionSSTable> sstables,
                                          Predicate<CompactionSSTable> compactinoFilter,
                                          ArenaSelector arenaSelector,
                                          boolean filterUnsuitable)
    {
        Map<CompactionSSTable, Arena> shardsBySSTables = new TreeMap<>(arenaSelector);
        Set<? extends CompactionSSTable> compacting = realm.getCompactingSSTables();
        for (CompactionSSTable sstable : sstables)
            if (!filterUnsuitable || compactinoFilter.test(sstable) && !compacting.contains(sstable))
                shardsBySSTables.computeIfAbsent(sstable, t -> new Arena(arenaSelector, realm))
                      .add(sstable);

        return shardsBySSTables.values();
    }

    // used by CNDB to deserialize aggregates
    public Arena getCompactionArena(Collection<? extends CompactionSSTable> sstables)
    {
        maybeUpdateSelector();
        Arena shard = new Arena(arenaSelector, realm);
        for (CompactionSSTable table : sstables)
            shard.add(table);
        return shard;
    }

    // used by CNDB to deserialize aggregates
    public Level getBucket(int index, long min)
    {
        return new Level(controller, index, min);
    }

    /**
     * @return a LinkedHashMap of shards with buckets where order of shards are preserved
     */
    @VisibleForTesting
    Map<Arena, List<Level>> getLevels()
    {
        return getLevels(realm.getLiveSSTables(), CompactionSSTable::isSuitableForCompaction);
    }

    /**
     * Groups the sstables passed in into shards and buckets. This is used by the strategy to determine
     * new compactions, and by external tools in CNDB to analyze the strategy decisions.
     *
     * @param sstables a collection of the sstables to be assigned to shards
     * @param compactionFilter a filter to exclude CompactionSSTables,
     *                         e.g., {@link CompactionSSTable#isSuitableForCompaction()}
     *
     * @return a map of shards to their buckets
     */
    public Map<Arena, List<Level>> getLevels(Collection<? extends CompactionSSTable> sstables,
                                             Predicate<CompactionSSTable> compactionFilter)
    {
        maybeUpdateSelector();
        Collection<Arena> shards = getCompactionArenas(sstables, compactionFilter);
        Map<Arena, List<Level>> ret = new LinkedHashMap<>(); // should preserve the order of shards

        for (Arena shard : shards)
        {
            List<Level> levels = new ArrayList<>(MAX_LEVELS);
            shard.sstables.sort(shardManager::compareByDensity);

            int index = 0;
            Level level = new Level(controller, index, 0);
            for (CompactionSSTable candidate : shard.sstables)
            {
                final double size = shardManager.density(candidate);
                if (size < level.max)
                {
                    level.add(candidate);
                    continue;
                }

                level.complete();
                levels.add(level); // add even if empty

                while (true)
                {
                    level = new Level(controller, ++index, level.max);
                    if (size < level.max)
                    {
                        level.add(candidate);
                        break;
                    }
                    else
                    {
                        levels.add(level); // add the empty level
                    }
                }
            }

            if (!level.sstables.isEmpty())
            {
                level.complete();
                levels.add(level);
            }

            if (!levels.isEmpty())
                ret.put(shard, levels);

            if (logger.isTraceEnabled())
                logger.trace("Arena {} has {} levels", shard, levels.size());
        }

        logger.debug("Found {} shards with buckets for {}.{}", ret.size(), realm.getKeyspaceName(), realm.getTableName());
        return ret;
    }

    private static int levelOf(CompactionPick pick)
    {
        return (int) pick.parent();
    }

    public TableMetadata getMetadata()
    {
        return realm.metadata();
    }

    private static boolean startsAfter(CompactionSSTable a, CompactionSSTable b)
    {
        return a.getFirst().compareTo(b.getLast()) > 0;
    }

    /**
     * Construct a minimal list of overlap sets, i.e. the sections of the range span when we have overlapping items,
     * where we ensure:
     * - non-overlapping items are never put in the same set
     * - no item is present in non-consecutive sets
     * - for any point where items overlap, the result includes a set listing all overlapping items
     *
     * For example, for inputs A[0, 4), B[2, 8), C[6, 10), D[1, 9) the result would be the sets ABD and BCD. We are not
     * interested in the spans where A, B, or C are present on their own or in combination with D, only that there
     * exists a set in the list that is a superset of any such combination, and that the non-overlapping A and C are
     * never together in a set.
     *
     * Note that the full list of overlap sets A, AD, ABD, BD, BCD, CD, C is also an answer that satisfies the three
     * conditions above, but it contains redundant sets (e.g. AD is already contained in ABD).
     *
     * @param items A list of items to distribute in overlap sets. This is assumed to be a transient list and the method
     *              may modify or consume it.
     * @param startsAfter Predicate determining if its left argument's start if fully after the right argument's end.
     *                    This will only be used with arguments where left's start is known to be after right's start.
     * @param startsComparator Comparator of items' starting positions.
     * @param endsComparator Comparator of items' ending positions.
     * @return List of overlap sets.
     */
    static <E> List<Set<E>> constructOverlapSets(List<E> items,
                                                 BiPredicate<E, E> startsAfter,
                                                 Comparator<E> startsComparator,
                                                 Comparator<E> endsComparator)
    {
        List<Set<E>> overlaps = new ArrayList<>();
        if (items.isEmpty())
            return overlaps;

        PriorityQueue<E> active = new PriorityQueue<>(endsComparator);
        items.sort(startsComparator);
        for (E item : items)
        {
            if (!active.isEmpty() && startsAfter.test(item, active.peek()))
            {
                // New item starts after some active ends. It does not overlap with it, so:
                // -- output the previous active set
                overlaps.add(new HashSet<>(active));
                // -- remove all items that also end before the current start
                do
                {
                    active.poll();
                }
                while (!active.isEmpty() && startsAfter.test(item, active.peek()));
            }

            // Add the new item to the active state. We don't care if it starts later than others in the active set,
            // the important point is that it overlaps with all of them.
            active.add(item);
        }

        assert !active.isEmpty();
        overlaps.add(new HashSet<>(active));

        return overlaps;
    }

    /**
     * A compaction arena contains the list of sstables that belong to this arena as well as the arena
     * selector used for comparison.
     */
    public static class Arena implements Comparable<Arena>
    {
        final List<CompactionSSTable> sstables;
        final ArenaSelector selector;
        private final CompactionRealm realm;

        Arena(ArenaSelector selector, CompactionRealm realm)
        {
            this.realm = realm;
            this.sstables = new ArrayList<>();
            this.selector = selector;
        }

        void add(CompactionSSTable ssTableReader)
        {
            sstables.add(ssTableReader);
        }

        public String name()
        {
            CompactionSSTable t = sstables.get(0);
            return selector.name(t);
        }

        @Override
        public int compareTo(Arena o)
        {
            return selector.compare(this.sstables.get(0), o.sstables.get(0));
        }

        @Override
        public String toString()
        {
            return String.format("%s, %d sstables", name(), sstables.size());
        }

        @VisibleForTesting
        public List<CompactionSSTable> getSSTables()
        {
            return sstables;
        }

        /**
         * Find fully expired SSTables. Those will be included in the aggregate no matter what.
         * @param gcBefore
         * @param ignoreOverlaps
         * @return expired SSTables
         */
        Set<CompactionSSTable> getExpiredSSTables(int gcBefore, boolean ignoreOverlaps)
        {
            return CompactionController.getFullyExpiredSSTables(realm,
                                                                sstables,
                                                                realm.getOverlappingLiveSSTables(sstables),
                                                                gcBefore,
                                                                ignoreOverlaps);
        }
    }

    @Override
    public String toString()
    {
        return String.format("Unified strategy %s", getMetadata());
    }

    /**
     * A level: index, sstables and some properties.
     */
    public static class Level
    {
        final List<CompactionSSTable> sstables;
        final int index;
        final double survivalFactor;
        final int scalingParameter; // scaling parameter used to calculate fanout and threshold
        final int fanout; // fanout factor between levels
        final int threshold; // number of SSTables that trigger a compaction
        final double min; // min density of sstables for this level
        final double max; // max density of sstables for this level
        double avg = 0; // avg size of sstables in this level
        int maxOverlap = -1; // maximum number of overlapping sstables

        Level(Controller controller, int index, double minSize)
        {
            this.index = index;
            this.survivalFactor = controller.getSurvivalFactor(index);
            this.scalingParameter = controller.getScalingParameter(index);
            this.fanout = controller.getFanout(index);
            this.threshold = controller.getThreshold(index);
            this.sstables = new ArrayList<>(threshold);
            this.min = minSize;
            this.max = controller.getMaxLevelDensity(index, this.min);
        }

        public Collection<CompactionSSTable> getSSTables()
        {
            return sstables;
        }

        public int getIndex()
        {
            return index;
        }

        void add(CompactionSSTable sstable)
        {
            this.sstables.add(sstable);
            this.avg += (sstable.onDiskLength() - avg) / sstables.size();
        }

        void complete()
        {
            if (logger.isTraceEnabled())
                logger.trace("Level: {}", this);
        }

        /**
         * Return the compaction aggregate
         */
        CompactionAggregate.UnifiedAggregate getCompactionAggregate(Arena arena,
                                                                    Set<CompactionSSTable> allExpiredSSTables,
                                                                    Controller controller,
                                                                    long spaceAvailable)
        {
            List<CompactionSSTable> expiredSet = Collections.emptyList();
            List<CompactionSSTable> liveSet = sstables;
            if (!allExpiredSSTables.isEmpty())
            {
                liveSet = new ArrayList<>();
                expiredSet = new ArrayList<>();
                bipartitionSSTables(sstables, allExpiredSSTables, liveSet, expiredSet);
            }

            if (logger.isTraceEnabled())
                logger.trace("Creating compaction aggregate with live set {}, and expired set {}", liveSet, expiredSet);

            List<CompactionPick> pending = new ArrayList<>();
            CompactionPick selected = null;

            List<Set<CompactionSSTable>> overlaps = constructOverlapSets(liveSet,
                                                                         UnifiedCompactionStrategy::startsAfter,
                                                                         CompactionSSTable.firstKeyComparator,
                                                                         CompactionSSTable.lastKeyComparator);
            for (Set<CompactionSSTable> overlap : overlaps)
                maxOverlap = Math.max(maxOverlap, overlap.size());

            List<Bucket> buckets = assignOverlapsIntoBuckets(controller, overlaps);
            int picksCount = 0;

            for (Bucket bucket : buckets)
            {
                CompactionPick bucketSelection = bucket.constructPicks(pending, controller, spaceAvailable);
                if (bucketSelection != null)
                {
                    if (controller.random().nextInt(++picksCount) == 0)
                    {
                        if (selected != null)
                            pending.add(selected);
                        selected = bucketSelection;
                    }
                }
            }

            if (selected == null)
                selected = CompactionPick.EMPTY;

            boolean hasExpiredSSTables = !expiredSet.isEmpty();
            if (hasExpiredSSTables && selected.equals(CompactionPick.EMPTY))
                // overrides default CompactionPick.EMPTY with parent equal to -1
                selected = CompactionPick.create(index, expiredSet, expiredSet);
            else if (hasExpiredSSTables)
                selected = selected.withExpiredSSTables(expiredSet);

            if (logger.isTraceEnabled())
                logger.trace("Returning compaction aggregate with selected compaction {}, and pending {}, for arena {}",
                             selected, pending, arena);
            return CompactionAggregate.createUnified(sstables, maxOverlap, selected, pending, arena, this);
        }

        /**
         * Assign overlap sections into buckets. Identify sections that have at least threshold-many overlapping
         * SSTables and apply the overlap inclusion method to combine with any neighbouring sections that contain
         * selected sstables to make sure we make full use of any sstables selected for compaction (i.e. avoid
         * recompacting, see {@link Controller#overlapInclusionMethod()}).
         */
        private List<Bucket> assignOverlapsIntoBuckets(Controller controller, List<Set<CompactionSSTable>> overlaps)
        {
            List<Bucket> buckets = new ArrayList<>();
            int regionCount = overlaps.size();
            int lastEnd = -1;
            for (int i = 0; i < regionCount; ++i)
            {
                Set<CompactionSSTable> bucket = overlaps.get(i);
                int maxOverlap = bucket.size();
                if (maxOverlap < threshold)
                    continue;
                int startIndex = i;
                int endIndex = i + 1;

                if (controller.overlapInclusionMethod() != Controller.OverlapInclusionMethod.NONE)
                {
                    Set<CompactionSSTable> allOverlapping = new HashSet<>(bucket);
                    Set<CompactionSSTable> overlapTarget = controller.overlapInclusionMethod() == Controller.OverlapInclusionMethod.TRANSITIVE
                                                           ? allOverlapping
                                                           : bucket;
                    int j;
                    for (j = i - 1; j > lastEnd; --j)
                    {
                        Set<CompactionSSTable> next = overlaps.get(j);
                        if (Sets.intersection(next, overlapTarget).isEmpty())
                            break;
                        allOverlapping.addAll(next);
                    }
                    startIndex = j + 1;
                    for (j = i + 1; j < regionCount; ++j)
                    {
                        Set<CompactionSSTable> next = overlaps.get(j);
                        if (Sets.intersection(next, overlapTarget).isEmpty())
                            break;
                        allOverlapping.addAll(next);
                    }
                    i = j - 1;
                    endIndex = j;
                }
                buckets.add(endIndex == startIndex + 1
                            ? new SimpleBucket(this, overlaps.get(startIndex))
                            : new MultiSetBucket(this, overlaps.subList(startIndex, endIndex)));
                lastEnd = i;
            }
            return buckets;
        }

        /**
         * Bipartitions SSTables into liveSet and expiredSet, depending on whether they are present in allExpiredSSTables.
         *
         * @param sstables list of SSTables in a bucket
         * @param allExpiredSSTables set of expired SSTables for all shards/buckets
         * @param liveSet empty list that is going to be filled up with SSTables that are not present in {@param allExpiredSSTables}
         * @param expiredSet empty list that is going to be filled up with SSTables that are present in {@param allExpiredSSTables}
         */
        private static void bipartitionSSTables(List<CompactionSSTable> sstables,
                                                Set<CompactionSSTable> allExpiredSSTables,
                                                List<CompactionSSTable> liveSet,
                                                List<CompactionSSTable> expiredSet)
        {
            for (CompactionSSTable sstable : sstables)
            {
                if (allExpiredSSTables.contains(sstable))
                    expiredSet.add(sstable);
                else
                    liveSet.add(sstable);
            }
        }

        @Override
        public String toString()
        {
            return String.format("W: %d, T: %d, F: %d, index: %d, min: %s, max %s, %d sstables, overlap %s",
                                 scalingParameter,
                                 threshold,
                                 fanout,
                                 index,
                                 densityAsString(min),
                                 densityAsString(max),
                                 sstables.size(),
                                 maxOverlap);
        }

        private String densityAsString(double density)
        {
            return FBUtilities.prettyPrintBinary(density, "B", " ");
        }
    }


    /**
     * A compaction bucket, i.e. a selection of overlapping sstables from which a compaction should be selected.
     */
    static abstract class Bucket
    {
        final Level level;
        final List<CompactionSSTable> allSSTablesSorted;
        final int maxOverlap;

        Bucket(Level level, Collection<CompactionSSTable> allSSTablesSorted, int maxOverlap)
        {
            // single section
            this.level = level;
            this.allSSTablesSorted = new ArrayList<>(allSSTablesSorted);
            this.allSSTablesSorted.sort(CompactionSSTable.maxTimestampDescending);  // we remove entries from the back
            this.maxOverlap = maxOverlap;
        }

        Bucket(Level level, List<Set<CompactionSSTable>> overlapSections)
        {
            // multiple sections
            this.level = level;
            int maxOverlap = 0;
            Set<CompactionSSTable> all = new HashSet<>();
            for (Set<CompactionSSTable> section : overlapSections)
            {
                maxOverlap = Math.max(maxOverlap, section.size());
                all.addAll(section);
            }
            this.allSSTablesSorted = new ArrayList<>(all);
            this.allSSTablesSorted.sort(CompactionSSTable.maxTimestampDescending);  // we remove entries from the back
            this.maxOverlap = maxOverlap;
        }

        /**
         * Select compactions from this bucket. Normally this would form a compaction out of all sstables in the
         * bucket, but if compaction is very late we may prefer to act more carefully:
         * - we should not use more inputs than the permitted maximum
         * - we should not select a compaction whose execution will use more temporary space than is available
         * - we should select SSTables in a way that preserves the structure of the compaction hierarchy
         * Note that numbers above refer to overlapping sstables, i.e. if A and B don't overlap with each other but
         * both overlap with C and D, all four will be selected to form a three-input compaction. A two-input may choose
         * CD, ABC or ABD.
         * Also, the subset is selected by max timestamp order, oldest first, to avoid violating sstable time order. In
         * the example above, if B is oldest and C is older than D, the two-input choice would be ABC (if A is older
         * than D) or BC (if A is younger, avoiding combining C with A skipping D).
         *
         * @param pending A list where any "pending" compactions are placed, i.e. operations that will need to be
         *                executed in the future.
         * @param controller The compaction controller.
         * @param spaceAvailable The amount of space available for compaction, limits the maximum number of sstables
         *                       that can be selected.
         * @return A compaction pick to execute next.
         */
        CompactionPick constructPicks(List<CompactionPick> pending, Controller controller, long spaceAvailable)
        {
            int count = maxOverlap;
            int threshold = level.threshold;
            int fanout = level.fanout;
            int index = level.index;
            int maxSSTablesToCompact = Math.max(fanout, (int) Math.min(spaceAvailable / level.avg, controller.maxSSTablesToCompact()));

            assert count >= threshold;
            if (count <= fanout)
            {
                /**
                 * Happy path. We are not late or (for levelled) we are only so late that a compaction now will
                 * have the  same effect as doing levelled compactions one by one. Compact all. We do not cap
                 * this pick at maxSSTablesToCompact due to an assumption that maxSSTablesToCompact is much
                 * greater than F. See {@link Controller#MAX_SSTABLES_TO_COMPACT_OPTION} for more details.
                 */
                return CompactionPick.create(index, allSSTablesSorted);
            }
            // The choices below assume that pulling the oldest sstables will reduce maxOverlap by the selected
            // number of sstables. This is not always true (we may, e.g. select alternately from different overlap
            // sections if the structure is complex enough), but is good enough heuristic that results in usable
            // compaction sets.
            else if (count <= fanout * controller.getFanout(index + 1) || maxSSTablesToCompact == fanout)
            {
                // Compaction is a bit late, but not enough to jump levels via layout compactions. We need a special
                // case to cap compaction pick at maxSSTablesToCompact.
                if (count <= maxSSTablesToCompact)
                    return CompactionPick.create(index, allSSTablesSorted);

                CompactionPick pick = CompactionPick.create(index, pullOldestSSTables(maxSSTablesToCompact));
                count -= maxSSTablesToCompact;
                while (count >= threshold)
                {
                    pending.add(CompactionPick.create(index, pullOldestSSTables(maxSSTablesToCompact)));
                    count -= maxSSTablesToCompact;
                }

                return pick;
            }
            // We may, however, have accumulated a lot more than T if compaction is very late, or a set of small
            // tables was dumped on us (e.g. when converting from legacy LCS or for tests).
            else
            {
                // We need to pick the compactions in such a way that the result of doing them all spreads the data in
                // a similar way to how compaction would lay them if it was able to keep up. This means:
                // - for tiered compaction (W >= 0), compact in sets of as many as required to get to a level.
                //   for example, for W=2 and 55 sstables, do 3 compactions of 16 sstables, 1 of 4, and leave the other 3 alone
                // - for levelled compaction (W < 0), compact all that would reach a level.
                //   for W=-2 and 55, this means one compaction of 48, one of 4, and one of 3 sstables.
                List<CompactionPick> picks = layoutCompactions(controller, maxSSTablesToCompact);
                // Out of the set of necessary compactions, choose the one to run randomly. This gives a better
                // distribution among levels and should result in more compactions running in parallel in a big data
                // dump.
                assert !picks.isEmpty();  // we only enter this if count > F: layoutCompactions must have selected something to run
                CompactionPick selected = picks.remove(controller.random().nextInt(picks.size()));
                pending.addAll(picks);
                return selected;
            }
        }

        private List<CompactionPick> layoutCompactions(Controller controller, int maxSSTablesToCompact)
        {
            List<CompactionPick> pending = new ArrayList<>();
            int pos = layoutCompactions(controller, level.index + 1, level.fanout, maxSSTablesToCompact, pending);
            int size = maxOverlap;
            if (size - pos >= level.threshold) // can only happen in the levelled case.
            {
                assert size - pos < maxSSTablesToCompact; // otherwise it should have already been picked
                pending.add(CompactionPick.create(level.index, allSSTablesSorted));
            }
            return pending;
        }

        /**
         * Collects in {@param list} compactions of {@param sstables} such that they land in {@param level} and higher.
         *
         * Recursively combines SSTables into {@link CompactionPick}s in way that up to {@param maxSSTablesToCompact}
         * SSTables are combined to reach the highest possible level, then the rest is combined for the level before,
         * etc up to {@param level}.
         *
         * To agree with what compaction normally does, the first sstables from the list are placed in the picks that
         * combine to reach the highest levels.
         *
         * @param controller
         * @param level minimum target level for compactions to land
         * @param step - number of source SSTables required to reach level
         * @param maxSSTablesToCompact limit on the number of sstables per compaction
         * @param list - result list of layout-preserving compaction picks
         * @return index of the last used SSTable from {@param sstables}; the number of remaining sstables will be lower
         *         than step
         */
        private int layoutCompactions(Controller controller,
                                      int level,
                                      int step,
                                      int maxSSTablesToCompact,
                                      List<CompactionPick> list)
        {
            if (step > maxOverlap || step > maxSSTablesToCompact)
                return 0;

            int W = controller.getScalingParameter(level);
            int F = controller.getFanout(level);
            int pos = layoutCompactions(controller,
                                        level + 1,
                                        step * F,
                                        maxSSTablesToCompact,
                                        list);

            int total = maxOverlap;
            // step defines the number of source sstables that are needed to reach this level (ignoring overwrites
            // and deletions).
            // For tiered compaction we will select batches of this many.
            int pickSize = step;
            if (W < 0)
            {
                // For levelled compaction all the sstables that would reach this level need to be compacted to one,
                // so select the highest multiple of step that is available, but make sure we don't do a compaction
                // bigger than the limit.
                pickSize *= Math.min(total - pos, maxSSTablesToCompact) / pickSize;

                if (pickSize == 0)  // Not enough sstables to reach this level, we can skip the processing below.
                    return pos;     // Note: this cannot happen on the top level, but can on lower ones.
            }

            while (pos + pickSize <= total)
            {
                // Note that we assign these compactions to the level that would normally produce them, which means that
                // they won't be taking up threads dedicated to the busy level.
                // Normally sstables end up on a level when a compaction on the previous brings their size to the
                // threshold (which corresponds to pickSize == step, always the case for tiered); in the case of
                // levelled compaction, when we compact more than 1 but less than F sstables on a level (which
                // corresponds to pickSize > step), it is an operation that is triggered on the same level.
                list.add(CompactionPick.create(pickSize > step ? level : level - 1,
                                               pullOldestSSTables(pickSize)));
                pos += pickSize;
            }

            // In the levelled case, if we had to adjust pickSize due to maxSSTablesToCompact, there may
            // still be enough sstables to reach this level (e.g. if max was enough for 2*step, but we had 3*step).
            if (pos + step <= total)
            {
                pickSize = ((total - pos) / step) * step;
                list.add(CompactionPick.create(pickSize > step ? level : level - 1,
                                               pullOldestSSTables(pickSize)));
                pos += pickSize;
            }
            return pos;
        }

        static <T> List<T> pullLast(List<T> source, int limit)
        {
            List<T> result = new ArrayList<>(limit);
            while (--limit >= 0)
                result.add(source.remove(source.size() - 1));
            return result;
        }

        /**
         * Pull the oldest sstables to get at most limit-many overlapping sstables to compact in each overlap section.
         */
        abstract Collection<CompactionSSTable> pullOldestSSTables(int overlapLimit);
    }

    public static class SimpleBucket extends Bucket
    {
        public SimpleBucket(Level level, Collection<CompactionSSTable> sstables)
        {
            super(level, sstables, sstables.size());
        }

        Collection<CompactionSSTable> pullOldestSSTables(int overlapLimit)
        {
            if (allSSTablesSorted.size() <= overlapLimit)
                return allSSTablesSorted;
            return pullLast(allSSTablesSorted, overlapLimit);
        }
    }

    public static class MultiSetBucket extends Bucket
    {
        final List<Set<CompactionSSTable>> overlapSets;

        public MultiSetBucket(Level level, List<Set<CompactionSSTable>> overlapSets)
        {
            super(level, overlapSets);
            this.overlapSets = overlapSets;
        }

        Collection<CompactionSSTable> pullOldestSSTables(int overlapLimit)
        {
            return pullLastWithOverlapLimit(allSSTablesSorted, overlapSets, overlapLimit);
        }

        static <T> Collection<T> pullLastWithOverlapLimit(List<T> allObjectsSorted, List<Set<T>> overlapSets, int limit)
        {
            // Keep selecting the oldest sstable until the next one we would add would bring the number selected in some
            // overlap section over `limit`.
            int setsCount = overlapSets.size();
            int[] selectedInBucket = new int[setsCount];
            int allCount = allObjectsSorted.size();
            for (int selectedCount = 0; selectedCount < allCount; ++selectedCount)
            {
                T candidate = allObjectsSorted.get(allCount - 1 - selectedCount);
                for (int i = 0; i < setsCount; ++i)
                {
                    if (overlapSets.get(i).contains(candidate))
                    {
                        ++selectedInBucket[i];
                        if (selectedInBucket[i] > limit)
                            return pullLast(allObjectsSorted, selectedCount);
                    }
                }
            }
            return allObjectsSorted;
        }
    }

    static class CompactionLimits
    {
        final int runningCompactions;
        final int maxConcurrentCompactions;
        final int maxCompactions;
        final int[] perLevel;
        int levelCount;
        final long spaceAvailable;
        final String rateLimitLog;
        final int runningAdaptiveCompactions;
        final int maxAdaptiveCompactions;

        public CompactionLimits(int runningCompactions,
                                int maxCompactions,
                                int maxConcurrentCompactions,
                                int[] perLevel,
                                int levelCount,
                                long spaceAvailable,
                                String rateLimitLog,
                                int runningAdaptiveCompactions,
                                int maxAdaptiveCompactions)
        {
            this.runningCompactions = runningCompactions;
            this.maxCompactions = maxCompactions;
            this.maxConcurrentCompactions = maxConcurrentCompactions;
            this.perLevel = perLevel;
            this.levelCount = levelCount;
            this.spaceAvailable = spaceAvailable;
            this.rateLimitLog = rateLimitLog;
            this.runningAdaptiveCompactions = runningAdaptiveCompactions;
            this.maxAdaptiveCompactions = maxAdaptiveCompactions;
        }

        @Override
        public String toString()
        {
            return String.format("Current limits: running=%d, max=%d, maxConcurrent=%d, perLevel=%s, levelCount=%d, spaceAvailable=%s, rateLimitLog=%s, runningAdaptiveCompactions=%d, maxAdaptiveCompactions=%d",
                                 runningCompactions, maxCompactions, maxConcurrentCompactions, Arrays.toString(perLevel), levelCount,
                                 FBUtilities.prettyPrintMemory(spaceAvailable), rateLimitLog, runningAdaptiveCompactions, maxAdaptiveCompactions);
        }
    }
}
