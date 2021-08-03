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
import java.io.IOError;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.SettableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.FSDiskFullWriteError;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;

public class BackgroundCompactionRunner implements Runnable
{
    private final static Logger logger = LoggerFactory.getLogger(BackgroundCompactionRunner.class);

    public enum RequestResult
    {
        /**
         * when the compaction was decided not to run because there were already running compactions for this CFS
         */
        SKIPPED,

        /**
         * when the compaction was aborted for the CFS because the CF got dropped in the meantime
         */
        ABORTED,

        /**
         * compaction tasks were completed for the CFS
         */
        COMPLETED
    }

    /**
     * Each time we start a compaction, the CFS is added to this set; it is removed when the compaction finishes
     * or when it is cancelled
     */
    private final Multiset<ColumnFamilyStore> ongoingCompactions = ConcurrentHashMultiset.create();

    /**
     * CFSs for which a compaction was requested mapped to the promise returned to the requesting code
     */
    private final ConcurrentMap<ColumnFamilyStore, List<SettableFuture<RequestResult>>> compactionRequests = new ConcurrentHashMap<>();

    /**
     * Futures of compactions or upgrades for each started task mapped to the corresponding CFS.
     * This map is used to do the {@link #cleanup()} - when the compaction gets cancelled we need to remove one CFSs
     * occurence from {@link #ongoingCompactions}.
     */
    private final ConcurrentMap<Future<?>, Pair<ColumnFamilyStore, List<SettableFuture<RequestResult>>>> compactionResults = new ConcurrentHashMap<>();

    private final AtomicInteger currentlyBackgroundUpgrading = new AtomicInteger(0);

    private final Random random = new Random();

    private final DebuggableThreadPoolExecutor executor;

    private final ActiveOperations active;

    BackgroundCompactionRunner(DebuggableThreadPoolExecutor compactionExecutor, ActiveOperations activeOperations)
    {
        this.executor = compactionExecutor;
        this.active = activeOperations;
    }

    /**
     * Marks a CFS for compaction. Since marked, it will become a possible candidate for compaction. The mark will be
     * cleared when we actually run the compaction for CFS.
     *
     * @return a promise which will be completed when the mark is cleared
     */
    Future<RequestResult> requestCompaction(ColumnFamilyStore cfs)
    {
        SettableFuture<RequestResult> futureResult = SettableFuture.create();
        compactionRequests.compute(cfs, (ignored, current) -> {
            if (current == null)
                current = Collections.synchronizedList(new ArrayList<>());
            current.add(futureResult);
            return current;
        });
        return futureResult;
    }

    @Override
    public void run()
    {
        // We shuffle the CFSs for which the compaction was requested so that with each run we traverse those CFSs
        // in different order and make each CFS have equal chance to be selected
        ArrayList<ColumnFamilyStore> compactionRequestsList = new ArrayList<>(compactionRequests.keySet());
        Collections.shuffle(compactionRequestsList, random);

        cleanup();

        for (ColumnFamilyStore cfs : compactionRequestsList)
        {
            // take the request
            List<SettableFuture<RequestResult>> promises = compactionRequests.remove(cfs);

            logger.trace("Checking compaction request for {}.{} - there are {} awaiting promises",
                         cfs.getKeyspaceName(),
                         cfs.getTableName(),
                         promises.size());

            if (promises.stream().allMatch(p -> p.isDone()))
            {
                logger.trace("There are no awaiting requests for {}.{} (all the requests were cancelled)",
                             cfs.getKeyspaceName(), cfs.getTableName());
                continue;
            }

            // when the compaction is already running for CFS and the executor is fully occupied
            // we will skip and cancel the request
            if (hasEnoughCompactionsRunning(cfs))
            {
                logger.trace("Background compaction is still running for {}.{} - skipping",
                             cfs.getKeyspaceName(), cfs.getTableName());
                promises.forEach(p -> p.set(RequestResult.SKIPPED));
                continue;
            }

            if (!cfs.isValid())
            {
                logger.trace("Aborting compaction for dropped CF {}.{}", cfs.getKeyspaceName(), cfs.getTableName());
                promises.forEach(p -> p.set(RequestResult.ABORTED));
                continue;
            }

            logger.trace("Scheduling a background task check for {}.{} with {}",
                         cfs.keyspace.getName(),
                         cfs.name,
                         cfs.getCompactionStrategy().getName());

            Collection<AbstractCompactionTask> tasks = cfs.getCompactionStrategy()
                                                          .getNextBackgroundTasks(CompactionManager.getDefaultGcBefore(cfs, FBUtilities.nowInSeconds()));

            CompletableFuture<?> resultFuture = tasks.isEmpty()
                                                ? DatabaseDescriptor.automaticSSTableUpgrade() ? maybeRunUpgradeTask(cfs) : null
                                                : runAllCompactionTasks(tasks);

            if (resultFuture != null)
            {
                ongoingCompactions.add(cfs);
                compactionResults.put(resultFuture, Pair.create(cfs, promises));

                resultFuture.handle((ignored, throwable) -> {
                    removeCompaction(resultFuture);

                    if (throwable != null)
                    {
                        logger.warn(String.format("Aborting compaction of %s.%s due to error", cfs.getKeyspaceName(), cfs.getTableName()), throwable);
                        promises.forEach(p -> p.setException(throwable));
                        handleCompactionError(throwable, cfs);
                    }
                    else
                    {
                        promises.forEach(p -> p.set(RequestResult.COMPLETED));
                    }

                    return null;
                });
            }
        }
    }


    private void cleanup()
    {
        List<Future<?>> toCleanUp = compactionResults.keySet().stream()
                                                     .filter(Future::isCancelled)
                                                     .collect(Collectors.toList());

        toCleanUp.forEach(this::removeCompaction);
    }

    private void removeCompaction(Future<?> resultFuture)
    {
        Pair<ColumnFamilyStore, List<SettableFuture<RequestResult>>> cfsWithPromises = compactionResults.remove(resultFuture);
        if (cfsWithPromises != null)
        {
            ColumnFamilyStore cfs = cfsWithPromises.left;
            ongoingCompactions.remove(cfs);
            cfsWithPromises.right.forEach(p -> {
                if (!p.isDone())
                    p.cancel(false);
            });
            logger.trace("Removed compaction for {}.{}", cfs.getKeyspaceName(), cfs.getTableName());
        }
    }

    private CompletableFuture<?> runAllCompactionTasks(Collection<AbstractCompactionTask> tasks)
    {
        logger.debug("Running compaction tasks: {}", tasks);
        return CompletableFuture.allOf(tasks.stream()
                                            .map(task -> CompletableFuture.runAsync(() -> task.execute(active), executor))
                                            .toArray(CompletableFuture<?>[]::new));
    }

    CompletableFuture<?> maybeRunUpgradeTask(ColumnFamilyStore cfs)
    {
        logger.trace("Checking for upgrade tasks {}.{}", cfs.getKeyspaceName(), cfs.getTableName());
        if (currentlyBackgroundUpgrading.incrementAndGet() <= DatabaseDescriptor.maxConcurrentAutoUpgradeTasks())
        {
            AbstractCompactionTask upgradeTask = findUpgradeSSTableTask(cfs);
            if (upgradeTask != null)
            {
                logger.debug("Running upgrade task: {}", upgradeTask);
                return CompletableFuture.runAsync(() -> upgradeTask.execute(active), executor)
                                        .handle((ignored1, ignored2) -> {
                                            currentlyBackgroundUpgrading.decrementAndGet();
                                            return null;
                                        });
            }
            else
            {
                logger.trace("No tasks available for {}.{}", cfs.getKeyspaceName(), cfs.getTableName());
                currentlyBackgroundUpgrading.decrementAndGet();
            }
        }
        else
        {
            currentlyBackgroundUpgrading.decrementAndGet();
            logger.trace("Skipped upgrade task for {}.{} because the limit of concurrent upgrade tasks has been reached", cfs.getKeyspaceName(), cfs.getTableName());
        }
        return null;
    }

    /**
     * Finds the oldest (by modification date) non-latest-version sstable on disk and creates an upgrade task for it
     */
    @VisibleForTesting
    public AbstractCompactionTask findUpgradeSSTableTask(ColumnFamilyStore cfs)
    {
        if (cfs.isAutoCompactionDisabled() || !DatabaseDescriptor.automaticSSTableUpgrade())
            return null;

        Set<SSTableReader> compacting = cfs.getTracker().getCompacting();
        List<SSTableReader> potentialUpgrade = cfs.getLiveSSTables()
                                                  .stream()
                                                  .filter(s -> !compacting.contains(s) && !s.descriptor.version.isLatestVersion())
                                                  .sorted((o1, o2) -> {
                                                      File f1 = new File(o1.descriptor.filenameFor(Component.DATA));
                                                      File f2 = new File(o2.descriptor.filenameFor(Component.DATA));
                                                      return Longs.compare(f1.lastModified(), f2.lastModified());
                                                  }).collect(Collectors.toList());
        for (SSTableReader sstable : potentialUpgrade)
        {
            LifecycleTransaction txn = cfs.getTracker().tryModify(sstable, OperationType.UPGRADE_SSTABLES);
            if (txn != null)
            {
                logger.debug("Running automatic sstable upgrade for {}", sstable);
                return cfs.getCompactionStrategy().createCompactionTask(txn, Integer.MIN_VALUE, Long.MAX_VALUE);
            }
        }
        return null;
    }

    private boolean hasEnoughCompactionsRunning(ColumnFamilyStore cfs)
    {
        int count = ongoingCompactions.count(cfs);
        return count > 0 && executor.getActiveTaskCount() >= executor.getMaximumPoolSize();
    }

    private static void handleCompactionError(Throwable t, ColumnFamilyStore cfs)
    {
        t = Throwables.unwrapped(t);
        // FSDiskFullWriteErrors caught during compaction are expected to be recoverable, so we don't explicitly
        // trigger the disk failure policy because of them (see CASSANDRA-12385).
        if (t instanceof IOError && !(t instanceof FSDiskFullWriteError))
        {
            logger.error("Potentially unrecoverable error during background compaction of table {}", cfs, t);
            // Strictly speaking it's also possible to hit a read-related IOError during compaction, although the
            // chances for that are much lower than the chances for write-related IOError. If we want to handle that,
            // we might have to rely on error message parsing...
            t = t instanceof FSError ? t : new FSWriteError(t);
            JVMStabilityInspector.inspectThrowable(t);
        }
        else
        {
            logger.error("Exception during background compaction of table {}", cfs, t);
        }
    }
}
