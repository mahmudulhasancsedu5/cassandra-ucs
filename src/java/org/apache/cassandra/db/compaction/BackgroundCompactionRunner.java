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
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.primitives.Longs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
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

    private final DebuggableScheduledThreadPoolExecutor checkExecutor;

    /**
     * Each time we start a compaction, the CFS is added to this set; it is removed when the compaction finishes
     * or when it is cancelled
     */
    private final Multiset<ColumnFamilyStore> ongoingCompactions = ConcurrentHashMultiset.create();

    /**
     * CFSs for which a compaction was requested mapped to the promise returned to the requesting code
     */
    private final ConcurrentMap<ColumnFamilyStore, List<CompletableFuture<RequestResult>>> compactionRequests = new ConcurrentHashMap<>();

    private final AtomicInteger currentlyBackgroundUpgrading = new AtomicInteger(0);

    private final Random random = new Random();

    private final DebuggableThreadPoolExecutor compactionExecutor;

    private final ActiveOperations activeOperations;


    BackgroundCompactionRunner(DebuggableThreadPoolExecutor compactionExecutor, ActiveOperations activeOperations)
    {
        this(compactionExecutor, new DebuggableScheduledThreadPoolExecutor("BackgroundTaskExecutor"), activeOperations);
    }

    @VisibleForTesting
    BackgroundCompactionRunner(DebuggableThreadPoolExecutor compactionExecutor, DebuggableScheduledThreadPoolExecutor checkExecutor, ActiveOperations activeOperations)
    {
        this.compactionExecutor = compactionExecutor;
        this.checkExecutor = checkExecutor;
        this.activeOperations = activeOperations;
    }

    /**
     * Marks a CFS for compaction. Since marked, it will become a possible candidate for compaction. The mark will be
     * cleared when we actually run the compaction for CFS.
     *
     * @return a promise which will be completed when the mark is cleared
     */
    CompletableFuture<RequestResult> requestCompaction(ColumnFamilyStore cfs)
    {
        logger.trace("Requested background compaction for {}.{}", cfs.getKeyspaceName(), cfs.getTableName());

        if (checkExecutor.isShutdown())
        {
            logger.info("Executor has been shut down, background compactions check will not be scheduled");
            CompletableFuture<RequestResult> futureResult = new CompletableFuture<>();
            futureResult.cancel(false);
            return futureResult;
        }

        CompletableFuture<RequestResult> futureResult = new CompletableFuture<>();
        compactionRequests.compute(cfs, (ignored, current) -> {
            if (current == null)
                current = Collections.synchronizedList(new ArrayList<>());
            current.add(futureResult);
            return current;
        });

        if (!maybeScheduleNextCheck())
        {
            futureResult.cancel(false);
        }
        return futureResult;
    }

    void shutdown()
    {
        checkExecutor.shutdown();
        compactionRequests.values().forEach(promises -> promises.forEach(p -> p.cancel(false)));
    }

    int getOngoingCompactionsCount(ColumnFamilyStore cfs)
    {
        return ongoingCompactions.count(cfs);
    }

    @Override
    public void run()
    {
        logger.trace("Running background compactions check");

        // We shuffle the CFSs for which the compaction was requested so that with each run we traverse those CFSs
        // in different order and make each CFS have equal chance to be selected
        ArrayList<ColumnFamilyStore> compactionRequestsList = new ArrayList<>(compactionRequests.keySet());
        Collections.shuffle(compactionRequestsList, random);

        for (ColumnFamilyStore cfs : compactionRequestsList)
        {
            List<CompletableFuture<RequestResult>> promises = compactionRequests.remove(cfs);

            logger.trace("Checking compaction request for {}.{} - there are {} awaiting promises",
                         cfs.getKeyspaceName(),
                         cfs.getTableName(),
                         promises.size());

            RequestResult completionStatus = canRunCompactionTasks(cfs, promises);
            if (completionStatus != null)
            {
                promises.stream().filter(p -> !p.isDone()).forEach(p -> p.complete(completionStatus));
                continue;
            }

            logger.trace("Scheduling a background task check for {}.{} with {}",
                         cfs.keyspace.getName(),
                         cfs.name,
                         cfs.getCompactionStrategy().getName());

            Supplier<CompletableFuture<?>> compactionTask = Optional.ofNullable(lazyRunCompactionTasks(cfs))
                                                                    .orElseGet(() -> lazyRunUpgradeTasks(cfs));

            if (compactionTask != null)
            {
                ongoingCompactions.add(cfs);

                compactionTask.get().handle((ignored, throwable) -> {
                    ongoingCompactions.remove(cfs);

                    if (throwable != null)
                    {
                        logger.warn(String.format("Aborting compaction of %s.%s due to error", cfs.getKeyspaceName(), cfs.getTableName()), throwable);
                        handleCompactionError(throwable, cfs);

                        promises.forEach(p -> p.completeExceptionally(throwable));
                    }
                    else
                    {
                        logger.trace("Finished compaction for {}.{}", cfs.getKeyspaceName(), cfs.getTableName());

                        // Since we have run at least one task and thus the set of sstables has changed,
                        // check for new compaction possibilities
                        requestCompaction(cfs);

                        promises.forEach(p -> p.complete(RequestResult.COMPLETED));
                    }

                    return null;
                });
            }
            else
            {
                promises.forEach(p -> p.complete(RequestResult.COMPLETED));
            }
        }
    }

    private boolean maybeScheduleNextCheck()
    {
        if (checkExecutor.getQueue().isEmpty())
        {
            try
            {
                checkExecutor.execute(this);
            }
            catch (RejectedExecutionException ex)
            {
                if (checkExecutor.isShutdown())
                    logger.info("Executor has been shut down, background compactions check will not be scheduled");
                else
                    logger.error("Failed to submit background compactions check", ex);

                return false;
            }
        }

        return true;
    }

    private Supplier<CompletableFuture<?>> lazyRunCompactionTasks(ColumnFamilyStore cfs)
    {
        Collection<AbstractCompactionTask> compactionTasks = cfs.getCompactionStrategy()
                                                                .getNextBackgroundTasks(CompactionManager.getDefaultGcBefore(cfs, FBUtilities.nowInSeconds()));
        if (!compactionTasks.isEmpty())
        {
            return () -> {
                logger.debug("Running compaction tasks: {}", compactionTasks);
                return CompletableFuture.allOf(compactionTasks.stream()
                                                              .map(task -> CompletableFuture.runAsync(() -> task.execute(activeOperations), compactionExecutor))
                                                              .toArray(CompletableFuture<?>[]::new));
            };
        }
        else
        {
            logger.debug("No compaction tasks for {}.{}", cfs.getKeyspaceName(), cfs.getTableName());
            return null;
        }
    }

    private Supplier<CompletableFuture<?>> lazyRunUpgradeTasks(ColumnFamilyStore cfs)
    {
        AbstractCompactionTask upgradeTask = getUpgradeSSTableTask(cfs);

        if (upgradeTask != null)
        {
            return () -> {
                logger.debug("Running upgrade task: {}", upgradeTask);
                return CompletableFuture.runAsync(() -> upgradeTask.execute(activeOperations), compactionExecutor)
                                        .handle((ignored1, ignored2) -> {
                                            currentlyBackgroundUpgrading.decrementAndGet();
                                            return null;
                                        });
            };
        }
        else
        {
            logger.debug("No upgrade tasks for {}.{}", cfs.getKeyspaceName(), cfs.getTableName());
            return null;
        }
    }

    /**
     * Checks if we can/should run compaction tasks for the CFS, given the provided promises. If we should run
     * compaction tasks for that CFS, we will return {@code null}, as there is no status to be set yet on the promises.
     * If the compaction tasks should not be run, we will return a non-null {@link RequestResult} to be set on the
     * awaiting (not done) promises. In that case, we should skip the compaction for this CFS.
     */
    private RequestResult canRunCompactionTasks(ColumnFamilyStore cfs, List<CompletableFuture<RequestResult>> promises)
    {
        if (promises.stream().allMatch(CompletableFuture::isDone))
        {
            logger.trace("There are no awaiting requests for {}.{} (all the requests were cancelled)",
                         cfs.getKeyspaceName(), cfs.getTableName());
            return RequestResult.COMPLETED;
        }

        // when the compaction is already running for CFS and the executor is fully occupied
        // we will skip and cancel the request
        if (ongoingCompactions.count(cfs) > 0 && compactionExecutor.getActiveTaskCount() >= compactionExecutor.getMaximumPoolSize())
        {
            logger.trace("Background compaction is still running for {}.{} - skipping",
                         cfs.getKeyspaceName(), cfs.getTableName());
            return RequestResult.SKIPPED;
        }

        if (!cfs.isValid())
        {
            logger.trace("Aborting compaction for dropped CF {}.{}", cfs.getKeyspaceName(), cfs.getTableName());
            promises.forEach(p -> p.complete(RequestResult.ABORTED));
            return RequestResult.ABORTED;
        }

        return null;
    }

    /**
     * Finds the oldest (by modification date) non-latest-version sstable on disk and creates an upgrade task for it
     */
    @VisibleForTesting
    public AbstractCompactionTask getUpgradeSSTableTask(ColumnFamilyStore cfs)
    {
        logger.trace("Checking for upgrade tasks {}.{}", cfs.getKeyspaceName(), cfs.getTableName());

        if (!DatabaseDescriptor.automaticSSTableUpgrade())
        {
            logger.trace("Automatic sstable upgrade is disabled - will not try to upgrade sstables of {}.{}",
                         cfs.getKeyspaceName(),
                         cfs.getTableName());
            return null;
        }

        if (currentlyBackgroundUpgrading.incrementAndGet() <= DatabaseDescriptor.maxConcurrentAutoUpgradeTasks())
        {
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
                    logger.debug("Found tasks for automatic sstable upgrade of {}", sstable);
                    return cfs.getCompactionStrategy().createCompactionTask(txn, Integer.MIN_VALUE, Long.MAX_VALUE);
                }
            }
        }
        else
        {
            logger.trace("Skipped upgrade task for {}.{} because the limit {} of concurrent upgrade tasks has been reached",
                         cfs.getKeyspaceName(),
                         cfs.getTableName(),
                         DatabaseDescriptor.maxConcurrentAutoUpgradeTasks());
        }

        currentlyBackgroundUpgrading.decrementAndGet();
        return null;
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
