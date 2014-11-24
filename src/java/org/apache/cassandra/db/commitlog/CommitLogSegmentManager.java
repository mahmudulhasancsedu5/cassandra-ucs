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
package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Uninterruptibles;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.WrappedRunnable;

/**
 * Performs eager-creation of commit log segments in a background thread. All the
 * public methods are thread safe.
 */
public class CommitLogSegmentManager
{
    static final Logger logger = LoggerFactory.getLogger(CommitLogSegmentManager.class);

    /**
     * Queue of work to be done by the manager thread.  This is usually a recycle operation, which returns
     * a CommitLogSegment, or a delete operation, which returns null.
     *
     * Note: Segment managements tasks cannot synchronize on the CommitLogSegmentManager.
     */
    private final BlockingQueue<Callable<CommitLogSegment>> segmentManagementTasks = new LinkedBlockingQueue<>();
    
    private final CommitLogVolume[] volumes;
    private final BlockingQueue<CommitLogVolume> queuedVolumes = new LinkedBlockingQueue<>(); // TODO: no priorities for now

    /** The segment we are currently allocating commit log records to */
    private volatile CommitLogSection allocatingFrom = null;

    /**
     * Tracks commitlog size, in multiples of the segment size.  We need to do this so we can "promise" size
     * adjustments ahead of actually adding/freeing segments on disk, so that the "evict oldest segment" logic
     * can see the effect of recycling segments immediately (even though they're really happening asynchronously
     * on the manager thread, which will take a ms or two).
     */
    private final AtomicLong size = new AtomicLong();

    /**
     * New segment creation is initially disabled because we'll typically get some "free" segments
     * recycled after log replay.
     */
    private volatile boolean createReserveSegments = false;

    private final Thread managerThread;
    private volatile boolean run = true;

    public CommitLogSegmentManager()
    {
        String[] volumeLocations = DatabaseDescriptor.getCommitLogLocations();
        volumes = new CommitLogVolume[volumeLocations.length];
        for (int i=0; i<volumeLocations.length; ++i)
        {
            volumes[i] = new CommitLogVolume(volumeLocations[i]);
            volumes[i].start();
        }

        // The run loop for the manager thread
        Runnable runnable = new WrappedRunnable()
        {
            public void runMayThrow() throws Exception
            {
                Callable<CommitLogSegment> task = null;
                while (run)
                {
                    // Management thread cannot call any methods that synchronize on CommitLogSegmentManager.this or
                    // it will deadlock with advanceVolume().
                    try
                    {
                        // Process any requested tasks
                        while (task != null)
                        {
                            CommitLogSegment recycled = task.call();
                            if (recycled != null)
                            {
                                // if the work resulted in a segment to recycle, publish it
                                CommitLogVolume volume = recycled.volume;
                                if (volume.makeAvailable(recycled))
                                    queuedVolumes.add(volume);
                            }
                            task = segmentManagementTasks.poll();
                        }

                        // No more requested work, check if we should create new segment(s)
                        for (CommitLogVolume volume : volumes)
                        {
                            if (!volume.hasAvailableSegments() && (volume.activeSegments.isEmpty() || createReserveSegments))
                            {
                                logger.debug("No segments in reserve; creating a fresh one");
                                size.addAndGet(DatabaseDescriptor.getCommitLogSegmentSize());
                                // TODO : some error handling in case we fail to create a new segment
                                // TODO : one drive full/bad should not stop the whole node
                                CommitLogSegment segment = volume.freshSegment();
                                if (volume.makeAvailable(segment))
                                    queuedVolumes.add(volume);
                            }
                        }

                        // flush old Cfs if we're full
                        long unused = unusedCapacity();
                        if (allocatingFrom != null && unused < 0)
                        {
                            List<CommitLogSegment> segmentsToRecycle = new ArrayList<>();
                            long spaceToReclaim = 0;
                            for (CommitLogVolume volume : volumes)
                                for (CommitLogSegment segment : volume.activeSegments)
                                {
                                    if (segment == allocatingFrom.segment)
                                        break;
                                    segmentsToRecycle.add(segment);
                                    spaceToReclaim += DatabaseDescriptor.getCommitLogSegmentSize();
                                    if (spaceToReclaim + unused >= 0)
                                        break;
                                }
                            flushDataFrom(segmentsToRecycle, false);
                        }

                        try
                        {
                            // wait for new work to be provided
                            task = segmentManagementTasks.take();
                        }
                        catch (InterruptedException e)
                        {
                            // shutdown signal; exit cleanly
                            continue;
                        }
                    }
                    catch (Throwable t)
                    {
                        JVMStabilityInspector.inspectThrowable(t);
                        if (!CommitLog.handleCommitError("Failed managing commit log segments", t))
                            return;
                        // sleep some arbitrary period to avoid spamming CL
                        Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
                    }
                }
            }
        };

        managerThread = new Thread(runnable, "COMMIT-LOG-ALLOCATOR");
        managerThread.start();
    }

    /**
     * Reserve space in the current segment for the provided mutation or, if there isn't space available,
     * create a new segment.
     *
     * @return the provided Allocation object
     */
    public Allocation allocate(Mutation mutation, int size)
    {
        CommitLogSection section = allocatingFrom();

        Allocation alloc;
        while ( null == (alloc = section.allocate(mutation, size)) )
        {
            section = advanceVolume(section);
        }

        return alloc;
    }

    private CommitLogSection allocatingFrom()
    {
        if (allocatingFrom != null)
            return allocatingFrom;
        return advanceVolume(null);
    }

    /**
     * Switches to the next volume. Idempotent.
     *
     * @param fromSection The section we are switching from.
     * @return New section to use.
     */
    private CommitLogSection advanceVolume(CommitLogSection fromSection)
    {
        if (allocatingFrom != fromSection)
            return allocatingFrom;

        // Lock necessary to ensure atomicity of:
        // - taking from volumes queue
        // - setting allocatingFrom
        // - assigning section id
        // Advancing section in the current volume doesn't necessarily need to happen in the same lock (as long as
        // advance happens before taking an element from the volume queue to ensure single-volume commit logs work);
        // having it execute within it simplifies the code.

        synchronized (this)
        {
            if (allocatingFrom != fromSection)
                return allocatingFrom;

            if (fromSection != null)
            {
                CommitLogVolume fromVolume = fromSection.volume;
                if (fromVolume.advanceSection(fromSection))
                    queuedVolumes.add(fromVolume);  // Put in queue only with available segment.
                else
                    wakeManager();
            }
            // Section ID is allocated here.
            // This can take forever. Other write threads should wait too.
            return allocatingFrom = Uninterruptibles.takeUninterruptibly(queuedVolumes).startSection();
        }
    }

    public ReplayPosition getContext()
    {
        return allocatingFrom().getContext();
    }

    // Blocking until sync is complete.
    public void sync(boolean syncAllSegments)
    {
        final CommitLogSection currentSection = allocatingFrom;
        if (currentSection == null)
            return;
        ReplayPosition position = syncAllSegments ? ReplayPosition.MAX_VALUE : currentSection.getContext();
        if (!currentSection.isEmpty())
            advanceVolume(currentSection);

        for (CommitLogVolume volume : volumes)
            volume.waitForSync(position);
    }

    void wakeManager()
    {
        // put a NO-OP on the queue, to trigger management thread (and create a new segment if necessary)
        segmentManagementTasks.add(new Callable<CommitLogSegment>()
        {
            public CommitLogSegment call()
            {
                return null;
            }
        });
    }

    /**
     * Switch to a new segment, regardless of how much is left in the current one.
     *
     * Flushes any dirty CFs for this segment and any older segments, and then recycles
     * the segments
     */
    void forceRecycleAll(Iterable<UUID> droppedCfs)
    {
        Collection<CommitLogSegment> segmentsToRecycle = getActiveSegments();

        // Queued volumes have an active segment with no attached section. Go through them to advance from that segment.
        // Synchronization needed to ensure the volumes we work with do not become active, which could create a section
        // on a closed segment, and that we don't miss a volume because it became active.
        CommitLogSection lastSection;
        boolean managementNeeded = false;
        synchronized (this)
        {
            lastSection = allocatingFrom();
            for (Iterator<CommitLogVolume> it = queuedVolumes.iterator(); it.hasNext(); )
            {
                CommitLogVolume volume = it.next();
                if (!volume.forceSegmentChange())
                {
                    it.remove();
                    managementNeeded = true;
                }
            }
        }
        // Now close the currently active volume's segment. This one contains an active section, so advance by closing
        // that. These actions are idempotent and don't need to be done synchronized.
        lastSection.segment.markComplete();
        advanceVolume(lastSection);
        if (managementNeeded)
            wakeManager();

        // make sure the writes have materialized inside of the memtables by waiting for all outstanding writes
        // on the relevant keyspaces to complete
        Set<Keyspace> keyspaces = new HashSet<>();
        for (UUID cfId : lastSection.segment.getDirtyCFIDs())   // Is this enough? Segment may be completely empty.
        {
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(cfId);
            if (cfs != null)
                keyspaces.add(cfs.keyspace);
        }
        for (Keyspace keyspace : keyspaces)
            keyspace.writeOrder.awaitNewBarrier();
        lastSection.waitForSync();

        // flush and wait for all CFs that are dirty in segments up-to and including 'last'
        Future<?> future = flushDataFrom(segmentsToRecycle, true);
        try
        {
            future.get();

            for (CommitLogVolume volume : volumes)
            {
                for (CommitLogSegment segment : volume.activeSegments)
                    for (UUID cfId : droppedCfs)
                        segment.markClean(cfId, ReplayPosition.MAX_VALUE);

                // now recycle segments that are unused, as we may not have triggered a discardCompletedSegments()
                // if the previous active segment was the only one to recycle (since an active segment isn't
                // necessarily dirty, and we only call dCS after a flush).
                for (CommitLogSegment segment : volume.activeSegments)
                    if (segment.isUnused())
                        recycleSegment(segment);

                CommitLogSegment first;
                if ((first = volume.activeSegments.peek()) != null && first.isCreatedBefore(lastSection.segment))
                    logger.error("Failed to force-recycle all segments; at least one segment is still in use with dirty CFs.");
            }
        }
        catch (Throwable t)
        {
            // for now just log the error and return false, indicating that we failed
            logger.error("Failed waiting for a forced recycle of in-use commit log segments", t);
        }
    }

    /**
     * Indicates that a segment is no longer in use and that it should be recycled.
     *
     * @param segment segment that is no longer in use
     */
    void recycleSegment(final CommitLogSegment segment)
    {
        boolean archiveSuccess = CommitLog.instance.archiver.maybeWaitForArchiving(segment.getName());
        segment.volume.makeInactive(segment);
        if (!archiveSuccess)
        {
            // if archiving (command) was not successful then leave the file alone. don't delete or recycle.
            discardSegment(segment, false);
            return;
        }
        if (isCapExceeded())
        {
            discardSegment(segment, true);
            return;
        }

        logger.debug("Recycling {}", segment);
        segmentManagementTasks.add(new Callable<CommitLogSegment>()
        {
            public CommitLogSegment call()
            {
                return segment.recycle();
            }
        });
    }

    /**
     * Differs from the above because it can work on any file instead of just existing
     * commit log segments managed by this manager.
     *
     * @param file segment file that is no longer in use.
     */
    void recycleSegment(final File file)
    {
        if (isCapExceeded()
            || CommitLogDescriptor.fromFileName(file.getName()).getMessagingVersion() != MessagingService.current_version)
        {
            // (don't decrease managed size, since this was never a "live" segment)
            logger.debug("(Unopened) segment {} is no longer needed and will be deleted now", file);
            FileUtils.deleteWithConfirm(file);
            return;
        }

        for (final CommitLogVolume volume : volumes)
            if (volume.shouldManage(file))
            {
                logger.debug("Recycling {}", file);
                // this wasn't previously a live segment, so add it to the managed size when we make it live
                size.addAndGet(DatabaseDescriptor.getCommitLogSegmentSize());
                segmentManagementTasks.add(new Callable<CommitLogSegment>()
                {
                    public CommitLogSegment call()
                    {
                        return new CommitLogSegment(volume, file.getPath());
                    }
                });
                return;
            }

        // This is not part of any managed volume. Delete.
        {
            // (don't decrease managed size, since this was never a "live" segment)
            logger.debug("(Unopened) segment {} is not in any of the commit log directories and will be deleted now", file);
            FileUtils.deleteWithConfirm(file);
            return;
        }
    }

    /**
     * Indicates that a segment file should be deleted.
     *
     * @param segment segment to be discarded
     */
    private void discardSegment(final CommitLogSegment segment, final boolean deleteFile)
    {
        logger.debug("Segment {} is no longer active and will be deleted {}", segment, deleteFile ? "now" : "by the archive script");
        size.addAndGet(-DatabaseDescriptor.getCommitLogSegmentSize());

        segmentManagementTasks.add(new Callable<CommitLogSegment>()
        {
            public CommitLogSegment call()
            {
                segment.close();
                if (deleteFile)
                    segment.delete();
                return null;
            }
        });
    }

    /**
     * @return the space (in bytes) used by all segment files.
     */
    public long bytesUsed()
    {
        return size.get();
    }

    /**
     * @param name the filename to check
     * @return true if file is managed by this manager.
     */
    public boolean manages(String name)
    {
        for (CommitLogVolume volume : volumes)
            if (volume.manages(name))
                    return true;
        return false;
    }

    /**
     * Check to see if the speculative current size exceeds the cap.
     *
     * @return true if cap is exceeded
     */
    private boolean isCapExceeded()
    {
        return unusedCapacity() < 0;
    }

    private long unusedCapacity()
    {
        long currentSize = size.get();
        logger.debug("Total active commitlog segment space used is {}", currentSize);
        return DatabaseDescriptor.getTotalCommitlogSpaceInMB() * 1024 * 1024 - currentSize;
    }

    /**
     * Throws a flag that enables the behavior of keeping at least one spare segment
     * available at all times.
     */
    public void enableReserveSegmentCreation()
    {
        createReserveSegments = true;
        wakeManager();
    }

    /**
     * Force a flush on all CFs that are still dirty in @param segments.
     *
     * @return a Future that will finish when all the flushes are complete.
     */
    private Future<?> flushDataFrom(Collection<CommitLogSegment> segments, boolean force)
    {
        if (segments.isEmpty())
            return Futures.immediateFuture(null);
        final ReplayPosition maxReplayPosition = getContext();

        // a map of CfId -> forceFlush() to ensure we only queue one flush per cf
        final Map<UUID, ListenableFuture<?>> flushes = new LinkedHashMap<>();

        for (CommitLogSegment segment : segments)
        {
            for (UUID dirtyCFId : segment.getDirtyCFIDs())
            {
                Pair<String,String> pair = Schema.instance.getCF(dirtyCFId);
                if (pair == null)
                {
                    // even though we remove the schema entry before a final flush when dropping a CF,
                    // it's still possible for a writer to race and finish his append after the flush.
                    logger.debug("Marking clean CF {} that doesn't exist anymore", dirtyCFId);
                    segment.markClean(dirtyCFId, ReplayPosition.MAX_VALUE);
                }
                else if (!flushes.containsKey(dirtyCFId))
                {
                    String keyspace = pair.left;
                    final ColumnFamilyStore cfs = Keyspace.open(keyspace).getColumnFamilyStore(dirtyCFId);
                    // can safely call forceFlush here as we will only ever block (briefly) for other attempts to flush,
                    // no deadlock possibility since switchLock removal
                    flushes.put(dirtyCFId, force ? cfs.forceFlush() : cfs.forceFlush(maxReplayPosition));
                }
            }
        }

        return Futures.allAsList(flushes.values());
    }

    /**
     * Resets all the segments, for testing purposes. DO NOT USE THIS OUTSIDE OF TESTS.
     */
    public void resetUnsafe()
    {
        logger.debug("Closing and clearing existing commit log segments...");

        while (!segmentManagementTasks.isEmpty())
            Thread.yield();
        for (CommitLogVolume volume : volumes)
            volume.resetUnsafe();

        queuedVolumes.clear();
        allocatingFrom = null;
        size.set(0);
        wakeManager();
    }

    /**
     * Initiates the shutdown process for the management thread.
     */
    public void shutdown()
    {
        for (CommitLogVolume volume : volumes)
            volume.shutdown();
        run = false;
        managerThread.interrupt();
    }

    /**
     * Returns when the management thread terminates.
     */
    public void awaitTermination() throws InterruptedException
    {
        for (CommitLogVolume volume : volumes)
            volume.join();
        managerThread.join();
    }

    /**
     * @return a read-only collection of the active commit log segments
     */
    Collection<CommitLogSegment> getActiveSegments()
    {
        ImmutableList.Builder<CommitLogSegment> builder = ImmutableList.builder();
        for (CommitLogVolume volume : volumes)
            builder.addAll(volume.activeSegments);
        return builder.build();
    }
}

