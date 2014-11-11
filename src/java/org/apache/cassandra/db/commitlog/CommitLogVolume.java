package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.cassandra.utils.concurrent.WaitQueue;

import com.google.common.collect.Iterables;

public class CommitLogVolume extends Thread
{
    final String location;

    /** Segments that are ready to be used. Head of the queue is the one we allocate writes to */
    private final ConcurrentLinkedQueue<CommitLogSegment> availableSegments = new ConcurrentLinkedQueue<>();

    /** Active segments, containing unflushed data */
    final ConcurrentLinkedQueue<CommitLogSegment> activeSegments = new ConcurrentLinkedQueue<>();

    private final WaitQueue hasAvailableSegments = new WaitQueue();

    /** The segment we are currently allocating commit log records to */
    private volatile CommitLogSegment allocatingFrom = null;
    
    private final BlockingQueue<CommitLogSection> syncQueue = new LinkedBlockingQueue<>();
    private final WaitQueue syncScheduled = new WaitQueue();
    private boolean run = true;

    public CommitLogVolume(String location)
    {
        super("COMMIT_LOG_VOLUME_SYNC: " + location);
        this.location = location;
        start();
    }

    public void run()
    {
        while (run)
        {
            try
            {
                CommitLogSection section = syncQueue.peek();
                while (section == null)
                {
                    WaitQueue.Signal signal = syncScheduled.register();
                    section = syncQueue.peek();
                    if (section == null)
                        signal.await();
                    else
                        signal.cancel();
                    section = syncQueue.peek();
                }
                section.sync();
                syncQueue.take();
            }
            catch (InterruptedException e)
            {
                // Continue to shut down cleanly.
            }
        }
    }

    public CommitLogSegment freshSegment()
    {
        return new CommitLogSegment(this, null);
    }


    /**
     * @param name the filename to check
     * @return true if file is managed by this volume thread.
     */
    public boolean manages(String name)
    {
        for (CommitLogSegment segment : Iterables.concat(activeSegments, availableSegments))
            if (segment.getName().equals(name))
                return true;
        return false;
    }

    /**
     * Checks if the given file belongs to this volume.
     * @param file the filename to check
     * @return true if file should be managed by this volume thread.
     */
    public boolean shouldManage(File file)
    {
        return file.getParentFile().equals(location);
    }

    public void makeAvailable(CommitLogSegment segment)
    {
        availableSegments.add(segment);
        hasAvailableSegments.signalAll();
    }

    // simple wrapper to ensure non-null value for allocatingFrom; only necessary on first call
    CommitLogSegment allocatingFrom(CommitLogSegmentManager manager)
    {
        CommitLogSegment r = allocatingFrom;
        if (r == null)
        {
            advanceAllocatingFrom(null, manager);
            r = allocatingFrom;
        }
        return r;
    }

    /**
     * Fetches a new segment from the queue, creating a new one if necessary, and activates it
     */
    void advanceAllocatingFrom(CommitLogSegment old, CommitLogSegmentManager manager)
    {
        while (true)
        {
            CommitLogSegment next;
            synchronized (this)
            {
                // do this in a critical section so we can atomically remove from availableSegments and add to allocatingFrom/activeSegments
                // see https://issues.apache.org/jira/browse/CASSANDRA-6557?focusedCommentId=13874432&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13874432
                if (allocatingFrom != old)
                    return;
                next = availableSegments.poll();
                if (next != null)
                {
                    allocatingFrom = next;
                    activeSegments.add(next);
                }
            }

            if (next != null)
            {
                if (old != null)
                {
                    // Now we can run the user defined command just after switching to the new commit log.
                    // (Do this here instead of in the recycle call so we can get a head start on the archive.)
                    CommitLog.instance.archiver.maybeArchive(old);

                    // ensure we don't continue to use the old file; not strictly necessary, but cleaner to enforce it
                    old.discardUnusedTail();
                }

                return;
            }

            // no more segments, so register to receive a signal when not empty
            WaitQueue.Signal signal = hasAvailableSegments.register(CommitLog.instance.metrics.waitingOnSegmentAllocation.time());

            // trigger the management thread; this must occur after registering
            // the signal to ensure we are woken by any new segment creation
            manager.wakeManager();

            // check if the queue has already been added to before waiting on the signal, to catch modifications
            // that happened prior to registering the signal; *then* check to see if we've been beaten to making the change
            if (!availableSegments.isEmpty() || allocatingFrom != old)
            {
                signal.cancel();
                // if we've been beaten, just stop immediately
                if (allocatingFrom != old)
                    return;
                // otherwise try again, as there should be an available segment
                continue;
            }

            // can only reach here if the queue hasn't been inserted into
            // before we registered the signal, as we only remove items from the queue
            // after updating allocatingFrom. Can safely block until we are signalled
            // by the allocator that new segments have been published
            signal.awaitUninterruptibly();
        }
    }
    
    /**
     * Resets all the segments, for testing purposes. DO NOT USE THIS OUTSIDE OF TESTS.
     */
    public void resetUnsafe()
    {
        for (CommitLogSegment segment : activeSegments)
            segment.close();
        activeSegments.clear();

        for (CommitLogSegment segment : availableSegments)
            segment.close();
        availableSegments.clear();

        allocatingFrom = null;
    }

    public void makeInactive(CommitLogSegment segment)
    {
        activeSegments.remove(segment);
    }

    public boolean hasAvailableSegments()
    {
        return !availableSegments.isEmpty();
    }

    public void waitForSync(ReplayPosition position)
    {
        CommitLogSection waitSection = null;
        for (CommitLogSection section : syncQueue)
            if (waitSection == null || waitSection.id <= position.segment)
                waitSection = section;

        waitSection.waitForSync();
    }

    public void scheduleSync(CommitLogSection commitLogSection)
    {
        syncQueue.add(commitLogSection);
        syncScheduled.signalAll();
    }

    public void shutdown()
    {
        run = false;
        interrupt();
    }
}
