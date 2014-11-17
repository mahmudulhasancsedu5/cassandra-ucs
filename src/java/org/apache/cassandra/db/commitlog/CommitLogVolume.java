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
    }

    public CommitLogSection startSection()
    {
        assert allocatingFrom != null;
        return new CommitLogSection(this, allocatingFrom);
    }

    boolean advanceSection(CommitLogSection old)
    {
        // Called by one thread only (Guarded by synchronization in CLSM.advanceVolume()).
        CommitLogSegment segment = old.segment;
        old.finish();
        scheduleSync(old);

        if (!segment.isStillAllocating())
        {
            // Now we can run the user defined command just after switching to the new commit log.
            // (Do this here instead of in the recycle call so we can get a head start on the archive.)
            CommitLog.instance.archiver.maybeArchive(segment);

            synchronized (this) // race with CLSM thread doing checkNewlyAvailable
            {
                segment = availableSegments.poll();
                if (segment == null)
                {
                    allocatingFrom = null;
                    return false;
                }
                activeSegments.add(segment);
                allocatingFrom = segment;
            }
        }
        return true;
    }

    boolean checkNewlyAvailable()
    {
        // Called by CSLM thread only
        if (allocatingFrom != null || availableSegments.isEmpty())
            return false;

        synchronized (this) // race with writing thread doing advanceSection
        {
            CommitLogSegment segment = availableSegments.remove();
            activeSegments.add(segment);
            allocatingFrom = segment;
            return true;
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

        if (waitSection != null)
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
