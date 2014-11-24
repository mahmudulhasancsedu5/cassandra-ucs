package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.cassandra.utils.concurrent.WaitQueue;

import com.google.common.collect.Iterables;

/**
 * This class manages a volume of commit logs. Each volume writes to a separate file location (which may be a separate
 * drive, but this is not required) and uses a separate sync thread.
 * 
 * The volume provides CL sections, which are the unit of writing mutations to disk, and maintains a set of available
 * and active CL segments. The segment manager uses the sections to write and switches volumes when a CL sync is
 * requested to spread the writing load between volumes.
 */
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
                section.write();
                syncQueue.take();
            }
            catch (InterruptedException e)
            {
                // Continue to shut down cleanly.
            }
            catch (Throwable t)
            {
                if (!CommitLog.handleCommitError("Failed to persist commits to disk", t))
                    break;

                // sleep for a second after an error, so we don't spam the log file
                try
                {
                    sleep(1000);
                }
                catch (InterruptedException e)
                {
                    // Continue to shut down cleanly.
                }
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

    /**
     * Starts a new section in the currently open segment.
     */
    CommitLogSection startSection()
    {
        return new CommitLogSection(this, allocatingFrom);
    }

    /**
     * Complete the currently used section and be ready to start a new one and returns true if the volume has a segment
     * ready for allocation and false if writing to this volume again requires a new segment to be made available.
     *
     * Unsafe to call from multiple threads or concurrently with allocation or forceSegmentChange. Caller must ensure
     * sections are completed serially and in order.
     */
    boolean advanceSection(CommitLogSection fromSection)
    {
        // Called by one thread only (Guarded by synchronization in CLSM.advanceVolume()).
        CommitLogSegment segment = fromSection.segment;
        assert segment == allocatingFrom && segment != null;
        fromSection.finish();
        scheduleSync(fromSection);

        if (!segment.isStillAllocating())
            return advanceSegment(segment);
        return true;
    }

    /**
     * Forces close of the currently used segment. Returns true if the volume has a segment ready for allocation and
     * false if writing to this volume again requires a new segment to be made available.
     *
     * Caller must ensure no section is open, and that this method is not called concurrently with allocation or
     * advanceSection.
     */
    boolean forceSegmentChange()
    {
        // Can't be called concurrently with advanceSection (Guarded by CLSM synchronization).
        assert allocatingFrom != null;
        CommitLogSegment segment = allocatingFrom;
        segment.markComplete();
        return advanceSegment(segment);
    }

    private boolean advanceSegment(CommitLogSegment segment)
    {
        // Now we can run the user defined command just after switching to the new commit log.
        // (Do this here instead of in the recycle call so we can get a head start on the archive.)
        CommitLog.instance.archiver.maybeArchive(segment);

        // If not synchronized makeAvailable can miss the transition to null allocatingFrom.
        synchronized (this)
        {
            segment = availableSegments.poll();
            if (segment == null)
            {
                allocatingFrom = null;
                return false;
            }
            else
            {
                activeSegments.add(segment);
                allocatingFrom = segment;
                return true;
            }
        }
    }

    /**
     * Makes a newly created or recycled segment available for use.
     * Returns true if the volume was previously unavailable for use because no segment was available.
     *
     * Safe to use concurrently with all other methods.
     */
    boolean makeAvailable(CommitLogSegment segment)
    {
        synchronized (this)
        {
            if (allocatingFrom != null)
            {
                availableSegments.add(segment);
                return false;
            }
            else
            {
                activeSegments.add(segment);
                allocatingFrom = segment;
                return true;
            }
        }
    }

    /**
     * Resets all the segments, for testing purposes. DO NOT USE THIS OUTSIDE OF TESTS.
     */
    public synchronized void resetUnsafe()
    {
        while (!syncQueue.isEmpty())
            Thread.yield();
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
