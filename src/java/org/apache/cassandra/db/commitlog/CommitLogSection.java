package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;

public class CommitLogSection
{

    private final static long idBase;
    private final static AtomicInteger nextId = new AtomicInteger(1);
    static
    {
        long maxId = Long.MIN_VALUE;
        for (String location : DatabaseDescriptor.getCommitLogLocations())
            for (File file : new File(location).listFiles())
            {
                if (CommitLogDescriptor.isValid(file.getName()))
                    maxId = Math.max(CommitLogDescriptor.fromFileName(file.getName()).id, maxId);
            }
        idBase = Math.max(System.currentTimeMillis(), maxId + 1);
    }
    
    final long id;
    final CommitLogSegment segment;
    final CommitLogVolume volume;
    final int sectionStart;
    int sectionEnd;

    public CommitLogSection(CommitLogVolume volume, CommitLogSegmentManager mgr)
    {
        this.id = idBase + nextId.getAndIncrement();
        this.volume = volume;
        segment = volume.allocatingFrom(mgr);
        sectionStart = segment.startSection();
    }
    

    /**
     * Allocate space in this buffer for the provided mutation, and return the allocated Allocation object.
     * Returns null if there is not enough space in this segment, and a new segment is needed.
     */
    Allocation allocate(Mutation mutation, int size)
    {
        return segment.allocate(mutation, size, id);
    }


    public ReplayPosition getContext()
    {
        return new ReplayPosition(id, segment.getPosition());
    }


    public boolean finish()
    {
        sectionEnd = segment.finishSection(sectionStart);
        // Schedules a call to our sync when the volume thread is available.
        volume.scheduleSync(this);

        return segment.isStillAllocating();
    }


    public boolean isEmpty()
    {
        return segment.getPosition() == sectionStart;
    }

    public void sync()
    {
        segment.sync(id, sectionStart, sectionEnd);
    }


    public void waitForSync()
    {
        segment.waitForSync(sectionEnd);
    }


}
