package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;

public class CommitLogSection
{

    private final static long idBase;
    private final static AtomicInteger nextId = new AtomicInteger(1);
    static
    {
        long maxId = System.currentTimeMillis();

        List<CommitLogReplayUnit> units = new ArrayList<>();
        for (String location : DatabaseDescriptor.getCommitLogLocations())
            for (File file : new File(location).listFiles())
            {
                try
                {
                    CommitLogReplayUnit.extractUnits(file, maxId, units);
                }
                catch (IOException e)
                {
                    // TODO: Figure out what to do apart from ignoring the exception.
                }
                for (CommitLogReplayUnit unit : units)
                    maxId = Math.max(unit.id() + 1, maxId);
                units.clear();
            }
        idBase = maxId;
    }
    
    final long id;
    final CommitLogSegment segment;
    final CommitLogVolume volume;
    final int sectionStart;
    int sectionEnd;

    public CommitLogSection(CommitLogVolume volume, CommitLogSegment segment)
    {
        this.id = idBase + nextId.getAndIncrement();
        this.volume = volume;
        this.segment = segment;
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


    public void finish()
    {
        // called by one thread only
        sectionEnd = segment.finishSection(sectionStart);
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
