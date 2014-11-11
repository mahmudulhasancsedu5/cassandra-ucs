package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.google.common.primitives.Longs;

public abstract class CommitLogReplayUnit implements Comparable<CommitLogReplayUnit>
{
    final File file;
    final CommitLogDescriptor descriptor;

    public CommitLogReplayUnit(File file, CommitLogDescriptor descriptor)
    {
        super();
        this.file = file;
        this.descriptor = descriptor;
    }
    
    public long id()
    {
        return descriptor.id;
    }

    @Override
    public int compareTo(CommitLogReplayUnit o)
    {
        return Longs.compare(id(), o.id());
    }
    
    abstract public boolean recover(CommitLogReplayer commitLogReplayer) throws IOException;

    public static void extractUnits(File file, long startId, List<? super CommitLogReplayUnit> units) throws IOException
    {
        CommitLogDescriptor desc = CommitLogDescriptor.fromFileName(file.getName());
        if (desc.version <= CommitLogDescriptor.VERSION_21)
        {
            if (desc.id >= startId)
                units.add(new CommitLogReplayFile(file, desc));
        } else {
            CommitLogReplaySection.extractSections(file, desc, startId, units);
        }
    }
}
