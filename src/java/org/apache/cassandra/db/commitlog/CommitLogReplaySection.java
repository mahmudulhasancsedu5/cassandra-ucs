package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.PureJavaCrc32;

/**
 * Implements replay of post-3.0 commit logs where a file may contain multiple section ids.
 * The static extractSections method obtains sections from a file, which then must be sorted to be recovered
 * in increasing id order.
 */
public class CommitLogReplaySection extends CommitLogReplayUnit
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogReplaySection.class);
    
    private final int sectionStart;
    private final int sectionEnd;

    public CommitLogReplaySection(File file, CommitLogDescriptor desc, int sectionStart, int sectionEnd)
    {
        super(file, desc);
        this.sectionStart = sectionStart;
        this.sectionEnd = sectionEnd;
    }

    @Override
    public boolean recover(CommitLogReplayer replayer) throws IOException
    {
        try (RandomAccessReader reader = RandomAccessReader.open(new File(file.getAbsolutePath())))
        {
            return replayer.replaySection(reader, sectionStart, sectionEnd, descriptor);
        }
    }

    public static void extractSections(File file, CommitLogDescriptor desc, long startId, List<? super CommitLogReplayUnit> units)
            throws IOException
    {
        RandomAccessReader reader = RandomAccessReader.open(new File(file.getAbsolutePath()));
        PureJavaCrc32 crc = new PureJavaCrc32();
        int nextSection = CommitLogDescriptor.HEADER_SIZE;
        for (;;)
        {
            int sectionStart = nextSection + CommitLogSegment.SYNC_MARKER_SIZE;
            if (sectionStart > reader.length())
                return;
   
            reader.seek(nextSection);
            long id = reader.readLong();
            nextSection = reader.readInt();
            int expectedCrc = reader.readInt();

            if (id == 0)    // end of segment
                return;

            crc.reset();
            crc.updateInt((int) (id & 0xFFFFFFFFL));
            crc.updateInt((int) (id >>> 32));
            crc.updateInt(nextSection);
            if (crc.getCrc() != expectedCrc)
                logger.warn("Encountered bad header at position {} of commit log {}, with invalid CRC.", sectionStart, file.getPath());

            if (id >= startId)
            {
                CommitLogDescriptor unitDesc = desc.withId(id);
                units.add(new CommitLogReplaySection(file, unitDesc, sectionStart, nextSection));
            }
        }
    }
}
