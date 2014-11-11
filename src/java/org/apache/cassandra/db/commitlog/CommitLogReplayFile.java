package org.apache.cassandra.db.commitlog;

import java.io.File;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.PureJavaCrc32;

/**
 * Implements pre-3.0 commit logs where one a segment id is given per file.
 */
public class CommitLogReplayFile extends CommitLogReplayUnit
{
    private static final Logger logger = LoggerFactory.getLogger(CommitLogReplayFile.class);

    public CommitLogReplayFile(File file, CommitLogDescriptor descriptor)
    {
        super(file, descriptor);
        assert descriptor.version < CommitLogDescriptor.VERSION_30;
    }

    private int readSyncMarker(CommitLogDescriptor descriptor, int offset, RandomAccessReader reader) throws IOException
    {
        if (offset > reader.length() - getSyncMarkerSize(descriptor.version))
        {
            if (offset != reader.length() && offset != Integer.MAX_VALUE)
                logger.warn("Encountered bad header at position {} of Commit log {}; not enough room for a header", offset, reader.getPath());
            // cannot possibly be a header here. if we're == length(), assume it's a correctly written final segment
            return -1;
        }
        reader.seek(offset);
        PureJavaCrc32 crc = new PureJavaCrc32();
        crc.updateInt((int) (descriptor.id & 0xFFFFFFFFL));
        crc.updateInt((int) (descriptor.id >>> 32));
        crc.updateInt((int) reader.getPosition());
        int end = reader.readInt();
        long filecrc;
        if (descriptor.version < CommitLogDescriptor.VERSION_21)
            filecrc = reader.readLong();
        else
            filecrc = reader.readInt() & 0xffffffffL;
        if (crc.getValue() != filecrc)
        {
            if (end != 0 || filecrc != 0)
            {
                logger.warn("Encountered bad header at position {} of commit log {}, with invalid CRC. The end of segment marker should be zero.", offset, reader.getPath());
            }
            return -1;
        }
        else if (end < offset || end > reader.length())
        {
            logger.warn("Encountered bad header at position {} of commit log {}, with bad position but valid CRC", offset, reader.getPath());
            return -1;
        }
        return end;
    }

    private int getSyncMarkerSize(int version)
    {
        return 8;
    }

    private int getStartOffset(ReplayPosition globalPosition, long segmentId, int version)
    {
        if (globalPosition.segment < segmentId)
        {
            if (version >= CommitLogDescriptor.VERSION_21)
                return CommitLogDescriptor.HEADER_SIZE + getSyncMarkerSize(version);
            else
                return 0;
        }
        else
        {
            assert globalPosition.segment == segmentId;
            return globalPosition.position;
        }
    }

    @Override
    public boolean recover(CommitLogReplayer replayer) throws IOException
    {
        logger.info("Replaying {}", file.getPath());
        final long segmentId = descriptor.id;
        logger.info("Replaying {} (CL version {}, messaging version {})",
                    file.getPath(),
                    descriptor.version,
                    descriptor.getMessagingVersion());
        RandomAccessReader reader = RandomAccessReader.open(new File(file.getAbsolutePath()));

        try
        {
            assert reader.length() <= Integer.MAX_VALUE;
            int offset = getStartOffset(replayer.globalPosition, segmentId, descriptor.version);
            if (offset < 0)
            {
                logger.debug("skipping replay of fully-flushed {}", file);
                return true;
            }

            int prevEnd = CommitLogDescriptor.HEADER_SIZE;
            while (true)
            {

                int end = prevEnd;
                if (descriptor.version < CommitLogDescriptor.VERSION_21)
                    end = Integer.MAX_VALUE;
                else
                {
                    do { end = readSyncMarker(descriptor, end, reader); }
                    while (end < offset && end > prevEnd);
                }

                if (end < prevEnd)
                    break;

                if (!replayer.replaySection(reader, offset, end, descriptor))
                    break;
                
                if (descriptor.version < CommitLogDescriptor.VERSION_21)
                    break;

                offset = end + getSyncMarkerSize(descriptor.version);
                prevEnd = end;
            }
            return true;
        }
        finally
        {
            FileUtils.closeQuietly(reader);
            logger.info("Finished reading {}", file);
        }
    }

}
