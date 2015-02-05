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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CLibrary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Compressed commit log segment. Provides an in-memory buffer for the mutation threads. On sync compresses the written
 * section of the buffer and writes it to the destination channel.
 */
public class NonCompressedSegment extends CommitLogSegment
{
    private static final Logger logger = LoggerFactory.getLogger(NonCompressedSegment.class);

    static Queue<ByteBuffer> bufferPool = new ConcurrentLinkedQueue<>();

    /**
     * Constructs a new segment file.
     *
     * @param filePath  if not null, recycles the existing file by renaming it and truncating it to CommitLog.SEGMENT_SIZE.
     */
    NonCompressedSegment(String filePath, CommitLog commitLog)
    {
        super(filePath, commitLog);
        try
        {
            channel.write((ByteBuffer) buffer.duplicate().flip());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    void recycleFile(String filePath)
    {
        File oldFile = new File(filePath);

        if (oldFile.exists())
        {
            logger.debug("Deleting old CommitLog segment {}", filePath);
            FileUtils.deleteWithConfirm(oldFile);
        }
    }

    ByteBuffer createBuffer()
    {
        ByteBuffer buf = bufferPool.poll();
        if (buf == null)
            buf = ByteBuffer.allocateDirect(DatabaseDescriptor.getCommitLogSegmentSize());
        else
            buf.clear();
        return buf;
    }

    static long startMillis = System.currentTimeMillis();

    @Override
    void write(int startMarker, int nextMarker)
    {
        int contentStart = startMarker + SYNC_MARKER_SIZE;
        int length = nextMarker - contentStart;
        // The length may be 0 when the segment is being closed.
        assert length > 0 || length == 0 && !isStillAllocating();

        try {
            // Only write after the previous write has completed.
            //waitForSync(startMarker, null);

            // Only one thread can be here at a given time.
            writeSyncMarker(buffer, startMarker, startMarker, nextMarker);
            channel.write((ByteBuffer) buffer.duplicate().limit(nextMarker).position(startMarker));
            channel.force(true);
        }
        catch (Exception e)
        {
            throw new FSWriteError(e, getPath());
        }
        CLibrary.trySkipCache(fd, startMarker, nextMarker);
    }

    /**
     * Recycle processes an unneeded segment file for reuse.
     *
     * @return a new CommitLogSegment representing the newly reusable segment.
     */
    NonCompressedSegment recycle(CommitLog commitLog)
    {
        // Run a sync to complete any outstanding writes.
        syncAndClose();
        return new NonCompressedSegment(getPath(), commitLog);
    }

    @Override
    protected void close()
    {
        super.close();
        bufferPool.add(buffer);
    }

    static void shutdown()
    {
        bufferPool.clear();
    }

}
