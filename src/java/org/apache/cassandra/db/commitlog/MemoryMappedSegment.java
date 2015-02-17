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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CLibrary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Memory-mapped segment. Maps the destination channel into an appropriately-sized memory-mapped buffer in which the
 * mutation threads write. On sync forces the buffer to disk.
 * If possible, recycles used segment files to avoid reallocating large chunks of disk.
 */
public class MemoryMappedSegment extends CommitLogSegment
{
    private static final Logger logger = LoggerFactory.getLogger(MemoryMappedSegment.class);

    /**
     * Constructs a new segment file.
     *
     * @param filePath  if not null, recycles the existing file by renaming it and truncating it to CommitLog.SEGMENT_SIZE.
     * @param commitLog the commit log it will be used with.
     */
    MemoryMappedSegment(String filePath, CommitLog commitLog)
    {
        super(filePath, commitLog);
        // mark the initial sync marker as uninitialised
        int firstSync = buffer.position();
        buffer.putInt(firstSync + 0, 0);
        buffer.putInt(firstSync + 4, 0);
    }

    void recycleFile(String filePath)
    {
        File oldFile = new File(filePath);

        if (oldFile.exists())
        {
            logger.debug("Re-using discarded CommitLog segment for {} from {}", id, filePath);
            if (!oldFile.renameTo(logFile))
                throw new FSWriteError(new IOException("Rename from " + filePath + " to " + id + " failed"), filePath);
        } else {
            logger.debug("Creating new commit log segment {}", logFile.getPath());
        }
    }

    ByteBuffer createBuffer()
    {
        try
        {
            // Extend or truncate the file size to the standard segment size as we may have restarted after a segment
            // size configuration change, leaving "incorrectly" sized segments on disk.
            // NOTE: while we're using RAF to easily adjust file size, we need to avoid using RAF
            // for grabbing the FileChannel due to FILE_SHARE_DELETE flag bug on windows.
            // See: https://bugs.openjdk.java.net/browse/JDK-6357433 and CASSANDRA-8308
            if (logFile.length() != DatabaseDescriptor.getCommitLogSegmentSize())
            {
                try (RandomAccessFile raf = new RandomAccessFile(logFile, "rw"))
                {
                    raf.setLength(DatabaseDescriptor.getCommitLogSegmentSize());
                }
                catch (IOException e)
                {
                    throw new FSWriteError(e, logFile);
                }
            }

            return channel.map(FileChannel.MapMode.READ_WRITE, 0, DatabaseDescriptor.getCommitLogSegmentSize());
        }
        catch (IOException e)
        {
            throw new FSWriteError(e, logFile);
        }
    }

    @Override
    void write(int startMarker, int nextMarker)
    {
        // if there's room in the discard section to write an empty header,
        // zero out the next sync marker so replayer can cleanly exit
        if (nextMarker <= buffer.capacity() - SYNC_MARKER_SIZE)
        {
            buffer.putInt(nextMarker, 0);
            buffer.putInt(nextMarker + 4, 0);
        }

        // write previous sync marker to point to next sync marker
        // we don't chain the crcs here to ensure this method is idempotent if it fails
        writeSyncMarker(buffer, startMarker, startMarker, nextMarker);

        try {
            ((MappedByteBuffer) buffer).force();
        }
        catch (Exception e) // MappedByteBuffer.force() does not declare IOException but can actually throw it
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
    MemoryMappedSegment recycle(CommitLog commitLog)
    {
        try
        {
            syncAndClose();
        }
        catch (FSWriteError e)
        {
            logger.error("I/O error flushing {} {}", this, e.getMessage());
            throw e;
        }

        return new MemoryMappedSegment(getPath(), commitLog);
    }

    @Override
    protected void close()
    {
        if (FileUtils.isCleanerAvailable())
            FileUtils.clean(buffer);
        super.close();
    }
}
