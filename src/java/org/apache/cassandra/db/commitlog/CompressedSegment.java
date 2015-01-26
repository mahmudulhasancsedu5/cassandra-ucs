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
import java.nio.channels.FileChannel;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.compress.ICompressor.WrappedArray;
import org.apache.cassandra.io.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * A single commit log file on disk. Manages creation of the file and writing mutations to disk,
 * as well as tracking the last mutation position of any "dirty" CFs covered by the segment file. Segment
 * files are initially allocated to a fixed size and can grow to accomidate a larger value if necessary.
 */
public class CompressedSegment extends CommitLogSegment
{
    private static final Logger logger = LoggerFactory.getLogger(CompressedSegment.class);

    static private final ThreadLocal<WrappedArray> compressedArrayHolder = new ThreadLocal<WrappedArray>() {
        protected WrappedArray initialValue()
        {
            return new WrappedArray(new byte[1024]);
        }
    };
    static private final ThreadLocal<ByteBuffer> compressedBufferHolder = new ThreadLocal<ByteBuffer>() {
        protected ByteBuffer initialValue()
        {
            return ByteBuffer.wrap(compressedArrayHolder.get().buffer);
        }
    };
    
    static Queue<ByteBuffer> bufferPool = new ConcurrentLinkedQueue<>();
    
    static final int COMPRESSED_MARKER_SIZE = SYNC_MARKER_SIZE + 4;

    /**
     * Constructs a new segment file.
     *
     * @param filePath  if not null, recycles the existing file by renaming it and truncating it to CommitLog.SEGMENT_SIZE.
     */
    CompressedSegment(String filePath)
    {
        super(filePath);
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
            buf = ByteBuffer.allocate(DatabaseDescriptor.getCommitLogSegmentSize());
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
        assert length > 0;

        try {

            int compressedLength = CommitLog.compressor.initialCompressedBufferLength(length);
            WrappedArray compressedArray = compressedArrayHolder.get();
            if (compressedArray.buffer.length < compressedLength + COMPRESSED_MARKER_SIZE)
            {
                compressedArray.buffer = new byte[compressedLength + COMPRESSED_MARKER_SIZE];
            }
            
            compressedLength = CommitLog.compressor.compress(buffer.array(), buffer.arrayOffset() + contentStart, length, compressedArray, COMPRESSED_MARKER_SIZE);

            ByteBuffer compressedBuffer = compressedBufferHolder.get();
            if (compressedBuffer.array() != compressedArray.buffer)
            {
                compressedBuffer = ByteBuffer.wrap(compressedArray.buffer);
                compressedBufferHolder.set(compressedBuffer);
            }
            compressedBuffer.position(0);
            compressedBuffer.limit(COMPRESSED_MARKER_SIZE + compressedLength);
            compressedBuffer.putInt(SYNC_MARKER_SIZE, length);

            // Only write after the previous write has completed.
            waitForSync(startMarker);

            // Only one thread can be here at a given time.
            writeSyncMarker(compressedBuffer, 0, (int) channel.position(), (int) channel.position() + compressedBuffer.remaining());
            channel.write(compressedBuffer);
            channel.force(true);
        }
        catch (Exception e)
        {
            throw new FSWriteError(e, getPath());
        }
    }

    /**
     * Recycle processes an unneeded segment file for reuse.
     *
     * @return a new CommitLogSegment representing the newly reusable segment.
     */
    CompressedSegment recycle()
    {
        // Run a sync to complete any outstanding writes.
        sync();

        close();
        return new CompressedSegment(getPath());
    }


    @Override
    void close()
    {
        super.close();
        synchronized (this)
        {
            if (buffer != null)
                bufferPool.add(buffer);
            buffer = null;
        }
    }

}
