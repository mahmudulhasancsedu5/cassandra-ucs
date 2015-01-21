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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.cassandra.concurrent.NamedThreadFactory;
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

    private final FileChannel channel;

    static Queue<ByteBuffer> bufferPool = new ConcurrentLinkedQueue<>();
    
    static final int COMPRESSED_MARKER_SIZE = SYNC_MARKER_SIZE + 4;

    static class CompressionTask implements Callable<CompressionTask>
    {
        ByteBuffer compressedBuffer;
        WrappedArray compressedArray;
        ByteBuffer sourceBuffer;
        int startMarker;
        int nextMarker;
        long syncTimestamp;
        boolean close;

        @Override
        public CompressionTask call() throws Exception
        {
            int contentStart = startMarker + SYNC_MARKER_SIZE;
            int length = nextMarker - contentStart;

            int compressedLength = CommitLog.compressor.initialCompressedBufferLength(length) + COMPRESSED_MARKER_SIZE;
            if (compressedArray == null || compressedArray.buffer.length < compressedLength)
                compressedArray = new WrappedArray(new byte[compressedLength]);
            
            compressedLength = CommitLog.compressor.compress(sourceBuffer.array(), sourceBuffer.arrayOffset() + contentStart, length, compressedArray, COMPRESSED_MARKER_SIZE);

            if (compressedBuffer == null || compressedBuffer.array() != compressedArray.buffer)
                compressedBuffer = ByteBuffer.wrap(compressedArray.buffer);
            compressedBuffer.position(0);
            compressedBuffer.limit(COMPRESSED_MARKER_SIZE + compressedLength);
            return this;
        }
    }

    private LinkedBlockingQueue<Future<CompressionTask>> compressions = new LinkedBlockingQueue<>();
    static private ExecutorService executor = Executors.newCachedThreadPool(new NamedThreadFactory("commit-log-compression"));
    long lastSyncedTimestamp = Long.MIN_VALUE;

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
            channel = logFileAccessor.getChannel();
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

    static Queue<CompressionTask> tasksPool = new ConcurrentLinkedQueue<>();
    
    public CompressionTask newCompressionTask(int startMarker, int nextMarker, long syncTimestamp, boolean close)
    {
        CompressionTask ct = tasksPool.poll();
        if (ct == null)
            ct = new CompressionTask();
        ct.sourceBuffer = buffer;
        ct.startMarker = startMarker;
        ct.nextMarker = nextMarker;
        ct.syncTimestamp = syncTimestamp;
        ct.close = close;
        return ct;
    }

    public void releaseCompressionTask(CompressionTask task)
    {
        tasksPool.add(task);
    }

    @Override
    void write(int startMarker, int nextMarker, long syncTimestamp, boolean close)
    {
        int contentStart = startMarker + SYNC_MARKER_SIZE;
        int length = nextMarker - contentStart;
        if (length == 0)
            // No content to write, but we can still advance in the input buffer.
            return;

        compressions.add(executor.submit(newCompressionTask(startMarker, nextMarker, syncTimestamp, close)));
    }

    @Override
    long retireInFlightWrites(long syncTimestamp)
    {
        // Guarded by sync lock.
        if (compressions.isEmpty())
            return lastSyncedTimestamp = syncTimestamp;
        try
        {
            int lastPosition = -1;
            boolean close = false;
            for (Future<CompressionTask> task = compressions.peek(); task != null; task = compressions.peek())
            {
                if (!task.isDone())
                    break;
                compressions.remove();
                CompressionTask t = task.get();

                ByteBuffer compressedBuffer = t.compressedBuffer;
                writeSyncMarker(compressedBuffer, 0, (int) channel.position(), (int) channel.position() + compressedBuffer.remaining());
                compressedBuffer.putInt(SYNC_MARKER_SIZE, t.nextMarker - (t.startMarker + SYNC_MARKER_SIZE));
                channel.write(compressedBuffer);

                lastSyncedTimestamp = t.syncTimestamp;
                lastPosition = t.nextMarker;
                close |= t.close;
                releaseCompressionTask(t);
            }

            if (lastPosition >= 0)
            {
                channel.force(true);
                retireWrite(lastPosition, close);
            }
        }
        catch (Exception e)
        {
            throw new FSWriteError(e, getPath());
        }
        return lastSyncedTimestamp;
    }

    /**
     * Recycle processes an unneeded segment file for reuse.
     * Synchronized to stop new syncs being initiated.
     *
     * @return a new CommitLogSegment representing the newly reusable segment.
     */
    synchronized CompressedSegment recycle()
    {
        // The file will be immediately deleted, don't try to write any data.

        // Cancel outstanding writes.
        for (Future<CompressionTask> task : compressions)
        {
            task.cancel(true);
        }
        compressions.clear();
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
