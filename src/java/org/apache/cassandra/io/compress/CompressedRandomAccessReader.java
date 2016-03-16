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
package org.apache.cassandra.io.compress;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.*;

/**
 * CRAR extends RAR to transparently uncompress blocks from the file into RAR.buffer.  Most of the RAR
 * "read bytes from the buffer, rebuffering when necessary" machinery works unchanged after that.
 */
public class CompressedRandomAccessReader
{
    @VisibleForTesting
    public abstract static class CompressedRebufferer extends AbstractRebufferer implements BufferlessRebufferer
    {
        final CompressionMetadata metadata;

        public CompressedRebufferer(ChannelProxy channel, CompressionMetadata metadata)
        {
            super(channel, metadata.dataLength);
            this.metadata = metadata;
        }

        @VisibleForTesting
        public double getCrcCheckChance()
        {
            return metadata.parameters.getCrcCheckChance();
        }

        @Override
        public String toString()
        {
            return String.format("%s(%s - chunk length %d, data length %d)",
                                 getClass().getSimpleName(),
                                 channel.filePath(),
                                 metadata.chunkLength(),
                                 metadata.dataLength);
        }

        @Override
        public int chunkSize()
        {
            return metadata.chunkLength();
        }

        @Override
        public boolean alignmentRequired()
        {
            return true;
        }

        @Override
        public BufferType preferredBufferType()
        {
            return metadata.compressor().preferredBufferType();
        }
    }
    
    static class StandardRebufferer extends CompressedRebufferer
    {
        // we read the raw compressed bytes into this buffer, then move the uncompressed ones into super.buffer.
        private final ThreadLocal<ByteBuffer> compressedHolder;

        public StandardRebufferer(ChannelProxy channel, CompressionMetadata metadata)
        {
            super(channel, metadata);
            compressedHolder = ThreadLocal.withInitial(this::allocateBuffer);
        }

        public ByteBuffer allocateBuffer()
        {
            return allocateBuffer(metadata.compressor().initialCompressedBufferLength(metadata.chunkLength()));
        }

        public ByteBuffer allocateBuffer(int size)
        {
            return metadata.compressor().preferredBufferType().allocate(size);
        }

        @Override
        public ByteBuffer rebuffer(long position, ByteBuffer uncompressed)
        {
            try
            {
                // accesses must always be aligned
                assert (position & -uncompressed.capacity()) == position;
                assert position <= fileLength;

                CompressionMetadata.Chunk chunk = metadata.chunkFor(position);
                ByteBuffer compressed = compressedHolder.get();

                if (compressed.capacity() < chunk.length)
                {
                    compressed = allocateBuffer(chunk.length);
                    compressedHolder.set(compressed);
                }
                else
                {
                    compressed.clear();
                }

                compressed.limit(chunk.length);
                if (channel.read(compressed, chunk.offset) != chunk.length)
                    throw new CorruptBlockException(channel.filePath(), chunk);

                compressed.flip();
                uncompressed.clear();

                try
                {
                    metadata.compressor().uncompress(compressed, uncompressed);
                }
                catch (IOException e)
                {
                    throw new CorruptBlockException(channel.filePath(), chunk);
                }
                finally
                {
                    uncompressed.flip();
                }

                if (getCrcCheckChance() > ThreadLocalRandom.current().nextDouble())
                {
                    compressed.rewind();
                    int checksum = (int) metadata.checksumType.of(compressed);

                    compressed.clear().limit(Integer.BYTES);
                    if (channel.read(compressed, chunk.offset + chunk.length) != Integer.BYTES
                        || compressed.getInt(0) != checksum)
                        throw new CorruptBlockException(channel.filePath(), chunk);
                }
                return uncompressed;
            }
            catch (CorruptBlockException e)
            {
                throw new CorruptSSTableException(e, channel.filePath());
            }
        }
    }

    static class MmapRebufferer extends CompressedRebufferer
    {
        protected final MmappedRegions regions;

        public MmapRebufferer(ChannelProxy channel, CompressionMetadata metadata, MmappedRegions regions)
        {
            super(channel, metadata);
            this.regions = regions;
        }

        @Override
        public ByteBuffer rebuffer(long position, ByteBuffer uncompressed)
        {
            try
            {
                // accesses must always be aligned
                assert (position & -uncompressed.capacity()) == position;
                assert position <= fileLength;

                CompressionMetadata.Chunk chunk = metadata.chunkFor(position);

                MmappedRegions.Region region = regions.floor(chunk.offset);
                long segmentOffset = region.offset();
                int chunkOffset = Ints.checkedCast(chunk.offset - segmentOffset);
                ByteBuffer compressedChunk = region.buffer();

                compressedChunk.position(chunkOffset).limit(chunkOffset + chunk.length);

                uncompressed.clear();

                try
                {
                    metadata.compressor().uncompress(compressedChunk, uncompressed);
                }
                catch (IOException e)
                {
                    throw new CorruptBlockException(channel.filePath(), chunk);
                }
                finally
                {
                    uncompressed.flip();
                }

                if (getCrcCheckChance() > ThreadLocalRandom.current().nextDouble())
                {
                    compressedChunk.position(chunkOffset).limit(chunkOffset + chunk.length);

                    int checksum = (int) metadata.checksumType.of(compressedChunk);

                    compressedChunk.limit(compressedChunk.capacity());
                    if (compressedChunk.getInt() != checksum)
                        throw new CorruptBlockException(channel.filePath(), chunk);
                }
                return uncompressed;
            }
            catch (CorruptBlockException e)
            {
                throw new CorruptSSTableException(e, channel.filePath());
            }

        }

        public void close()
        {
            regions.closeQuietly();
            super.close();
        }
    }

    public static BufferlessRebufferer bufferlessRebufferer(ChannelProxy channel, CompressionMetadata metadata, MmappedRegions regions)
    {
        return regions != null
               ? new MmapRebufferer(channel, metadata, regions)
               : new StandardRebufferer(channel, metadata);
    }

    public final static class Builder extends RandomAccessReader.Builder
    {
        private final CompressionMetadata metadata;

        public Builder(CompressedSegmentedFile file)
        {
            super(file);
            metadata = file.getMetadata();
            assert Integer.bitCount(metadata.chunkLength()) == 1; //must be a power of two
        }

        public Builder(ChannelProxy channel, CompressionMetadata metadata)
        {
            super(channel);
            this.metadata = metadata;
            assert Integer.bitCount(metadata.chunkLength()) == 1; //must be a power of two
        }

        @Override
        public Builder regions(MmappedRegions regions)
        {
            assert fileRebufferer == null;
            fileRebufferer = CompressedRandomAccessReader.bufferlessRebufferer(channel, metadata, regions);
            return this;
        }

        public BufferlessRebufferer bufferlessRebufferer()
        {
            return CompressedRandomAccessReader.bufferlessRebufferer(channel, metadata, null);
        }
    }
}
