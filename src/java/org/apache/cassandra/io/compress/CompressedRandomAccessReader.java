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
import java.util.zip.Checksum;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import org.apache.cassandra.io.FSReadError;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.utils.memory.BufferPool;

/**
 * CRAR extends RAR to transparently uncompress blocks from the file into RAR.buffer.  Most of the RAR
 * "read bytes from the buffer, rebuffering when necessary" machinery works unchanged after that.
 */
public class CompressedRandomAccessReader
{
    @VisibleForTesting
    public abstract static class CompressedRebufferer extends AbstractRebufferer
    {
        final CompressionMetadata metadata;

        final ByteBuffer uncompressed;
        final BufferType bufferType;
        final int bufferSize;

        // re-use single crc object
        final Checksum checksum;

        public CompressedRebufferer(ChannelProxy channel, CompressionMetadata metadata)
        {
            super(channel, metadata.dataLength);
            this.metadata = metadata;
            checksum = metadata.checksumType.newInstance();
            bufferType = metadata.compressor().preferredBufferType();
            bufferSize = metadata.chunkLength();
            uncompressed = RandomAccessReader.allocateBuffer(bufferSize, bufferType);
            uncompressed.limit(0);
        }

        @Override
        public void close()
        {
            BufferPool.put(uncompressed);
        }

        @Override
        public ByteBuffer initialBuffer()
        {
            return uncompressed;
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
    }

    static class StandardRebufferer extends CompressedRebufferer
    {
        // we read the raw compressed bytes into this buffer, then move the uncompressed ones into super.buffer.
        private ByteBuffer compressed;

        // raw checksum bytes
        final ByteBuffer checksumBytes;

        public StandardRebufferer(ChannelProxy channel, CompressionMetadata metadata)
        {
            super(channel, metadata);
            compressed = RandomAccessReader.allocateBuffer(
                     metadata.compressor().initialCompressedBufferLength(bufferSize),
                     bufferType);
            checksumBytes = ByteBuffer.wrap(new byte[4]);
        }

        @Override
        public ByteBuffer rebuffer(long position)
        {
            try
            {
                assert position < fileLength;

                CompressionMetadata.Chunk chunk = metadata.chunkFor(position);

                if (compressed.capacity() < chunk.length)
                {
                    BufferPool.put(compressed);
                    compressed = RandomAccessReader.allocateBuffer(chunk.length, bufferType);
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
                    metadata.checksumType.update( checksum, compressed);

                    if (checksum(chunk) != (int) checksum.getValue())
                        throw new CorruptBlockException(channel.filePath(), chunk);

                    // reset checksum object back to the original (blank) state
                    checksum.reset();
                }

                // buffer offset is always aligned
                bufferOffset = position & ~(uncompressed.capacity() - 1);
                uncompressed.position((int) (position - bufferOffset));
                // the length() can be provided at construction time, to override the true (uncompressed) length of the file;
                // this is permitted to occur within a compressed segment, so we truncate validBufferBytes if we cross the imposed length
                assert bufferOffset + uncompressed.limit() <= fileLength;
//                if (bufferOffset + buffer.limit() > fileLength)
//                    buffer.limit((int)(fileLength - bufferOffset));
                return uncompressed;
            }
            catch (CorruptBlockException e)
            {
                throw new CorruptSSTableException(e, channel.filePath());
            }
            catch (IOException e)
            {
                throw new FSReadError(e, channel.filePath());
            }
        }

        private int checksum(CompressionMetadata.Chunk chunk) throws IOException
        {
            long position = chunk.offset + chunk.length;
            checksumBytes.clear();
            if (channel.read(checksumBytes, position) != checksumBytes.capacity())
                throw new CorruptBlockException(channel.filePath(), chunk);
            return checksumBytes.getInt(0);
        }

        @Override
        public void close()
        {
            super.close();
            BufferPool.put(compressed);
        }
    }

    static class MemmapRebufferer extends CompressedRebufferer
    {
        protected final MmappedRegions regions;

        public MemmapRebufferer(ChannelProxy channel, CompressionMetadata metadata, MmappedRegions regions)
        {
            super(channel, metadata);
            this.regions = regions;
        }

        @Override
        public ByteBuffer rebuffer(long position)
        {
            try
            {
                assert position <= fileLength;

                CompressionMetadata.Chunk chunk = metadata.chunkFor(position);

                MmappedRegions.Region region = regions.floor(chunk.offset);
                long segmentOffset = region.bottom();
                int chunkOffset = Ints.checkedCast(chunk.offset - segmentOffset);
                ByteBuffer compressedChunk = region.buffer.duplicate();

                // FIXME: what if it's crossing a boundary?
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

                    metadata.checksumType.update( checksum, compressedChunk);

                    compressedChunk.limit(compressedChunk.capacity());
                    if (compressedChunk.getInt() != (int) checksum.getValue())
                        throw new CorruptBlockException(channel.filePath(), chunk);

                    // reset checksum object back to the original (blank) state
                    checksum.reset();
                }

                // buffer offset is always aligned
                bufferOffset = position & ~(uncompressed.capacity() - 1);
                uncompressed.position((int) (position - bufferOffset));
                // the length() can be provided at construction time, to override the true (uncompressed) length of the file;
                // this is permitted to occur within a compressed segment, so we truncate validBufferBytes if we cross the imposed length
                assert bufferOffset + uncompressed.limit() <= fileLength;
//              if (bufferOffset + buffer.limit() > fileLength)
//                  buffer.limit((int)(fileLength - bufferOffset));
                return uncompressed;
            }
            catch (CorruptBlockException e)
            {
                throw new CorruptSSTableException(e, channel.filePath());
            }

        }
    }

    public final static class Builder extends RandomAccessReader.Builder
    {
        private final CompressionMetadata metadata;

        public Builder(ICompressedFile file)
        {
            super(file.channel());
            metadata = file.getMetadata();
            regions = file.regions();
            assert Integer.bitCount(metadata.chunkLength()) == 1; //must be a power of two
        }

        public Builder(ChannelProxy channel, CompressionMetadata metadata)
        {
            super(channel);
            this.metadata = metadata;
            assert Integer.bitCount(metadata.chunkLength()) == 1; //must be a power of two
        }

        @Override
        protected Rebufferer createRebufferer()
        {
            return regions != null
                    ? new MemmapRebufferer(channel, metadata, regions)
                    : new StandardRebufferer(channel, metadata);
        }
    }
}
