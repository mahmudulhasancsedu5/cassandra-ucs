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
package org.apache.cassandra.io.util;

import com.google.common.util.concurrent.RateLimiter;

import org.apache.cassandra.cache.ChunkCache;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.Config.DiskAccessMode;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.CompressedRandomAccessReader;
import org.apache.cassandra.io.compress.CompressedSequentialWriter;
import org.apache.cassandra.io.compress.CompressionMetadata;
import org.apache.cassandra.utils.concurrent.Ref;

public class CompressedSegmentedFile extends SegmentedFile implements ICompressedFile
{
    public final CompressionMetadata metadata;

    public CompressedSegmentedFile(ChannelProxy channel, CompressionMetadata metadata, Config.DiskAccessMode mode)
    {
        this(channel,
             metadata,
             mode == DiskAccessMode.mmap
             ? MmappedRegions.map(channel, metadata)
             : null);
    }

    public CompressedSegmentedFile(ChannelProxy channel, CompressionMetadata metadata, MmappedRegions regions)
    {
        this(channel, metadata, regions, createRebufferer(channel, metadata, regions));
    }

    private static BaseRebufferer createRebufferer(ChannelProxy channel, CompressionMetadata metadata, MmappedRegions regions)
    {
        return ChunkCache.instance.wrap(CompressedRandomAccessReader.bufferlessRebufferer(channel, metadata, regions));
    }

    public CompressedSegmentedFile(ChannelProxy channel, CompressionMetadata metadata, MmappedRegions regions, BaseRebufferer rebufferer)
    {
        super(new Cleanup(channel, metadata, regions, rebufferer), channel, rebufferer, metadata.compressedFileLength);
        this.metadata = metadata;
    }

    private CompressedSegmentedFile(CompressedSegmentedFile copy)
    {
        super(copy);
        this.metadata = copy.metadata;
    }

    public ChannelProxy channel()
    {
        return channel;
    }

    private static final class Cleanup extends SegmentedFile.Cleanup
    {
        final CompressionMetadata metadata;

        protected Cleanup(ChannelProxy channel, CompressionMetadata metadata, MmappedRegions regions, BaseRebufferer rebufferer)
        {
            super(channel, rebufferer);
            this.metadata = metadata;
        }
        public void tidy()
        {
            ChunkCache.instance.invalidateFile(name());
            metadata.close();

            super.tidy();
        }
    }

    public CompressedSegmentedFile sharedCopy()
    {
        return new CompressedSegmentedFile(this);
    }

    public void addTo(Ref.IdentityCollection identities)
    {
        super.addTo(identities);
        metadata.addTo(identities);
    }

    public static class Builder extends SegmentedFile.Builder
    {
        final CompressedSequentialWriter writer;
        final Config.DiskAccessMode mode;

        public Builder(CompressedSequentialWriter writer)
        {
            this.writer = writer;
            this.mode = DatabaseDescriptor.getDiskAccessMode();
        }

        protected CompressionMetadata metadata(String path, long overrideLength)
        {
            if (writer == null)
                return CompressionMetadata.create(path);

            return writer.open(overrideLength);
        }

        public SegmentedFile complete(ChannelProxy channel, int bufferSize, long overrideLength)
        {
            return new CompressedSegmentedFile(channel, metadata(channel.filePath(), overrideLength), mode);
        }
    }

    public void dropPageCache(long before)
    {
        if (before >= metadata.dataLength)
            super.dropPageCache(0);
        super.dropPageCache(metadata.chunkFor(before).offset);
    }

    public RandomAccessReader createReader()
    {
        return new CompressedRandomAccessReader.Builder(this).build();
    }

    public RandomAccessReader createReader(RateLimiter limiter)
    {
        return new CompressedRandomAccessReader.Builder(this).limiter(limiter).build();
    }

    public CompressionMetadata getMetadata()
    {
        return metadata;
    }

    public long dataLength()
    {
        return metadata.dataLength;
    }
}
