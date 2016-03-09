package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.*;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.memory.BufferPool;

public class ReaderCache extends CacheLoader<ReaderCache.Key, ByteBuffer> implements RemovalListener<ReaderCache.Key, ByteBuffer>
{
    public static ReaderCache instance = DatabaseDescriptor.getFileCacheSizeInMB() > 0 ? new ReaderCache() : null;

    static ByteBuffer EMPTY = ByteBuffer.allocate(0);

    private final LoadingCache<Key, ByteBuffer> cache;

    static class Key
    {
        final SegmentedFile file;
        final long position;

        public Key(SegmentedFile file, long position)
        {
            super();
            this.file = file;
            this.position = position;
        }

        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + file.path().hashCode();
            result = prime * result + Long.hashCode(position);
            return result;
        }

        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;

            Key other = (Key) obj;
            return (position == other.position)
                    && file.path().equals(other.file.path());
        }
    }

    public ReaderCache()
    {
        cache = CacheBuilder.newBuilder()
                .maximumWeight(BufferPool.MEMORY_USAGE_THRESHOLD * 15 / 16)
                .weigher((key, buffer) -> ((ByteBuffer) buffer).capacity())
                .removalListener(this)
                .build(this);
    }

    @Override
    public ByteBuffer load(Key key) throws Exception
    {
        Rebufferer rebufferer = key.file.cacheRebufferer();
        synchronized (rebufferer)
        {
            ByteBuffer buffer = rebufferer.rebuffer(key.position, BufferPool.get(key.file.chunkSize()));
            assert buffer != null;
            assert rebufferer.bufferOffset() == key.position;
            return buffer;
        }
    }

    @Override
    public void onRemoval(RemovalNotification<Key, ByteBuffer> removal)
    {
        BufferPool.put(removal.getValue());
    }

    public void close()
    {
        cache.invalidateAll();
    }

    public Rebufferer newRebufferer(SegmentedFile file)
    {
        return new CachingRebufferer(file);
    }

    public void invalidatePosition(SegmentedFile file, long position)
    {
        long pageAlignedPos = position & -file.chunkSize();
        cache.invalidate(new Key(file, pageAlignedPos));
    }
    
    public void invalidateFile(String fileName)
    {
        cache.invalidateAll(Iterables.filter(cache.asMap().keySet(), x -> x.file.path().equals(fileName)));
    }

    // TODO: Invalidate caches for obsoleted/MOVED_START tables?

    class CachingRebufferer implements Rebufferer
    {
        private final SegmentedFile source;
        final long alignmentMask;
        long bufferOffset = 0;

        public CachingRebufferer(SegmentedFile file)
        {
            source = file;
            int chunkSize = file.chunkSize();
            assert Integer.bitCount(chunkSize) == 1;    // Must be power of two
            alignmentMask = -chunkSize;
        }

        @Override
        public ByteBuffer rebuffer(long position, ByteBuffer buffer)
        {
            try
            {
                assert buffer == null;
                long pageAlignedPos = position & alignmentMask;
                buffer = cache.get(new Key(source, pageAlignedPos)).duplicate();
                bufferOffset = pageAlignedPos;
                buffer.position(Ints.checkedCast(position - bufferOffset));
                return buffer;
            }
            catch (ExecutionException e)
            {
                throw new AssertionError();
            }
        }

        @Override
        public void close()
        {
        }

        @Override
        public long bufferOffset()
        {
            return bufferOffset;
        }

        @Override
        public ChannelProxy channel()
        {
            return source.channel;
        }

        @Override
        public long fileLength()
        {
            return source.dataLength();
        }

        @Override
        public ByteBuffer initialBuffer()
        {
            return EMPTY;
        }
    }
}
