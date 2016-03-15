package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Throwables;
import com.google.common.cache.*;
import com.google.common.collect.Iterables;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.utils.memory.BufferPool;

public class ReaderCache extends CacheLoader<ReaderCache.Key, ReaderCache.Buffer> implements RemovalListener<ReaderCache.Key, ReaderCache.Buffer>
{
    public static final int RESERVED_POOL_SPACE_IN_MB = 32;
    public static final long cacheSize = 1024L * 1024L * Math.max(0, DatabaseDescriptor.getFileCacheSizeInMB() - RESERVED_POOL_SPACE_IN_MB);

    public static final ReaderCache instance = cacheSize > 0 ? new ReaderCache() : null;

    private final LoadingCache<Key, Buffer> cache;

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

    static class Buffer implements Rebufferer.BufferHolder
    {
        private final ByteBuffer buffer;
        private final long offset;
        private final AtomicInteger references = new AtomicInteger(0);

        public Buffer(ByteBuffer buffer, long offset)
        {
            this.buffer = buffer;
            this.offset = offset;
        }

        Buffer reference()
        {
            references.incrementAndGet();
            return this;
        }

        @Override
        public ByteBuffer buffer()
        {
            assert references.get() > 0;
            return buffer.duplicate();
        }

        @Override
        public long offset()
        {
            return offset;
        }

        @Override
        public void release()
        {
            if (references.decrementAndGet() == 0)
                BufferPool.put(buffer);
        }
    }

    public ReaderCache()
    {
        cache = CacheBuilder.newBuilder()
                .maximumWeight(cacheSize)
                .weigher((key, buffer) -> ((Buffer) buffer).buffer.capacity())
                .removalListener(this)
                .build(this);
    }

    @Override
    public Buffer load(Key key) throws Exception
    {
        BufferlessRebufferer rebufferer = key.file.cacheRebufferer();
        synchronized (rebufferer)
        {
            ByteBuffer buffer = rebufferer.rebuffer(key.position, BufferPool.get(key.file.chunkSize()));
            assert buffer != null;
            return new Buffer(buffer, key.position).reference();
        }
    }

    @Override
    public void onRemoval(RemovalNotification<Key, Buffer> removal)
    {
        removal.getValue().release();
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

        public CachingRebufferer(SegmentedFile file)
        {
            source = file;
            int chunkSize = file.chunkSize();
            assert Integer.bitCount(chunkSize) == 1;    // Must be power of two
            alignmentMask = -chunkSize;
        }

        @Override
        public Buffer rebuffer(long position)
        {
            try
            {
                long pageAlignedPos = position & alignmentMask;
                return cache.get(new Key(source, pageAlignedPos)).reference();
            }
            catch (Throwable t)
            {
                Throwables.propagateIfInstanceOf(t.getCause(), CorruptSSTableException.class);
                throw Throwables.propagate(t);
            }
            // TODO: Deal with being evicted while still in use.
        }

        @Override
        public void close()
        {
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
        public double getCrcCheckChance()
        {
            return source.cacheRebufferer().getCrcCheckChance();
        }
    }
}
