package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.*;
import com.google.common.primitives.Ints;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;

public class ReaderCache extends CacheLoader<ReaderCache.Key, ByteBuffer> implements RemovalListener<ReaderCache.Key, ByteBuffer>
{
    public static ReaderCache instance = DatabaseDescriptor.getFileCacheSizeInMB() > 0 ? new ReaderCache() : null;

    static ByteBuffer EMPTY = ByteBuffer.allocate(0);

    private final LoadingCache<Key, ByteBuffer> cache;
    private final int alignmentMask;
    private final int bufferSize;

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
                    && file.path() == other.file.path();
        }
    }

    public ReaderCache()
    {
        bufferSize = BufferPool.CHUNK_SIZE;
        assert Integer.bitCount(bufferSize) == 1;
        alignmentMask = -bufferSize;
        cache = CacheBuilder.newBuilder()
                .maximumSize(BufferPool.sizeInBytes() * 15 / (16 * bufferSize))
                .removalListener(this)
                .build(this);
    }

    @Override
    public ByteBuffer load(Key key) throws Exception
    {
        Rebufferer rebufferer = key.file.cacheRebufferer();
        synchronized (rebufferer)
        {
            ByteBuffer buffer = rebufferer.rebuffer(key.position, BufferPool.get(bufferSize));
            assert rebufferer.bufferOffset() == key.position;
//            assert buffer.remaining() == bufferSize;
            // FIXME: Stale data? When sstable limits move, should invalidate last chunk.
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

    class CachingRebufferer implements Rebufferer
    {
        private final SegmentedFile source;
        long bufferOffset = 0;

        public CachingRebufferer(SegmentedFile file)
        {
            source = file;
        }

        @Override
        public ByteBuffer rebuffer(long position, ByteBuffer buffer)
        {
            try
            {
                assert buffer == null;
                long pageAlignedPos = position & alignmentMask;
                bufferOffset = pageAlignedPos;
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
