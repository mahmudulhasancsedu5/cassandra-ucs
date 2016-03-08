package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.*;
import com.google.common.primitives.Ints;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;

public class ReaderCache extends CacheLoader<Long, ByteBuffer> implements RemovalListener<Long, ByteBuffer>
{
    static ByteBuffer EMPTY = ByteBuffer.allocate(0);

    private final Rebufferer rebufferer;
    private final LoadingCache<Long, ByteBuffer> cache;
    private final int bufferSize;

    public ReaderCache(Rebufferer wrapped, int bufferSize, long maxMemory)
    {
        this.rebufferer = wrapped;
        this.bufferSize = bufferSize;
        assert Integer.bitCount(bufferSize) == 1;
        cache = CacheBuilder.newBuilder()
                .maximumSize(maxMemory / bufferSize)
                .removalListener(this)
                .build(this);
    }

    @Override
    public ByteBuffer load(Long position) throws Exception
    {
        ByteBuffer result = rebufferer.rebuffer(position, BufferPool.get(bufferSize));
        assert rebufferer.bufferOffset() == position;
        return result;
    }

    @Override
    public void onRemoval(RemovalNotification<Long, ByteBuffer> removal)
    {
        BufferPool.put(removal.getValue());
    }

    public void close()
    {
        cache.invalidateAll();
        rebufferer.close();
    }
    
    public Rebufferer newRebufferer()
    {
        return new CachingRebufferer(this);
    }

    static class CachingRebufferer implements Rebufferer
    {
        private final Rebufferer source;
        private final LoadingCache<Long, ByteBuffer> cache;
        private final int alignmentMask;
        long bufferOffset = 0;

        public CachingRebufferer(ReaderCache readerCache)
        {
            cache = readerCache.cache;
            source = readerCache.rebufferer;
            alignmentMask = -readerCache.bufferSize;
        }

        @Override
        public ByteBuffer rebuffer(long position, ByteBuffer buffer)
        {
            try
            {
                assert buffer == null;
                long pageAlignedPos = position & alignmentMask;
                bufferOffset = pageAlignedPos;
                buffer = cache.get(pageAlignedPos).duplicate();
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
            return source.channel();
        }

        @Override
        public long fileLength()
        {
            return source.fileLength();
        }

        @Override
        public ByteBuffer initialBuffer()
        {
            return EMPTY;
        }
    }
}
