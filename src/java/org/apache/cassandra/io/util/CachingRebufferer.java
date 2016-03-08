package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.*;
import com.google.common.primitives.Ints;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;

public class CachingRebufferer extends CacheLoader<Long, ByteBuffer> implements Rebufferer, RemovalListener<Long, ByteBuffer>
{
    static ByteBuffer EMPTY = ByteBuffer.allocate(0);

    protected final Rebufferer rebufferer;
    private final LoadingCache<Long, ByteBuffer> cache;
    private final int bufferSize;
    long bufferOffset = 0;

    public CachingRebufferer(Rebufferer wrapped, BufferType bufferType, int bufferSize, long maxMemory)
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
    public ByteBuffer rebuffer(long position, ByteBuffer buffer)
    {
        try
        {
            assert buffer == null;
            long pageAlignedPos = position & -bufferSize;
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

    @Override
    public void close()
    {
        cache.invalidateAll();
        rebufferer.close();
    }

    @Override
    public long bufferOffset()
    {
        return rebufferer.bufferOffset();
    }

    @Override
    public ChannelProxy channel()
    {
        return rebufferer.channel();
    }

    @Override
    public long fileLength()
    {
        return rebufferer.fileLength();
    }

    @Override
    public ByteBuffer initialBuffer()
    {
        return EMPTY;
    }
}
