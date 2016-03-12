package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.RateLimiter;

public class LimitingRebufferer implements Rebufferer
{
    final private Rebufferer wrapped;
    final private RateLimiter limiter;
    final private int limitQuant;

    public LimitingRebufferer(Rebufferer wrapped, RateLimiter limiter, int limitQuant)
    {
        this.wrapped = wrapped;
        this.limiter = limiter;
        this.limitQuant = limitQuant;
    }

    @Override
    public BufferHolder rebuffer(long position)
    {
        BufferHolder bufferHolder = wrapped.rebuffer(position);
        ByteBuffer buffer = bufferHolder.buffer();
        int remaining = Ints.checkedCast(buffer.limit() + bufferHolder.offset() - position);
        if (remaining == 0)
            return bufferHolder;

        if (remaining > limitQuant)
        {
            buffer.limit(buffer.position() + limitQuant);
            remaining = limitQuant;
        }
        limiter.acquire(remaining);
        return bufferHolder;
    }

    @Override
    public void close()
    {
        wrapped.close();
    }

    @Override
    public ChannelProxy channel()
    {
        return wrapped.channel();
    }

    @Override
    public long fileLength()
    {
        return wrapped.fileLength();
    }

    @Override
    public double getCrcCheckChance()
    {
        return wrapped.getCrcCheckChance();
    }
}
