package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

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
    public ByteBuffer rebuffer(long position)
    {
        ByteBuffer buffer = wrapped.rebuffer(position);
        int remaining = buffer.remaining();
        if (remaining == 0)
            return buffer;

        if (remaining > limitQuant)
        {
            buffer.limit(buffer.position() + limitQuant);
            remaining = limitQuant;
        }
        limiter.acquire(remaining);
        return buffer;
    }

    @Override
    public void close()
    {
        wrapped.close();
    }

    @Override
    public long bufferOffset(long position)
    {
        return wrapped.bufferOffset(position);
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
    public ByteBuffer initialBuffer()
    {
        ByteBuffer buffer = wrapped.initialBuffer();
        buffer.limit(0);    // To ensure we do acquire quant on first reads.
        return buffer;
    }

    @Override
    public double getCrcCheckChance()
    {
        return wrapped.getCrcCheckChance();
    }
}
