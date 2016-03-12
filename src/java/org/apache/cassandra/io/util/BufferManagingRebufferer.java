package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;

public abstract class BufferManagingRebufferer implements Rebufferer, Rebufferer.BufferHolder
{
    protected final BufferlessRebufferer rebufferer;
    protected final ByteBuffer buffer;
    protected long offset = 0;

    abstract long alignedPosition(long position);

    public BufferManagingRebufferer(BufferlessRebufferer wrapped, BufferType bufferType, int bufferSize)
    {
        this.rebufferer = wrapped;
        buffer = RandomAccessReader.allocateBuffer(bufferSize, bufferType);
        buffer.limit(0);
    }

    @Override
    public void close()
    {
        BufferPool.put(buffer);
        rebufferer.close();
    }

    @Override
    public void release()
    {
        // nothing to do, we don't delete buffers before we're closed.
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
    public BufferHolder rebuffer(long position)
    {
        offset = alignedPosition(position);
        rebufferer.rebuffer(offset, buffer);
        return this;
    }

    public ByteBuffer buffer()
    {
        return buffer;
    }
    
    public long offset()
    {
        return offset;
    }

    @Override
    public double getCrcCheckChance()
    {
        return rebufferer.getCrcCheckChance();
    }

    public static class Unaligned extends BufferManagingRebufferer implements Rebufferer
    {
        public Unaligned(BufferlessRebufferer wrapped, BufferType bufferType, int bufferSize)
        {
            super(wrapped, bufferType, bufferSize);
        }

        @Override
        long alignedPosition(long position)
        {
            return position;
        }
    }

    public static class Aligned extends BufferManagingRebufferer implements Rebufferer
    {
        public Aligned(BufferlessRebufferer wrapped, BufferType bufferType, int bufferSize)
        {
            super(wrapped, bufferType, bufferSize);
            assert Integer.bitCount(bufferSize) == 1;
        }

        @Override
        long alignedPosition(long position)
        {
            return position & -buffer.capacity();
        }
    }
}
