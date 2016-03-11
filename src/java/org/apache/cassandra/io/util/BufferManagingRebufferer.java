package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

import com.google.common.primitives.Ints;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;

public abstract class BufferManagingRebufferer implements Rebufferer
{
    protected final BufferlessRebufferer rebufferer;
    protected final ByteBuffer buffer;

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
        return buffer;
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
        public ByteBuffer rebuffer(long position)
        {
            rebufferer.rebuffer(position, buffer);
            return buffer;
        }

        @Override
        public long bufferOffset(long position)
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
        public ByteBuffer rebuffer(long position)
        {
            long pageAlignedPos = bufferOffset(position);
            rebufferer.rebuffer(pageAlignedPos, buffer);
            buffer.position(Ints.checkedCast(position - pageAlignedPos));
            return buffer;
        }

        @Override
        public long bufferOffset(long position)
        {
            return position & -buffer.capacity();
        }
    }
}
