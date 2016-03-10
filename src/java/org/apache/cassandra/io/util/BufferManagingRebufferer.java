package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.memory.BufferPool;

public class BufferManagingRebufferer implements Rebufferer
{
    protected final Rebufferer rebufferer;
    protected final ByteBuffer ownedBuffer;

    public BufferManagingRebufferer(Rebufferer wrapped, BufferType bufferType, int bufferSize)
    {
        this.rebufferer = wrapped;
        ownedBuffer = RandomAccessReader.allocateBuffer(bufferSize, bufferType);
        ownedBuffer.limit(0);
    }

    @Override
    public ByteBuffer rebuffer(long position, ByteBuffer buffer)
    {
        assert buffer == null;
        buffer = rebufferer.rebuffer(position, ownedBuffer);
        assert ownedBuffer == buffer;
        return buffer;
    }

    @Override
    public void close()
    {
        BufferPool.put(ownedBuffer);
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
        return ownedBuffer;
    }

    @Override
    public double getCrcCheckChance()
    {
        return rebufferer.getCrcCheckChance();
    }
}
