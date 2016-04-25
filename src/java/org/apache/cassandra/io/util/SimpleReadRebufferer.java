package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

import com.google.common.primitives.Ints;

import org.apache.cassandra.io.compress.BufferType;

class SimpleReadRebufferer extends AbstractReaderFileProxy implements BufferlessRebufferer
{
    private final int bufferSize;
    private final BufferType bufferType;

    public SimpleReadRebufferer(ChannelProxy channel, long fileLength, BufferType bufferType, int bufferSize)
    {
        super(channel, fileLength);
        this.bufferSize = bufferSize;
        this.bufferType = bufferType;
    }

    @Override
    public void rebuffer(long position, ByteBuffer buffer)
    {
        buffer.clear();
        channel.read(buffer, position);
        buffer.flip();
    }

    @Override
    public int chunkSize()
    {
        return bufferSize;
    }

    @Override
    public BufferType preferredBufferType()
    {
        return bufferType;
    }

    @Override
    public boolean alignmentRequired()
    {
        return false;
    }

    @Override
    public Rebufferer instantiateRebufferer()
    {
        return BufferManagingRebufferer.on(this);
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s - chunk length %d, data length %d)",
                             getClass().getSimpleName(),
                             channel.filePath(),
                             bufferSize,
                             fileLength());
    }
}