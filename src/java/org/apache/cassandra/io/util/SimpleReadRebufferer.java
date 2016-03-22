package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

import com.google.common.primitives.Ints;

import org.apache.cassandra.io.compress.BufferType;

class SimpleReadRebufferer extends AbstractRebufferer implements BufferlessRebufferer
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
    public ByteBuffer rebuffer(long position, ByteBuffer buffer)
    {
        assert position <= fileLength;

        buffer.clear();
        buffer.limit(Ints.checkedCast(Math.min(fileLength - position, buffer.capacity())));
        channel.read(buffer, position);
        buffer.flip();
        return buffer;
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
    public String toString()
    {
        return String.format("%s(%s - chunk length %d, data length %d)",
                             getClass().getSimpleName(),
                             channel.filePath(),
                             bufferSize,
                             fileLength());
    }
}