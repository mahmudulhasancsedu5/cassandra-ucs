package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

import com.google.common.primitives.Ints;

class SimpleReadRebufferer extends AbstractRebufferer implements BufferlessRebufferer
{
    public SimpleReadRebufferer(ChannelProxy channel, long fileLength)
    {
        super(channel, fileLength);
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
}