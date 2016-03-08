package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

import com.google.common.primitives.Ints;

class UncompressedReadRebufferer extends AbstractRebufferer
{
    public UncompressedReadRebufferer(ChannelProxy channel, long fileLength)
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

        bufferOffset = position;
        return buffer;
    }

//    public ByteBuffer rebufferPageAligned(long position, ByteBuffer buffer)
//    {
//        assert position <= fileLength;
//        long pageAlignedPos = position & ~4095;
//
//        buffer.clear();
//
//        buffer.limit(Ints.checkedCast(Math.min(fileLength - pageAlignedPos, buffer.capacity())));
//        channel.read(buffer, pageAlignedPos);
//
//        buffer.flip();
//        buffer.position(Ints.checkedCast(position - pageAlignedPos));
//        bufferOffset = pageAlignedPos;
//        return buffer;
//    }
}