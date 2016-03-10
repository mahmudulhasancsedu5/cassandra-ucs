package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

public abstract class AbstractRebufferer implements Rebufferer
{
    protected final ChannelProxy channel;
    protected final long fileLength;
    protected long bufferOffset;

    public AbstractRebufferer(ChannelProxy channel, long fileLength)
    {
        this.channel = channel;
        this.fileLength = fileLength >= 0 ? fileLength : channel.size();
    }

    @Override
    public long bufferOffset()
    {
        return bufferOffset;
    }

    @Override
    public ChannelProxy channel()
    {
        return channel;
    }

    @Override
    public long fileLength()
    {
        return fileLength;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "(filePath='" + channel + "')";
    }

    @Override
    public void close()
    {
        // nothing in base class
    }

    @Override
    public ByteBuffer initialBuffer()
    {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " cannot be used directly.");
    }

    @Override
    public double getCrcCheckChance()
    {
        return 0; // Only valid for compressed files.
    }
}