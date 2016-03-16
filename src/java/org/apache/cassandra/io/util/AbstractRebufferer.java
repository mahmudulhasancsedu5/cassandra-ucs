package org.apache.cassandra.io.util;

public abstract class AbstractRebufferer implements BaseRebufferer
{
    protected final ChannelProxy channel;
    protected final long fileLength;

    public AbstractRebufferer(ChannelProxy channel, long fileLength)
    {
        this.channel = channel;
        this.fileLength = fileLength >= 0 ? fileLength : channel.size();
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
    public double getCrcCheckChance()
    {
        return 0; // Only valid for compressed files.
    }
}