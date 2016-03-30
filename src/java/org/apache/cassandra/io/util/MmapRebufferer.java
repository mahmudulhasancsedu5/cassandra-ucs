package org.apache.cassandra.io.util;

/**
 * Rebufferer for memory-mapped files. Thread-safe and shared among reader instances.
 * This is simply a thin wrapper around MmappedRegions as the buffers there can be used directly after duplication.
 */
class MmapRebufferer extends AbstractRebufferer implements Rebufferer, SharedRebufferer
{
    protected final MmappedRegions regions;

    public MmapRebufferer(ChannelProxy channel, long fileLength, MmappedRegions regions)
    {
        super(channel, fileLength);
        this.regions = regions;
    }

    public BufferHolder rebuffer(long position)
    {
        return regions.floor(position);
    }

    public void close()
    {
        regions.closeQuietly();
    }

    @Override
    public void closeReader()
    {
        // Instance is shared among readers. Nothing to release.
    }

    @Override
    public String toString()
    {
        return String.format("%s(%s - data length %d)",
                             getClass().getSimpleName(),
                             channel.filePath(),
                             fileLength());
    }
}