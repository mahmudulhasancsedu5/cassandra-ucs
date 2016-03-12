package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

import com.google.common.primitives.Ints;

class UncompressedMmapRebufferer extends AbstractRebufferer implements Rebufferer, Rebufferer.BufferHolder
{
    protected final MmappedRegions regions;
    ByteBuffer buffer;
    long offset = 0;

    public UncompressedMmapRebufferer(ChannelProxy channel, long fileLength, MmappedRegions regions)
    {
        super(channel, fileLength);
        this.regions = regions;
    }

    @Override
    public BufferHolder rebuffer(long position)
    {
        MmappedRegions.Region region = regions.floor(position);
        offset = region.bottom();
        buffer = region.buffer.duplicate();
        return this;
    }

    @Override
    public ByteBuffer buffer()
    {
        return buffer;
    }

    @Override
    public long offset()
    {
        return offset;
    }

    @Override
    public void release()
    {
        // nothing to do, we don't delete buffers before we're closed.
    }

    @Override
    public void close()
    {
        // nothing -- regions managed elsewhere
    }
}