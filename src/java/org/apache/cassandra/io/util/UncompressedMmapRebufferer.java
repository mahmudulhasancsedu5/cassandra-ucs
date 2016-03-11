package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

import com.google.common.primitives.Ints;

class UncompressedMmapRebufferer extends AbstractRebufferer implements Rebufferer
{
    protected final MmappedRegions regions;

    public UncompressedMmapRebufferer(ChannelProxy channel, long fileLength, MmappedRegions regions)
    {
        super(channel, fileLength);
        this.regions = regions;
    }

    @Override
    public ByteBuffer rebuffer(long position)
    {
        MmappedRegions.Region region = regions.floor(position);
        long bufferOffset = region.bottom();
        ByteBuffer buffer = region.buffer.duplicate();
        buffer.position(Ints.checkedCast(position - bufferOffset));
        return buffer;
    }

    @Override
    public long bufferOffset(long position)
    {
        return regions.floor(position).bottom();
    }

    @Override
    public void close()
    {
        // nothing -- regions managed elsewhere
    }

    @Override
    public ByteBuffer initialBuffer()
    {
        // Note: this will not read anything unless we do access the buffer.
        return rebuffer(0);
    }
}