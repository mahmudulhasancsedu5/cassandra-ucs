package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

class MmapRebufferer extends AbstractRebufferer implements Rebufferer
{
    protected final MmappedRegions regions;

    static class Buffer implements BufferHolder
    {
        private final ByteBuffer buffer;
        private final long offset;

        public Buffer(ByteBuffer buffer, long offset)
        {
            this.buffer = buffer;
            this.offset = offset;
        }

        public ByteBuffer buffer()
        {
            return buffer.duplicate();
        }

        public long offset()
        {
            return offset;
        }

        public void release()
        {
            // nothing to do
        }
    }

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
}