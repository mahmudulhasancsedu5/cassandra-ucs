package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

import org.apache.cassandra.utils.memory.BufferPool;

/**
 * Buffer provider for bufferless rebufferers. Instances of this class are reader-specific and thus do not need to be
 * thread-safe since the reader itself isn't.
 */
public abstract class BufferManagingRebufferer implements Rebufferer, Rebufferer.BufferHolder
{
    protected final BufferlessRebufferer rebufferer;
    protected final ByteBuffer buffer;
    protected long offset = 0;

    public static BufferManagingRebufferer on(BufferlessRebufferer wrapped)
    {
        return wrapped.alignmentRequired()
             ? new Aligned(wrapped)
             : new Unaligned(wrapped);
    }

    abstract long alignedPosition(long position);

    public BufferManagingRebufferer(BufferlessRebufferer wrapped)
    {
        this.rebufferer = wrapped;
        buffer = RandomAccessReader.allocateBuffer(wrapped.chunkSize(), wrapped.preferredBufferType());
        buffer.limit(0);
    }

    @Override
    public void closeReader()
    {
        BufferPool.put(buffer);
        offset = -1;
    }

    @Override
    public void close()
    {
        assert offset == -1;    // reader must be closed at this point.
        rebufferer.close();
    }

    @Override
    public void release()
    {
        // nothing to do, we don't delete buffers before we're closed.
    }

    @Override
    public ChannelProxy channel()
    {
        return rebufferer.channel();
    }

    @Override
    public long fileLength()
    {
        return rebufferer.fileLength();
    }

    @Override
    public BufferHolder rebuffer(long position)
    {
        offset = alignedPosition(position);
        rebufferer.rebuffer(offset, buffer);
        return this;
    }

    public ByteBuffer buffer()
    {
        return buffer;
    }
    
    public long offset()
    {
        return offset;
    }

    @Override
    public double getCrcCheckChance()
    {
        return rebufferer.getCrcCheckChance();
    }

    public static class Unaligned extends BufferManagingRebufferer implements Rebufferer
    {
        public Unaligned(BufferlessRebufferer wrapped)
        {
            super(wrapped);
        }

        @Override
        long alignedPosition(long position)
        {
            return position;
        }
    }

    public static class Aligned extends BufferManagingRebufferer implements Rebufferer
    {
        public Aligned(BufferlessRebufferer wrapped)
        {
            super(wrapped);
            assert Integer.bitCount(wrapped.chunkSize()) == 1;
        }

        @Override
        long alignedPosition(long position)
        {
            return position & -buffer.capacity();
        }
    }
}
