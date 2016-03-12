package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

/**
 * Rebufferer for reading data by a RandomAccessReader.
 */
public interface Rebufferer extends RebuffererData
{
    /**
     * Rebuffer (move on or seek to) a given position, and return a buffer that can be used there.
     * The only guarantee about the size of the returned data is that it will not be empty unless rebuffering at the
     * end of the file, and in that case the returned buffer will contain the requested position, i.e.
     * {@code offset <= position < offset + bh.buffer().limit()}, but will not be positioned there.
     */
    BufferHolder rebuffer(long position);

    public interface BufferHolder
    {
        ByteBuffer buffer();
        long offset();
        void release();
    }

    static final BufferHolder EMPTY = new BufferHolder()
    {
        final ByteBuffer EMPTY_BUFFER = ByteBuffer.allocate(0);

        @Override
        public ByteBuffer buffer()
        {
            return EMPTY_BUFFER;
        }

        @Override
        public long offset()
        {
            return 0;
        }

        @Override
        public void release()
        {
            // nothing to do
        }
    };
}