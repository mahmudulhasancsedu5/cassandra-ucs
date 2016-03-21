package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

/**
 * Rebufferer for reading data by a RandomAccessReader.
 */
public interface Rebufferer extends BaseRebufferer
{
    /**
     * Rebuffer (move on or seek to) a given position, and return a buffer that can be used there.
     * The only guarantee about the size of the returned data is that unless rebuffering at the end of the file,
     * the buffer will not be empty and will contain the requested position, i.e.
     * {@code offset <= position < offset + bh.buffer().limit()}, but the buffer will not be positioned there.
     */
    BufferHolder rebuffer(long position);

    /**
     * Called when a reader is closed. Should clean up reader-specific data.
     */
    void closeReader();

    public interface BufferHolder
    {
        /**
         * Returns a useable buffer (i.e. one whose position and limit can be freely modified). Its limit will be set
         * to the size of the available data in the buffer.
         * The buffer must be treated as read-only.
         */
        ByteBuffer buffer();

        /**
         * Position in the file of the start of the buffer.
         */
        long offset();

        /**
         * To be called when this buffer is no longer in use. Must be called for all BufferHolders, or ReaderCache
         * will not be able to free blocks.
         */
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