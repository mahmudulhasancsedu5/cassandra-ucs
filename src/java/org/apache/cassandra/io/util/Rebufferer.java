package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

/**
 * Rebufferer for reading data by a RandomAccessReader.
 */
public interface Rebufferer extends RebuffererData
{
    /**
     * Rebuffer (move on or seek to) a given position, and return a buffer that can be used there.
     * The returned buffer will be positioned so that position == buffer.position() + bufferOffset(position).
     * The only guarantee about the size of the returned data is that it will not be empty unless rebuffered at the end
     * of the file.
     */
    ByteBuffer rebuffer(long position);

    /**
     * Returns the offset of the buffer that will be returned when rebuffering to the given position.
     */
    long bufferOffset(long position);

    /**
     * Returns an initial buffer, which may be empty or contain the data at the beginning of the file.
     */
    ByteBuffer initialBuffer();
}