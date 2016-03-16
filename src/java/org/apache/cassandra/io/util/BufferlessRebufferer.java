package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

/**
 * Rebuffering component that does not need to use its own buffer.
 * A caching or buffer-managing rebufferer will reference one of these to do the actual reading.
 * Note: Implementations of this interface must be thread-safe!
 */
public interface BufferlessRebufferer extends RebuffererData
{
    /**
     * Rebuffer (i.e. read) at the given position, attempting to fill the capacity of the given buffer.
     * The returned buffer must be positioned at 0, with limit set at the size of the available data.
     * Rebufferer may have requirements for the positioning and/or size of the buffer (e.g. chunk-aligned and
     * chunk-sized). These must be satisfied by the caller. 
     *
     * @return buffer
     */
    ByteBuffer rebuffer(long position, ByteBuffer buffer);
}