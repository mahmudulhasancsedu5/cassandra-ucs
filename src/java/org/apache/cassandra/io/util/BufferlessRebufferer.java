package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

import org.apache.cassandra.io.compress.BufferType;

/**
 * Rebuffering component that needs but does not hold a buffer.
 * A caching or buffer-managing rebufferer will reference one of these to do the actual reading.
 * Note: Implementations of this interface must be thread-safe!
 */
public interface BufferlessRebufferer extends BaseRebufferer
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

    /**
     * Buffer size required for this rebufferer. Must be power of 2 if alignment is required.
     */
    int chunkSize();

    /**
     * If true, positions passed to this rebufferer must be aligned to chunkSize.
     */
    boolean alignmentRequired();

    /**
     * Specifies type of buffer the caller should attempt to give.
     */
    BufferType preferredBufferType();
}