package org.apache.cassandra.io.util;

/**
 * Base class for RandomAccessReader rebufferers. There are two kinds of this, and a SegmentedFile must provide a
 * threadsafe instance of one of them.
 */
public interface BaseRebufferer extends AutoCloseable
{
    void close();               // no checked exceptions

    ChannelProxy channel();

    long fileLength();

    /**
     * Needed for tests. Returns the table's CRC check chance, which is only set for compressed tables.
     */
    double getCrcCheckChance();
}