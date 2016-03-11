package org.apache.cassandra.io.util;

public interface RebuffererData extends AutoCloseable
{
    void close();               // no checked exceptions

    ChannelProxy channel();

    long fileLength();

    /**
     * Needed for tests. Returns the table's CRC check chance, which is only set for compressed tables.
     */
    double getCrcCheckChance();
}