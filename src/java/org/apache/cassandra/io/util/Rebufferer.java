package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

public interface Rebufferer extends AutoCloseable
{

    ByteBuffer rebuffer(long position, ByteBuffer buffer);

    void close();

    long bufferOffset();

    ChannelProxy channel();

    long fileLength();

    ByteBuffer initialBuffer();

    /**
     * Needed for tests. Returns the table's CRC check chance, which is only set for compressed tables.
     */
    double getCrcCheckChance();

}