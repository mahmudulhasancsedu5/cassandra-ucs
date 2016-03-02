package org.apache.cassandra.io.util;

import java.nio.ByteBuffer;

public interface Rebufferer extends AutoCloseable
{

    ByteBuffer rebuffer(long position);

    void close();

    long bufferOffset();

    ChannelProxy channel();

    long fileLength();

    ByteBuffer initialBuffer();

}