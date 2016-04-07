package org.apache.cassandra.cache;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.BaseRebufferer;
import org.apache.cassandra.io.util.BufferlessRebufferer;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.metrics.CacheMissMetrics;

public class ChunkCache
{
    static final int RESERVED_POOL_SPACE_IN_MB = 32;
    static final long INITIAL_CACHE_SIZE = 1024L * 1024L * Math.max(0, DatabaseDescriptor.getFileCacheSizeInMB() - RESERVED_POOL_SPACE_IN_MB);

    static public ChunkCacheType instance = INITIAL_CACHE_SIZE > 0 ? new ChunkCacheOwnMap(INITIAL_CACHE_SIZE) : new NoCache();

    @VisibleForTesting
    public static void switchTo(ChunkCacheType newInstance)
    {
        ChunkCacheType oldInstance = instance;
        instance = (newInstance != null) ? newInstance : new NoCache();
        oldInstance.close();
    }

    public interface ChunkCacheType extends AutoCloseable
    {
        BaseRebufferer wrap(BufferlessRebufferer file);

        void invalidatePosition(SegmentedFile dfile, long position);

        void invalidateFile(String fileName);

        void close();

        CacheMissMetrics metrics();
    }

    public static class NoCache implements ChunkCacheType
    {
        public BaseRebufferer wrap(BufferlessRebufferer file)
        {
            return file;
        }

        public void invalidatePosition(SegmentedFile dfile, long position)
        {
        }

        public void invalidateFile(String fileName)
        {
        }

        public void close()
        {
        }

        public CacheMissMetrics metrics()
        {
            return null;
        }
    }
}