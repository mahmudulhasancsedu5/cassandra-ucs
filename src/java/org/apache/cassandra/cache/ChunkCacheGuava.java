package org.apache.cassandra.cache;

import java.nio.ByteBuffer;

import com.google.common.cache.*;
import com.google.common.collect.Iterables;

import org.apache.cassandra.io.util.*;
import org.apache.cassandra.metrics.CacheMissMetrics;

public class ChunkCacheGuava extends ChunkCacheBase
        implements ChunkCache.ChunkCacheType, CacheSize, RemovalListener<ChunkCacheGuava.Key, ChunkCacheGuava.Buffer> 
{
    final Cache<Key, Buffer> cache;
    private final long cacheSize;
    private final CacheMissMetrics metrics;

    public ChunkCacheGuava(long cacheSize)
    {
        this.cacheSize = cacheSize;
        cache = CacheBuilder.newBuilder()
                .maximumWeight(cacheSize)
                .weigher((key, buffer) -> weight((Buffer) buffer))
                .removalListener(this)
                .build();
        metrics = new CacheMissMetrics("ChunkCache", this);
    }

    @Override
    public void onRemoval(RemovalNotification<Key, Buffer> removal)
    {
        removal.getValue().release();
    }

    @Override
    public void close()
    {
        cache.invalidateAll();
        metrics.close();
    }

    @Override
    public CacheMissMetrics metrics()
    {
        return metrics;
    }

    @Override
    public void invalidatePosition(SegmentedFile dfile, long position)
    {
        if (!(dfile.rebufferer() instanceof CachingRebufferer))
            return;

        ((CachingRebufferer) dfile.rebufferer()).invalidate(position);
    }

    @Override
    public void invalidateFile(String fileName)
    {
        cache.invalidateAll(Iterables.filter(cache.asMap().keySet(), x -> x.path.equals(fileName)));
    }

    @Override
    Buffer getAndReference(Key key)
    {
        Buffer buf = cache.getIfPresent(key);
        if (buf == null)
            return null;
        return buf.reference();
    }

    @Override
    Buffer put(ByteBuffer buffer, Key key)
    {
        Buffer buf = new Buffer(buffer, key.position).reference(); // two refs, one for caller one for cache
        cache.put(key, buf);
        return buf;
    }

    @Override
    public void invalidate(Key key)
    {
        cache.invalidate(key);
    }

    // TODO: Invalidate caches for obsoleted/MOVED_START tables?

    // CacheSize methods

    @Override
    public long capacity()
    {
        return cacheSize;
    }

    @Override
    public void setCapacity(long capacity)
    {
        throw new UnsupportedOperationException("Chunk cache size cannot be changed.");
    }

    @Override
    public int size()
    {
        return cache.asMap().size();
    }

    @Override
    public long weightedSize()
    {
        return cache.asMap().values().stream().mapToLong(ChunkCacheBase::weight).sum();
    }
}
