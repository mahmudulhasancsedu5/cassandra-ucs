package org.apache.cassandra.cache;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.cassandra.io.util.*;
import org.apache.cassandra.metrics.CacheMissMetrics;

public class ChunkCacheICache extends ChunkCacheBase
        implements ChunkCache.ChunkCacheType, EvictionStrategy.RemovalListener
{
    private final ICache<Key, Buffer> cache;
    public final CacheMissMetrics metrics;

    public ChunkCacheICache(Class<? extends EvictionStrategy> evictionStrategyClass, long cacheSize)
    {
        try
        {
            EvictionStrategy evictionStrategy = evictionStrategyClass.getConstructor(EvictionStrategy.RemovalListener.class,
                                                                                     EvictionStrategy.Weigher.class,
                                                                                     long.class)
                    .newInstance((EvictionStrategy.RemovalListener) this,
                                 (EvictionStrategy.Weigher) ((key, buffer) -> weight((Buffer) buffer)),
                                 cacheSize);
            cache = SharedEvictionStrategyCache.create(evictionStrategy);
            metrics = new CacheMissMetrics("ChunkCache", cache);
        } catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException
                | SecurityException | InvocationTargetException e)
        {
            throw new AssertionError(e);
        }
    }

    public ChunkCacheICache(long cacheSize)
    {
        cache = LirsCache.create(this, (key, buffer) -> weight((Buffer) buffer), cacheSize);
        metrics = new CacheMissMetrics("ChunkCache", cache);
    }

    @Override
    public void onRemove(Object key, Object buffer)
    {
        ((Buffer) buffer).release();
    }

    @Override
    public void close()
    {
        cache.clear();
        metrics.close();
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
        for (Iterator<Key> it = cache.keyIterator(); it.hasNext();)
        {
            Key key = it.next();
            if (key.path.equals(fileName))
                cache.remove(key);
        }
    }

    @Override
    public CacheMissMetrics metrics()
    {
        return metrics;
    }

    @Override
    Buffer getAndReference(Key key)
    {
        Buffer buf = cache.get(key);
        if (buf == null)
            return null;
        return buf.reference();
    }

    @Override
    Buffer put(ByteBuffer buffer, Key key)
    {
        Buffer buf = new Buffer(buffer, key.position);
        if (cache.putIfAbsent(key, buf))
            buf.reference();
//        cache.put(key, buf);
        return buf;
    }

    @Override
    public void invalidate(Key key)
    {
        cache.remove(key);
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + ":" + cache.toString();
    }
}
