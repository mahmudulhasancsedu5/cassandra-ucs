package org.apache.cassandra.cache;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import net.sf.ehcache.*;
import net.sf.ehcache.event.CacheEventListener;
import net.sf.ehcache.store.Policy;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.metrics.CacheMissMetrics;

public class ChunkCacheEHCache extends ChunkCacheBase
        implements ChunkCache.ChunkCacheType, CacheSize, CacheEventListener
{
    private final Cache cache;
    public final CacheMissMetrics metrics;
    private final long cacheSize;
    final AtomicLong currentSize = new AtomicLong(0);
    private CacheManager manager;

    public ChunkCacheEHCache(long cacheSize, Policy policy)
    {
        this.cacheSize = cacheSize;
        manager = CacheManager.create();
        cache = new Cache("ChunkCache" + policy.getName(), (int) (cacheSize / 65536), false, false, Integer.MAX_VALUE, Integer.MAX_VALUE);
        cache.getCacheEventNotificationService().registerListener(this);
        manager.addCache(cache);
        cache.setMemoryStoreEvictionPolicy(policy);
        metrics = new CacheMissMetrics("ChunkCache", this);
    }

    @Override
    public void close()
    {
        for (Object o : cache.getKeys())
            cache.remove(o);
//        manager.shutdown();
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
        cache.removeAll(ImmutableSet.copyOf(
                Iterables.filter(
                        (Iterable<?>) cache.getKeys(),
                        x -> ((Key)x).path.equals(fileName))));
    }

    @Override
    public CacheMissMetrics metrics()
    {
        return metrics;
    }

    @Override
    Buffer getAndReference(Key key)
    {
        Element element = cache.get(key);
        if (element == null)
            return null;
        return ((Buffer) element.getObjectValue()).reference();
    }

    @Override
    Buffer put(ByteBuffer buffer, Key key)
    {
        Buffer buf = new Buffer(buffer, key.position);
        cache.putIfAbsent(new Element(key, buf));       // ref added if put
        return buf;
    }

    @Override
    public void invalidate(Key key)
    {
        cache.remove(key);
    }

    // CacheEventListener methods

    @Override
    public void notifyElementRemoved(Ehcache cache, Element element) throws CacheException
    {
        Buffer buf = (Buffer) element.getObjectValue();
        if (buf != null)
        {
            currentSize.addAndGet(-weight(buf));
            buf.release();
        }
    }

    @Override
    public void notifyElementPut(Ehcache cache, Element element) throws CacheException
    {
        Buffer buf = (Buffer) element.getObjectValue();
        currentSize.addAndGet(weight(buf));
        buf.reference();
    }

    @Override
    public void notifyElementUpdated(Ehcache cache, Element element) throws CacheException
    {
        throw new AssertionError();
    }

    @Override
    public void notifyElementExpired(Ehcache cache, Element element)
    {
        throw new AssertionError();
    }

    @Override
    public void notifyElementEvicted(Ehcache cache, Element element)
    {
        Buffer buf = (Buffer) element.getObjectValue();
        currentSize.addAndGet(-weight(buf));
        buf.release();
    }

    @Override
    public void notifyRemoveAll(Ehcache cache)
    {
        // Don't know what to do.
        // We should be empty at this point.
//        for (Object o : cache.getKeys())
//            cache.remove(o);
//        assert cache.getSize() == 0;
    }

    @Override
    public void dispose()
    {
    }

    public Object clone()
    {
        throw new AssertionError();
    }

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
        return cache.getSize();
    }

    @Override
    public long weightedSize()
    {
        return currentSize.get();
    }
}
