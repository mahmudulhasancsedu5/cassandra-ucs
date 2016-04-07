package org.apache.cassandra.cache;

import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Throwables;

import com.codahale.metrics.Timer;
import org.apache.cassandra.cache.ChunkCache.ChunkCacheType;
import org.apache.cassandra.cache.ChunkCacheBase.Buffer;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.BufferlessRebufferer;
import org.apache.cassandra.io.util.ChannelProxy;
import org.apache.cassandra.io.util.Rebufferer;
import org.apache.cassandra.io.util.SegmentedFile;
import org.apache.cassandra.metrics.CacheMissMetrics;
import org.apache.cassandra.utils.memory.BufferPool;

public class ChunkCacheOwnMap implements ChunkCacheType, SharedEvictionStrategyCache.RemovalListener<Long, ChunkCacheOwnMap.Buffer>
{
    private final Map<FileSelector, ICache<Long, Buffer>> caches;
    private final EvictionStrategy evictionStrategy;
    public final CacheMissMetrics metrics;

    @SuppressWarnings("unchecked")
    public ChunkCacheOwnMap(Class<? extends EvictionStrategy> evictionStrategyClass, long cacheSize)
    {
        try
        {
            caches = new HashMap<>();
            evictionStrategy = evictionStrategyClass.getConstructor(EvictionStrategy.Weigher.class, long.class)
                    .newInstance((EvictionStrategy.Weigher) ((pos, buffer) -> weight((Buffer) buffer)), cacheSize);
            metrics = new CacheMissMetrics("ChunkCache", evictionStrategy);
        } catch (IllegalAccessException | IllegalArgumentException | InstantiationException | NoSuchMethodException
                | SecurityException | InvocationTargetException e)
        {
            throw new AssertionError(e);
        }
    }

    public ChunkCacheOwnMap(long cacheSize)
    {
        caches = new HashMap<>();
        evictionStrategy = new EvictionStrategyLirsSync((pos, buffer) -> weight((Buffer) buffer), cacheSize);
        metrics = new CacheMissMetrics("ChunkCache", evictionStrategy);
    }

    public static int weight(Buffer buffer)
    {
        return buffer.buffer.capacity();
    }

    @Override
    public void remove(Long position, Buffer buffer)
    {
        buffer.release();
    }

    @Override
    public final Rebufferer wrap(BufferlessRebufferer file)
    {
        return new CachingRebufferer(file);
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
        for (Map.Entry<FileSelector, ICache<Long, Buffer>> cache : caches.entrySet())
            if (cache.getKey().path.equals(fileName))
                cache.getValue().clear();
    }

    @Override
    public CacheMissMetrics metrics()
    {
        return metrics;
    }

    @Override
    public void close()
    {
        caches.clear();
        evictionStrategy.clear();
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + ":" + evictionStrategy.getClass().getSimpleName();
    }

    protected static class FileSelector
    {
        final BufferlessRebufferer file;
        final String path;

        public FileSelector(BufferlessRebufferer file)
        {
            super();
            this.file = file;
            this.path = file.channel().filePath();
        }

        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + path.hashCode();
            result = prime * result + file.getClass().hashCode();
            return result;
        }

        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;

            FileSelector other = (FileSelector) obj;
            return file.getClass() == other.file.getClass()
                   && path.equals(other.path);
        }
    }

    protected static class Buffer implements Rebufferer.BufferHolder
    {
        private final ByteBuffer buffer;
        private final long offset;
        private final AtomicInteger references;

        public Buffer(ByteBuffer buffer, long offset)
        {
            this.buffer = buffer;
            this.offset = offset;
            references = new AtomicInteger(1);  // start referenced.
        }

        Buffer reference()
        {
            int refCount;
            do
            {
                refCount = references.get();
                if (refCount == 0)
                    // Buffer was released before we managed to reference it. 
                    return null;
            } while (!references.compareAndSet(refCount, refCount + 1));

            return this;
        }

        @Override
        public ByteBuffer buffer()
        {
            assert references.get() > 0;
            return buffer.duplicate();
        }

        @Override
        public long offset()
        {
            return offset;
        }

        @Override
        public void release()
        {
            if (references.decrementAndGet() == 0)
                BufferPool.put(buffer);
        }
    }

    /**
     * Rebufferer providing cached chunks where data is obtained from the specified BufferlessRebufferer.
     * Thread-safe. One instance per SegmentedFile, created by ReaderCache.maybeWrap if the cache is enabled.
     */
    class CachingRebufferer implements Rebufferer
    {
        private final BufferlessRebufferer source;
        final long alignmentMask;
        ICache<Long, Buffer> cache;

        public CachingRebufferer(BufferlessRebufferer file)
        {
            source = file;
            synchronized (caches)
            {
                cache = caches.computeIfAbsent(new FileSelector(file),
                                               fs -> SharedEvictionStrategyCache.create(ChunkCacheOwnMap.this, evictionStrategy));
            }
            int chunkSize = file.chunkSize();
            assert Integer.bitCount(chunkSize) == 1;    // Must be power of two
            alignmentMask = -chunkSize;
        }

        @Override
        public Buffer rebuffer(long position)
        {
            try
            {
                CacheMissMetrics metrics = metrics();
                metrics.requests.mark();
                Long pageAlignedPos = position & alignmentMask;
                Buffer buf;
                do
                    try (Timer.Context ctxReq = metrics.reqLatency.time())
                    {
                        buf = getAndReference(pageAlignedPos);
                        if (buf == null)
                        {
                            metrics.misses.mark();
                            ByteBuffer buffer;
                            try (Timer.Context ctx = metrics.missLatency.time())
                            {
                                buffer = source.rebuffer(pageAlignedPos, BufferPool.get(source.chunkSize()));
                                assert buffer != null;
                            }
                            buf = put(buffer, pageAlignedPos);
                        }
                    }
                while (buf == null);

                return buf;
            }
            catch (Throwable t)
            {
                Throwables.propagateIfInstanceOf(t.getCause(), CorruptSSTableException.class);
                throw Throwables.propagate(t);
            }
        }

        public void invalidate(long position)
        {
            long pageAlignedPos = position & alignmentMask;
            cache.remove(pageAlignedPos);
        }

        Buffer getAndReference(Long position)
        {
            Buffer buf = cache.get(position);
            if (buf == null)
                return null;
            return buf.reference();
        }

        Buffer put(ByteBuffer buffer, Long position)
        {
            Buffer buf = new Buffer(buffer, position);
            if (cache.putIfAbsent(position, buf))
                buf.reference();
//            cache.put(key, buf);
            return buf;
        }

        @Override
        public void close()
        {
            source.close();
        }

        @Override
        public void closeReader()
        {
            // Instance is shared among readers. Nothing to release.
        }

        @Override
        public ChannelProxy channel()
        {
            return source.channel();
        }

        @Override
        public long fileLength()
        {
            return source.fileLength();
        }

        @Override
        public double getCrcCheckChance()
        {
            return source.getCrcCheckChance();
        }

        @Override
        public String toString()
        {
            return "CachingRebufferer:" + source.toString();
        }
    }
}