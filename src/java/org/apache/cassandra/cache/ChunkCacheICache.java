package org.apache.cassandra.cache;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Throwables;

import com.codahale.metrics.Timer;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.util.*;
import org.apache.cassandra.metrics.CacheMissMetrics;
import org.apache.cassandra.utils.memory.BufferPool;

public class ChunkCacheICache
        implements ChunkCache.ChunkCacheType, CacheImpl.RemovalListener<ChunkCacheICache.Key, ChunkCacheICache.Buffer> 
{
    private final ICache<Key, Buffer> cache;
    public final CacheMissMetrics metrics;

    static class Key
    {
        final BufferlessRebufferer file;
        final String path;
        final long position;

        public Key(BufferlessRebufferer file, long position)
        {
            super();
            this.file = file;
            this.position = position;
            this.path = file.channel().filePath();
        }

        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + path.hashCode();
            result = prime * result + file.getClass().hashCode();
            result = prime * result + Long.hashCode(position);
            return result;
        }

        public boolean equals(Object obj)
        {
            if (this == obj)
                return true;
            if (obj == null)
                return false;

            Key other = (Key) obj;
            return (position == other.position)
                    && file.getClass() == other.file.getClass()
                    && path.equals(other.path);
        }
    }

    static class Buffer implements Rebufferer.BufferHolder
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

    public ChunkCacheICache(long cacheSize)
    {
        cache = CacheImpl.create(this, (key, buffer) -> ((Buffer) buffer).buffer.capacity(), cacheSize);
        metrics = new CacheMissMetrics("ChunkCache", cache);
    }

    @Override
    public void remove(Key key, Buffer buffer)
    {
        buffer.release();
    }

    @Override
    public void close()
    {
        cache.clear();
        metrics.close();
    }

    @Override
    public Rebufferer wrap(BufferlessRebufferer file)
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

    // TODO: Invalidate caches for obsoleted/MOVED_START tables?

    /**
     * Rebufferer providing cached chunks where data is obtained from the specified BufferlessRebufferer.
     * Thread-safe. One instance per SegmentedFile, created by ReaderCache.maybeWrap if the cache is enabled.
     */
    class CachingRebufferer implements Rebufferer
    {
        private final BufferlessRebufferer source;
        final long alignmentMask;

        public CachingRebufferer(BufferlessRebufferer file)
        {
            source = file;
            int chunkSize = file.chunkSize();
            assert Integer.bitCount(chunkSize) == 1;    // Must be power of two
            alignmentMask = -chunkSize;
        }

        @Override
        public Buffer rebuffer(long position)
        {
            try
            {
                metrics.requests.mark();
                long pageAlignedPos = position & alignmentMask;
                Buffer buf;
                Key key = new Key(source, pageAlignedPos);
                do
                    try (Timer.Context ctxReq = metrics.reqLatency.time())
                    {
                        buf = cache.get(key);
                        if (buf != null)
                            buf = buf.reference();
                        if (buf == null)
                        {
                            metrics.misses.mark();
                            try (Timer.Context ctx = metrics.missLatency.time())
                            {
                                ByteBuffer buffer = source.rebuffer(pageAlignedPos, BufferPool.get(source.chunkSize()));
                                assert buffer != null;
                                buf = new Buffer(buffer, key.position).reference();     // two refs, one for caller one for cache
                            }
                            cache.put(key, buf);
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
            cache.remove(new Key(source, pageAlignedPos));
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
