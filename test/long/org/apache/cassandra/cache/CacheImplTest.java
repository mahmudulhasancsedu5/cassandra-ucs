package org.apache.cassandra.cache;

import static org.junit.Assert.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import org.junit.Test;

import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import org.apache.cassandra.cache.CacheImpl.Weigher;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.EstimatedHistogramReservoir;

public class CacheImplTest implements CacheImpl.RemovalListener<Long, CacheImplTest.Data>, Weigher<Long, CacheImplTest.Data>
{
    static final int ENTRY_SIZE = 2;
    static final long CACHE_SIZE = 600_000;
    static final long KEY_RANGE = 5 * CACHE_SIZE;
    static final int SWITCH_PERIOD = 20_000;
    static final int THREAD_ITERS = 500_000;
    static final int THREADS_IN_PARALLEL = 50;
    static final int THREAD_RUNS = 200;
    static final int RELEASE_CHECKED_PERIOD = 0;
    static final int REMOVE_PERIOD = 1600;
    
    static final int[] WORKING_SET_SIZES = new int[] { 10_000, 30_000, 100_000, (int) KEY_RANGE };
    static final int[] WORKING_SET_CHANCES = new int[] { 15, 10, 5, 1 };
    static final int[] WORKING_SETS;
    static {
        int acc = 0;
        for (int i = 0; i < WORKING_SET_CHANCES.length; ++i)
            WORKING_SET_CHANCES[i] = acc += WORKING_SET_CHANCES[i];
        WORKING_SETS = new int[acc];
        int j = 0;
        for (int i = 0; i < acc; ++i)
        {
            if (i == WORKING_SET_CHANCES[j])
                ++j;
            WORKING_SETS[i] = WORKING_SET_SIZES[j];
        }
    }

    class Data
    {
        final long gen;
        final long[] data;
        volatile boolean released;
        volatile boolean removed;

        Data(Random r, long key)
        {
            data = new long[ENTRY_SIZE];
            gen = r.nextLong();
            Arrays.fill(data, convert(key, gen));
            released = false;
            removed = false;
        }

        void check(Random r, long key)
        {
            long v = convert(key, gen);
            int idx = r.nextInt(data.length);
            assertEquals(v, data[idx]);
            if (removed /*| released*/)
                assertNotSame(this, cache.get(key));
        }
    }

    class ReleaseChecked extends Data
    {
        ReleaseChecked(Random r, long key)
        {
            super(r, key);
        }

        @Override
        protected void finalize()
        {
            assertTrue(released);
        }
    }

    class TestRunnable implements Runnable
    {
        ThreadLocalRandom rand = ThreadLocalRandom.current();
        final int wsSize;

        TestRunnable(int wsSize)
        {
            this.wsSize = wsSize;
        }

        @Override
        public void run()
        {
            Long[] ws = new Long[wsSize];
            for (int i = 0; i < ws.length; ++i)
                ws[i] = rand.nextLong(KEY_RANGE);
            for (int i = 0; i < THREAD_ITERS; ++i)
            {
                Long key = ws[rand.nextInt(ws.length)];

                if (rand.nextInt(REMOVE_PERIOD) != 0)
                    get(rand, key).check(rand, key);
                else
                    remove(rand, key);

                if (rand.nextInt(SWITCH_PERIOD) == 0)
                    ws[rand.nextInt(ws.length)] = rand.nextLong(KEY_RANGE);
            }
        }
    }
    
    class GuavaCache implements ICache<Long, Data>, com.google.common.cache.RemovalListener<Long, Data>
    {
        final Cache<Long, Data> cache;

        public GuavaCache(long size)
        {
            cache = CacheBuilder.newBuilder()
                    .concurrencyLevel(THREADS_IN_PARALLEL)
                    .maximumWeight(size)
                    .weigher((key, buffer) -> (int) weight(null, null))
                    .removalListener(this)
                    .build();
        }

        @Override
        public long capacity()
        {
            return 0;
        }

        @Override
        public void setCapacity(long capacity)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size()
        {
            return (int) cache.size();
        }

        @Override
        public long weightedSize()
        {
            return cache.asMap().values().stream().mapToLong(x -> weight(null, x)).sum();
        }

        @Override
        public void put(Long key, Data value)
        {
            cache.put(key, value);
        }

        @Override
        public boolean putIfAbsent(Long key, Data value)
        {
            return cache.asMap().putIfAbsent(key, value) == null;
        }

        @Override
        public boolean replace(Long key, Data old, Data value)
        {
            return cache.asMap().replace(key, old, value);
        }

        @Override
        public Data get(Long key)
        {
            return cache.getIfPresent(key);
        }

        @Override
        public void remove(Long key)
        {
            cache.invalidate(key);
        }

        @Override
        public void clear()
        {
            cache.invalidateAll();
        }

        @Override
        public Iterator<Long> keyIterator()
        {
            return cache.asMap().keySet().iterator();
        }

        @Override
        public Iterator<Long> hotKeyIterator(int n)
        {
            return null;
        }

        @Override
        public boolean containsKey(Long key)
        {
            return cache.getIfPresent(key) != null;
        }

        @Override
        public void onRemoval(RemovalNotification<Long, Data> removed)
        {
            CacheImplTest.this.remove(removed.getKey(), removed.getValue());
        }
    }

    ICache<Long, Data> cache;
    Timer getLatency;
    Timer missLatency;
    Timer reqLatency;

    @Test
    public void testLirs() throws InterruptedException, ExecutionException
    {
        testCache(LirsCache.create(this, this, CACHE_SIZE * weight(null, null)));
    }

    @Test
    public void testFifo() throws InterruptedException, ExecutionException
    {
        testCache(CacheImpl.create(this, this, CACHE_SIZE * weight(null, null), new EvictionStrategyFifo<>()));
    }

    @Test
    public void testLru() throws InterruptedException, ExecutionException
    {
        testCache(CacheImpl.create(this, this, CACHE_SIZE * weight(null, null), new EvictionStrategyLru<>()));
    }

    @Test
    public void testGuava() throws InterruptedException, ExecutionException
    {
        testCache(new GuavaCache(CACHE_SIZE * weight(null, null)));
    }

    public void testCache(ICache<Long, Data> cacheToTest) throws InterruptedException, ExecutionException
    {
        getLatency = new Timer(new EstimatedHistogramReservoir(false));
        missLatency = new Timer(new EstimatedHistogramReservoir(false));
        reqLatency = new Timer(new EstimatedHistogramReservoir(false));
        cache = cacheToTest;

        ExecutorService es = Executors.newFixedThreadPool(THREADS_IN_PARALLEL);
        List<Future<?>> tasks = new ArrayList<>(THREAD_RUNS);
        long startTime = System.currentTimeMillis();
        Multiset<Integer> wsSizes = HashMultiset.create();
        Random seeded = new Random(1);
        for (int i = 0; i < THREAD_RUNS; ++i)
        {
            int sz = WORKING_SETS[seeded.nextInt(WORKING_SETS.length)];
            tasks.add(es.submit(new TestRunnable(sz)));
            wsSizes.add(sz);
        }

        for (Future<?> f : tasks)
        {
            f.get();
            System.out.print('.');
        }
        long endTime = System.currentTimeMillis();
        System.out.println();

        printLatency("Request", reqLatency);
        printLatency("Get", getLatency);
        printLatency("Miss", missLatency);
        long reqCount = reqLatency.getCount();
        long missCount = missLatency.getCount();
        double cacheLatency = reqLatency.getSnapshot().getMean() -
                missLatency.getSnapshot().getMean() * missCount / reqCount;
        double hitRatio = 1.0 * (reqCount - missCount) / reqCount;
        System.out.format("Mean cache latency: %.0fns\n", cacheLatency);
        System.out.format("%s size %,d(%s) requests %,d hit ratio %f\n",
                cache.getClass().getSimpleName(),
                cache.size(),
                FileUtils.stringifyFileSize(cache.weightedSize()),
                reqCount,
                hitRatio);
        System.out.format("%,d threads [%d in parallel] of %,d lookups completed in %,d ms\n",
                THREAD_RUNS, THREADS_IN_PARALLEL, THREAD_ITERS, endTime - startTime);
        System.out.format("Working set sizes: %s\n", wsSizes);

        if (cache instanceof CacheImpl)
            ((CacheImpl<?, ?, ?>) cache).checkState();
        cache.clear();
        assertEquals(0, aliveCount.get());

        System.gc();
        System.runFinalization();
        Thread.sleep(500);
        System.gc();
        System.runFinalization();
    }

    private void printLatency(String name, Timer latency)
    {
        Snapshot s = latency.getSnapshot();
        System.out.format("%8s latency: mean: %,9.0f|min: %,9d|median: %,9.0f|75pct: %,9.0f|95pct: %,9.0f|98pct: %,9.0f|99pct: %,9.0f|99.9pct: %,9.0f|max: %,9d ns\n",
                name, s.getMean(), s.getMin(), s.getMedian(), s.get75thPercentile(), s.get95thPercentile(), s.get98thPercentile(), s.get99thPercentile(), s.get999thPercentile(), s.getMax());
    }

    public Data get(Random r, Long key)
    {
        Data d;
        Timer.Context ctxReq = reqLatency.time();
        {
            Timer.Context ctxGet = getLatency.time();
            {
                d = cache.get(key);
            }
            ctxGet.close();
            if (d == null)
            {
                Timer.Context ctx = missLatency.time();
                {
                    d = load(r, key);
                }
                ctx.close();
//                cache.put(key, d);
                if (!cache.putIfAbsent(key, d))
                    remove(key, d);
            }
        }
        ctxReq.close();
        return d;
    }

//    public Data get(Random r, Long key)
//    {
//        Data d = cache.get(key);
//        if (d == null)
//        {
//            d = load(r, key);
//            if (!cache.putIfAbsent(key, d))
//                remove(key, d);
//        }
//        return d;
//    }
    AtomicLong aliveCount = new AtomicLong();
    
    public Data load(Random r, Long key)
    {
        aliveCount.incrementAndGet();
        return (RELEASE_CHECKED_PERIOD > 0 && r.nextInt(RELEASE_CHECKED_PERIOD) == 0) ? new ReleaseChecked(r, key) : new Data(r, key);
    }

    public void remove(Random r, Long key)
    {
        Data d = get(r, key);
        cache.remove(key);
        d.removed = true;
    }

    static final long convert(long v, long gen)
    {
        return Long.reverse(v) ^ gen;
    }

    @Override
    public long weight(Long key, Data value)
    {
        return ENTRY_SIZE * Long.BYTES;
    }

    @Override
    public void remove(Long key, Data value)
    {
        value.check(ThreadLocalRandom.current(), key);
        assertFalse(value.released);
        value.released = true;
        aliveCount.decrementAndGet();
    }
}
