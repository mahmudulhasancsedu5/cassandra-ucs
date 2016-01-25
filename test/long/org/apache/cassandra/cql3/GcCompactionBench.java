/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.cql3;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import com.google.common.collect.Iterables;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config.CommitLogSync;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.CompactionController;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;

public class GcCompactionBench extends CQLTester
{
    private static final int DEL_SECTIONS = 1000;
    private static final int RANGE_FREQUENCY_INV = 4;
    static final int COUNT = 19000;
    static final int ITERS = 29;

    static final int KEY_RANGE = 50;
    static final int CLUSTERING_RANGE = 1000 * 1000 * 35;

    static final int EXTRA_SIZE = 1025;

    // The name of this method is important!
    // CommitLog settings must be applied before CQLTester sets up; by using the same name as its @BeforeClass method we
    // are effectively overriding it.
    @BeforeClass
    public static void setUpClass()
    {
        DatabaseDescriptor.setCommitLogSync(CommitLogSync.periodic);
        DatabaseDescriptor.setCommitLogSyncPeriod(100);
        CQLTester.setUpClass();
    }

    @Before
    public void before() throws Throwable
    {
        createTable("CREATE TABLE %s(" +
                    "  key int," +
                    "  column int," +
                    "  data int," +
                    "  extra text," +
                    "  PRIMARY KEY(key, column)" +
                    ")"
                   );

    }
    AtomicLong id = new AtomicLong();
    long compactionTimeNanos = 0;

    void pushData(Random rand, int count) throws Throwable
    {
        for (int i = 0; i < count; ++i)
        {
            long ii = id.incrementAndGet();
            if (ii % 1000 == 0)
                System.out.print('.');
            int key = rand.nextInt(KEY_RANGE);
            int column = rand.nextInt(CLUSTERING_RANGE);
            execute("INSERT INTO %s (key, column, data, extra) VALUES (?, ?, ?, ?)", key, column, (int) ii, genExtra(rand));
            maybeCompact(ii);
        }
    }

    private String genExtra(Random rand)
    {
        StringBuilder builder = new StringBuilder(EXTRA_SIZE);
        for (int i = 0; i < EXTRA_SIZE; ++i)
            builder.append((char) ('a' + rand.nextInt('z' - 'a' + 1)));
        return builder.toString();
    }
    
    void deleteData(Random rand, int count) throws Throwable
    {
        for (int i = 0; i < count; ++i)
        {
            int key;
            UntypedResultSet res;
            long ii = id.incrementAndGet();
            if (ii % 1000 == 0)
                System.out.print('-');
            if (rand.nextInt(RANGE_FREQUENCY_INV) != 1)
            {
                do
                {
                    key = rand.nextInt(KEY_RANGE);
                    long cid = rand.nextInt(DEL_SECTIONS);
                    int cstart = (int) (cid * CLUSTERING_RANGE / DEL_SECTIONS);
                    int cend = (int) ((cid + 1) * CLUSTERING_RANGE / DEL_SECTIONS);
                    res = execute("SELECT column FROM %s WHERE key = ? AND column >= ? AND column < ?", key, cstart, cend);
                } while (res.size() == 0);
                UntypedResultSet.Row r = Iterables.get(res, rand.nextInt(res.size()));
                int clustering = r.getInt("column");
                execute("DELETE FROM %s WHERE key = ? AND column = ?", key, clustering);
            }
            else
            {
                key = rand.nextInt(KEY_RANGE);
                long cid = rand.nextInt(DEL_SECTIONS);
                int cstart = (int) (cid * CLUSTERING_RANGE / DEL_SECTIONS);
                int cend = (int) ((cid + 1) * CLUSTERING_RANGE / DEL_SECTIONS);
                res = execute("DELETE FROM %s WHERE key = ? AND column >= ? AND column < ?", key, cstart, cend);
            }
            maybeCompact(ii);
        }
    }

    private void maybeCompact(long ii)
    {
        if (ii % 10000 == 0)
        {
            System.out.print("F");
            flush();
            if (ii % 100000 == 0)
            {
                System.out.print("C");
                long startTime = System.nanoTime();
                getCurrentColumnFamilyStore().enableAutoCompaction(true);
                long endTime = System.nanoTime();
                compactionTimeNanos += endTime - startTime;
                getCurrentColumnFamilyStore().disableAutoCompaction();
            }
        }
    }

    public void testGcCompaction(boolean doGC, boolean cull, String compactionClass) throws Throwable
    {
        id.set(0);
        compactionTimeNanos = 0;
        alterTable("ALTER TABLE %s WITH compaction = { 'class' :  '" + compactionClass + "'  };");
        boolean doOngoingGC = doGC;
        CompactionController.doGC = doOngoingGC;
        CompactionController.cull = cull;
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        cfs.disableAutoCompaction();

        long onStartTime = System.currentTimeMillis();
        ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<?>> tasks = new ArrayList<>();
        for (int ti = 0; ti < 1; ++ti)
        {
            Random rand = new Random(ti);
            tasks.add(es.submit(() -> 
            {
                for (int i = 0; i < 10; ++i)
                    try
                    {
                        pushData(rand, COUNT);
                        deleteData(rand, COUNT / 3);
                    }
                    catch (Throwable e)
                    {
                        throw new AssertionError(e);
                    }
            }));
        }
        for (Future<?> task : tasks)
            task.get();

        flush();
        long onEndTime = System.currentTimeMillis();
        int startRowCount = countRows(cfs);
        int startTombCount = countTombstoneMarkers(cfs);
        int startRowDeletions = countRowDeletions(cfs);
        int startTableCount = cfs.getLiveSSTables().size();
        long startSize = SSTableReader.getTotalBytes(cfs.getLiveSSTables());
        cfs.snapshot("Before");
        System.out.println();

        CompactionController.doGC = doGC;
        CompactionController.cull = cull;
        long startTime = System.currentTimeMillis();
        for (SSTableReader reader : cfs.getLiveSSTables())
            CompactionManager.instance.forceUserDefinedCompaction(reader.getFilename());
        long endTime = System.currentTimeMillis();

        cfs.snapshot("After");

        int endRowCount = countRows(cfs);
        int endTombCount = countTombstoneMarkers(cfs);
        int endRowDeletions = countRowDeletions(cfs);
        int endTableCount = cfs.getLiveSSTables().size();
        long endSize = SSTableReader.getTotalBytes(cfs.getLiveSSTables());

        System.out.println(cfs.getCompactionParametersJson());
        System.out.println(String.format("%s compactions completed in %.3fs",
                doGC ? cull ? "GC" : "NoCullGC" : "Copy", (endTime - startTime) * 1e-3));
        System.out.println(String.format("Operations completed in %.3fs, out of which %.3f for ongoing %sbackground compactions",
                (onEndTime - onStartTime) * 1e-3, compactionTimeNanos * 1e-9, doOngoingGC ? cull ? "GC " : "NoCullGC " : ""));
        System.out.println(String.format("At start: %12d tables %12d bytes %12d rows %12d deleted rows %12d tombstone markers",
                startTableCount, startSize, startRowCount, startRowDeletions, startTombCount));
        System.out.println(String.format("At end:   %12d tables %12d bytes %12d rows %12d deleted rows %12d tombstone markers",
                endTableCount, endSize, endRowCount, endRowDeletions, endTombCount));
    }

    @Test
    public void testGcCompaction() throws Throwable
    {
        testGcCompaction(true, true, "LeveledCompactionStrategy");
    }

    @Test
    public void testNoCullGcCompaction() throws Throwable
    {
        testGcCompaction(true, false, "LeveledCompactionStrategy");
    }

    @Test
    public void testCopyCompaction() throws Throwable
    {
        testGcCompaction(false, false, "LeveledCompactionStrategy");
    }

    @Test
    public void testGcCompactionSizeTiered() throws Throwable
    {
        testGcCompaction(true, true, "SizeTieredCompactionStrategy");
    }

    @Test
    public void testNoCullCompactionSizeTiered() throws Throwable
    {
        testGcCompaction(true, false, "SizeTieredCompactionStrategy");
    }

    @Test
    public void testCopyCompactionSizeTiered() throws Throwable
    {
        testGcCompaction(false, false, "SizeTieredCompactionStrategy");
    }

    int countTombstoneMarkers(ColumnFamilyStore cfs)
    {
        int nowInSec = FBUtilities.nowInSeconds();
        return count(cfs, x -> x.isRangeTombstoneMarker());
    }

    int countRowDeletions(ColumnFamilyStore cfs)
    {
        int nowInSec = FBUtilities.nowInSeconds();
        return count(cfs, x -> x.isRow() && !((Row) x).deletion().isLive());
    }

    int countRows(ColumnFamilyStore cfs)
    {
        int nowInSec = FBUtilities.nowInSeconds();
        return count(cfs, x -> x.isRow() && ((Row) x).hasLiveData(nowInSec));
    }

    private int count(ColumnFamilyStore cfs, Predicate<Unfiltered> predicate)
    {
        int count = 0;
        for (SSTableReader reader : cfs.getLiveSSTables())
            count += count(reader, predicate);
        return count;
    }

    int count(SSTableReader reader, Predicate<Unfiltered> predicate)
    {
        int instances = 0;
        try (ISSTableScanner partitions = reader.getScanner())
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator iter = partitions.next())
                {
                    while (iter.hasNext())
                    {
                        Unfiltered atom = iter.next();
                        if (predicate.test(atom))
                            ++instances;
                    }
                }
            }
        }
        return instances;
    }
}