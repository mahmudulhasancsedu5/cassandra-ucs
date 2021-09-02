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

import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.memtable.SkipListMemtable;
import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.utils.FBUtilities;
import org.github.jamm.MemoryMeter;

@RunWith(Parameterized.class)
public class MemtableSizeTest extends CQLTester
{
    // The meter in ObjectSizes uses omitSharedBufferOverhead which counts off-heap data too
    // Note: To see a printout of the usage for each object, add .enableDebug() here (most useful with smaller number of
    // partitions).
    static final MemoryMeter meter = new MemoryMeter().ignoreKnownSingletons()
                                                      .withGuessing(MemoryMeter.Guess.FALLBACK_UNSAFE);

    static String keyspace;
    String table;
    ColumnFamilyStore cfs;

    int partitions = 50_000;
    int rowsPerPartition = 4;

    int deletedPartitions = 10_000;
    int deletedRows = 5_000;

    @Parameterized.Parameter()
    public String memtableClass;

    @Parameterized.Parameters(name = "{0}")
    public static List<Object> parameters()
    {
        return ImmutableList.of("SkipListMemtable",
                                "TrieMemtable");
    }

    // Must be within 3% of the real usage. We are actually more precise than this, but the threshold is set higher to
    // avoid flakes. For on-heap allocators we allow for extra overheads below.
    final int MAX_DIFFERENCE_PERCENT = 3;
    // Slab overhead, added when the memtable uses heap_buffers.
    final int SLAB_OVERHEAD = 1024 * 1024;
    // Extra leniency for unslabbed buffers. We are not as precise there, and it's not a mode in real use.
    final int UNSLABBED_EXTRA_PERCENT = 2;

    @BeforeClass
    public static void setUp()
    {
        CQLTester.setUpClass();
        CQLTester.prepareServer();
        System.err.println("setupClass done.");
    }

    @Test
    public void testSize() throws Throwable
    {
        CQLTester.disablePreparedReuseForTest();
        keyspace = createKeyspace("CREATE KEYSPACE %s with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } and durable_writes = false");
        table = createTable(keyspace, "CREATE TABLE %s ( userid bigint, picid bigint, commentid bigint, PRIMARY KEY(userid, picid))" +
                                  " with compression = {'enabled': false}" +
                                  " and memtable = { 'class': '" + memtableClass + "'}");
        execute("use " + keyspace + ';');

        String writeStatement = "INSERT INTO " + table + "(userid,picid,commentid)VALUES(?,?,?)";
        forcePreparedValues();

        cfs = Keyspace.open(keyspace).getColumnFamilyStore(table);
        cfs.disableAutoCompaction();
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);

        Memtable memtable = cfs.getTracker().getView().getCurrentMemtable();
        long deepSizeBefore = meter.measureDeep(memtable);
        System.out.println("Memtable deep size before " +
                          FBUtilities.prettyPrintMemory(deepSizeBefore));
        long i;
        long limit = partitions;
        System.out.println("Writing " + partitions + " partitions of " + rowsPerPartition + " rows");
        for (i = 0; i < limit; ++i)
        {
            for (long j = 0; j < rowsPerPartition; ++j)
                execute(writeStatement, i, j, i + j);
        }

        System.out.println("Deleting " + deletedPartitions + " partitions");
        limit += deletedPartitions;
        for (; i < limit; ++i)
        {
            // no partition exists, but we will create a tombstone
            execute("DELETE FROM " + table + " WHERE userid = ?", i);
        }

        System.out.println("Deleting " + deletedRows + " rows");
        limit += deletedRows;
        for (; i < limit; ++i)
        {
            // no row exists, but we will create a tombstone (and partition)
            execute("DELETE FROM " + table + " WHERE userid = ? AND picid = ?", i, 0L);
        }

        Assert.assertSame("Memtable flushed during test. Test was not carried out correctly.",
                          memtable,
                          cfs.getTracker().getView().getCurrentMemtable());

        Memtable.MemoryUsage usage = Memtable.getMemoryUsage(memtable);
        long actualHeap = usage.ownsOnHeap;
        System.out.println(String.format("Memtable in %s mode: %d ops, %s serialized bytes, %s",
                                         DatabaseDescriptor.getMemtableAllocationType(),
                                         memtable.getOperations(),
                                         FBUtilities.prettyPrintMemory(memtable.getLiveDataSize()),
                                         usage));

        long deepSizeAfter = meter.measureDeep(memtable);
        System.out.println("Memtable deep size " +
                          FBUtilities.prettyPrintMemory(deepSizeAfter));

        long expectedHeap = deepSizeAfter - deepSizeBefore;
        long max_difference = MAX_DIFFERENCE_PERCENT * expectedHeap / 100;
        long trie_overhead = memtable instanceof TrieMemtable ? ((TrieMemtable)memtable).unusedReservedMemory() : 0;
        switch (DatabaseDescriptor.getMemtableAllocationType())
        {
            case heap_buffers:
                max_difference += SLAB_OVERHEAD;
                actualHeap += trie_overhead;    // adjust trie memory with unused buffer space if on-heap
                break;
            case unslabbed_heap_buffers:
                if (memtable instanceof SkipListMemtable)
                    max_difference += expectedHeap * UNSLABBED_EXTRA_PERCENT / 100;   // We are not as precise for this
                else
                    actualHeap += trie_overhead;    // adjust trie memory with unused buffer space if on-heap
                break;
        }
        String message = String.format("Expected heap usage close to %s, got %s, %s difference.\n",
                                       FBUtilities.prettyPrintMemory(expectedHeap),
                                       FBUtilities.prettyPrintMemory(actualHeap),
                                       FBUtilities.prettyPrintMemory(expectedHeap - actualHeap));
        System.out.println(message);
        Assert.assertTrue(message, Math.abs(actualHeap - expectedHeap) <= max_difference);
    }
}