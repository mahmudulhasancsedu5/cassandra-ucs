package org.apache.cassandra.db.commitlog;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.io.FileInputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.JMXEnabledThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.UUIDGen;

import com.google.common.util.concurrent.RateLimiter;

public class ComitLogStress2
{

    public static ByteBuffer dataSource;

    public static int numCells = 1;

    public static int cellSize = 1024;

    public static int rateLimit = 0;

    public static void main(String[] args) throws Exception {
        FileInputStream fis = new FileInputStream("/tmp/alice29.txt");

        dataSource = ByteBuffer.allocateDirect((int)fis.getChannel().size());
        while (dataSource.hasRemaining()) {
            fis.getChannel().read(dataSource);
        }
        dataSource.flip();

        int NUM_THREADS = Runtime.getRuntime().availableProcessors();
        if (args.length >= 1) {
            NUM_THREADS = Integer.parseInt(args[0]);
            System.out.println("Setting num threads to: " + NUM_THREADS);
        }

        if (args.length >= 2) {
            numCells = Integer.parseInt(args[1]);
            System.out.println("Setting num cells to: " + numCells);
        }

        if (args.length >= 3) {
            cellSize = Integer.parseInt(args[1]);
            System.out.println("Setting cell size to: " + cellSize + " be aware the source corpus may be small");
        }

        if (args.length >= 4) {
            rateLimit = Integer.parseInt(args[1]);
            System.out.println("Setting per thread rate limit to: " + rateLimit);
        }

        ScheduledExecutorService scheduled = Executors.newScheduledThreadPool(1);

        org.apache.cassandra.SchemaLoader.loadSchema();
        org.apache.cassandra.SchemaLoader.schemaDefinition(""); // leave def. blank to maintain old behaviour

        final List<CommitlogExecutor> threads = new ArrayList<>();
        for (int ii = 0; ii < NUM_THREADS; ii++) {
            final CommitlogExecutor t = new CommitlogExecutor();
            threads.add(t);
            t.start();
        }

        final long start = System.currentTimeMillis();
        System.out.println(String.format("%s,%s,%s,%s,%s,%s,%s,%s", "seconds", "max_mb", "allocated_mb", "free_mb", "diffrence", "count", "average", "average_mb"));
        scheduled.scheduleAtFixedRate(new Runnable() {
            long lastUpdate = 0;
            final Deque<ReplayPosition> positions = new ArrayDeque<>();

            public void run() {
                Runtime runtime = Runtime.getRuntime();
                long maxMemory = mb(runtime.maxMemory());
                long allocatedMemory = mb(runtime.totalMemory());
                long freeMemory = mb(runtime.freeMemory());
                long temp = 0;
                for (CommitlogExecutor cle : threads) {
                    temp += cle.counter.get();
                }
                double avg = (temp / ((System.currentTimeMillis() - start) / 1000.0));
                System.out.println(String.format("%s,%s,%s,%s,%s,%s,%.3f,%.3f", ((System.currentTimeMillis() - start) / 1000),
                        maxMemory, allocatedMemory, freeMemory, (temp - lastUpdate), lastUpdate, avg, avg * cellSize * numCells / (1<<20)));
                lastUpdate = temp;

                positions.offer(threads.get(0).rp);
                if (positions.size() > 50) {
                    CommitLog.instance.discardCompletedSegments( Schema.instance.getCFMetaData("Keyspace1", "Standard1").cfId, positions.poll());
                }
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    private static long mb(long maxMemory) {
        return maxMemory / (1024 * 1024);
    }

    public static ByteBuffer randomBytes(int quantity, ThreadLocalRandom tlr) {
        ByteBuffer slice = ByteBuffer.allocate(quantity);
        ByteBuffer source = dataSource.duplicate();
        source.position(tlr.nextInt(source.capacity() - quantity));
        source.limit(source.position() + quantity);
        slice.put(source);
        slice.flip();
        return slice;
    }

    public static class CommitlogExecutor extends Thread {
        final AtomicLong counter = new AtomicLong();

        volatile ReplayPosition rp;

        public void run() {
            RateLimiter rl = rateLimit != 0 ? RateLimiter.create(rateLimit) : null;
            final ThreadLocalRandom tlr = ThreadLocalRandom.current();
            while (true) {
                if (rl != null)
                    rl.acquire();
                String ks = "Keyspace1";
                ByteBuffer key = randomBytes(16, tlr);
                Mutation mutation = new Mutation(ks, key);

                for (int ii = 0; ii < numCells; ii++) {
                    mutation.add("Standard1", Util.cellname("name" + ii), randomBytes(cellSize, tlr),
                            System.currentTimeMillis());
                }
                rp = CommitLog.instance.add(mutation);
                counter.incrementAndGet();
            }
        }
    }
}
