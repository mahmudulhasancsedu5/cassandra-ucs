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

package org.apache.cassandra.distributed.test;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.net.ArtificialLatency;

import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.UNSAFE_DELAY_QUORUM;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.UNSAFE_DELAY_SERIAL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class CASArtificialLatencyTest extends TestBaseImpl
{

    @Test
    public void simpleArtificialLatencyTest() throws Throwable
    {
        System.setProperty("cassandra.artificial_latency_verbs", "PAXOS_PREPARE,PAXOS_PROPOSE,PAXOS_COMMIT,READ");
        System.setProperty("cassandra.artificial_latency_ms", "100");
        try (Cluster cluster = init(Cluster.create(3)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

            long start, end;
            start = System.nanoTime();
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 1) IF NOT EXISTS", UNSAFE_DELAY_SERIAL, UNSAFE_DELAY_QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", UNSAFE_DELAY_SERIAL),
                       row(1, 1, 1));
            end = System.nanoTime();
            Assert.assertTrue(NANOSECONDS.toMillis(end - start) >= 800);
            start = System.nanoTime();
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 3 WHERE pk = 1 and ck = 1 IF v = 2", UNSAFE_DELAY_SERIAL, UNSAFE_DELAY_QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", UNSAFE_DELAY_SERIAL),
                       row(1, 1, 1));
            end = System.nanoTime();
            Assert.assertTrue(NANOSECONDS.toMillis(end - start) >= 600);
            start = System.nanoTime();
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 2 WHERE pk = 1 and ck = 1 IF v = 1", UNSAFE_DELAY_SERIAL, UNSAFE_DELAY_QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", UNSAFE_DELAY_SERIAL),
                       row(1, 1, 2));
            end = System.nanoTime();
            Assert.assertTrue(NANOSECONDS.toMillis(end - start) >= 800);
            cluster.forEach(i -> i.runOnInstance(() -> ArtificialLatency.setEnabled(false)));
            start = System.nanoTime();
            cluster.coordinator(1).execute("UPDATE " + KEYSPACE + ".tbl SET v = 3 WHERE pk = 1 and ck = 1 IF v = 2", UNSAFE_DELAY_SERIAL, UNSAFE_DELAY_QUORUM);
            assertRows(cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl WHERE pk = 1", UNSAFE_DELAY_SERIAL),
                       row(1, 1, 3));
            end = System.nanoTime();
            Assert.assertTrue(NANOSECONDS.toMillis(end - start) < 800);
        }
    }

}
