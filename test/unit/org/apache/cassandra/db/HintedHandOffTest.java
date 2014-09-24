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
package org.apache.cassandra.db;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy;
import org.apache.cassandra.db.composites.Composites;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.BootStrapper;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;

public class HintedHandOffTest extends SchemaLoader
{

    public static final String KEYSPACE4 = "Keyspace4";
    public static final String STANDARD1_CF = "Standard1";
    public static final String COLUMN1 = "column1";

    // Test compaction of hints column family. It shouldn't remove all columns on compaction.
    @Test
    public void testCompactionOfHintsCF() throws Exception
    {
        // prepare hints column family
        Keyspace systemKeyspace = Keyspace.open("system");
        ColumnFamilyStore hintStore = systemKeyspace.getColumnFamilyStore(SystemKeyspace.HINTS_CF);
        hintStore.clearUnsafe();
        hintStore.metadata.gcGraceSeconds(36000); // 10 hours
        hintStore.setCompactionStrategyClass(SizeTieredCompactionStrategy.class.getCanonicalName());
        hintStore.disableAutoCompaction();

        // insert 1 hint
        Mutation rm = new Mutation(KEYSPACE4, ByteBufferUtil.bytes(1));
        rm.add(STANDARD1_CF, Util.cellname(COLUMN1), ByteBufferUtil.EMPTY_BYTE_BUFFER, System.currentTimeMillis());

        HintedHandOffManager.instance.hintFor(rm,
                                              System.currentTimeMillis(),
                                              HintedHandOffManager.calculateHintTTL(rm),
                                              UUID.randomUUID())
                                     .apply();

        // flush data to disk
        hintStore.forceBlockingFlush();
        assertEquals(1, hintStore.getSSTables().size());

        // submit compaction
        FBUtilities.waitOnFuture(HintedHandOffManager.instance.compact());
        while (CompactionManager.instance.getPendingTasks() > 0 || CompactionManager.instance.getActiveCompactions() > 0)
            TimeUnit.SECONDS.sleep(1);

        // single row should not be removed because of gc_grace_seconds
        // is 10 hours and there are no any tombstones in sstable
        assertEquals(1, hintStore.getSSTables().size());
    }

    @Test
    public void testHintsMetrics() throws Exception
    {
        for (int i = 0; i < 99; i++)
            HintedHandOffManager.instance.metrics.incrPastWindow(InetAddress.getLocalHost());
        HintedHandOffManager.instance.metrics.log();

        UntypedResultSet rows = executeInternal(String.format("SELECT hints_dropped FROM system.%s WHERE peer = ?", SystemKeyspace.PEER_EVENTS_CF),
                                                InetAddress.getLocalHost());
        Map<UUID, Integer> returned = rows.one().getMap("hints_dropped", UUIDType.instance, Int32Type.instance);
        assertEquals(Iterators.getLast(returned.values().iterator()).intValue(), 99);
    }

    @Test(timeout = 5000)
    public void testTruncateHints() throws Exception
    {
        Keyspace systemKeyspace = Keyspace.open("system");
        ColumnFamilyStore hintStore = systemKeyspace.getColumnFamilyStore(SystemKeyspace.HINTS_CF);
        hintStore.clearUnsafe();

        // insert 1 hint
        Mutation rm = new Mutation(KEYSPACE4, ByteBufferUtil.bytes(1));
        rm.add(STANDARD1_CF, Util.cellname(COLUMN1), ByteBufferUtil.EMPTY_BYTE_BUFFER, System.currentTimeMillis());

        HintedHandOffManager.instance.hintFor(rm,
                                              System.currentTimeMillis(),
                                              HintedHandOffManager.calculateHintTTL(rm),
                                              UUID.randomUUID())
                                     .apply();

        assert getNoOfHints() == 1;

        HintedHandOffManager.instance.truncateAllHints();

        while(getNoOfHints() > 0)
        {
            Thread.sleep(100);
        }

        assert getNoOfHints() == 0;
    }

    @Test(timeout = 6000)
    public void testChangedTopology() throws Exception
    {
        Keyspace systemKeyspace = Keyspace.open("system");
        ColumnFamilyStore hintStore = systemKeyspace.getColumnFamilyStore(SystemKeyspace.HINTS_CF);
        hintStore.clearUnsafe();
        Keyspace ks = Keyspace.open(KEYSPACE4);
        ks.createReplicationStrategy(KSMetaData.testMetadata(KEYSPACE4, SimpleStrategy.class, ImmutableMap.of("replication_factor", "1")));

        // Prepare metadata with injected stale endpoint.
        TokenMetadata tokenMeta = StorageService.instance.getTokenMetadata();
        InetAddress local = FBUtilities.getBroadcastAddress();
        tokenMeta.updateNormalTokens(BootStrapper.getRandomTokens(tokenMeta, 1), local);
        InetAddress endpoint = InetAddress.getByName("1.1.1.1");
        UUID targetId = UUID.randomUUID();
        tokenMeta.updateHostId(targetId, endpoint);

        // Inject mock gossip state for endpoint.
        EndpointState localState = Gossiper.instance.getEndpointStateForEndpoint(local);
        Gossiper.instance.initializeNodeUnsafe(endpoint, targetId, 0);
        Gossiper.instance.injectApplicationState(endpoint, ApplicationState.SCHEMA, localState.getApplicationState(ApplicationState.SCHEMA));
        FailureDetector.instance.report(endpoint);

        // insert 1 hint
        Mutation rm = new Mutation(KEYSPACE4, ByteBufferUtil.bytes(1));
        rm.add(STANDARD1_CF, Util.cellname(COLUMN1), ByteBufferUtil.EMPTY_BYTE_BUFFER, System.currentTimeMillis());

        HintedHandOffManager.instance.hintFor(rm,
                                              System.currentTimeMillis(),
                                              HintedHandOffManager.calculateHintTTL(rm),
                                              targetId)
                                     .apply();

        assert getNoOfHints() == 1;

        HintedHandOffManager.instance.scheduleHintDelivery(endpoint);

        while(getNoOfHints() > 0)
        {
            Thread.sleep(100);
        }

        // Should now be delivered to localhost.
        assert getNoOfHints() == 0;
    }

    @Test(timeout = 6000)
    public void testChangedTopologyToHint() throws Exception
    {
        Keyspace systemKeyspace = Keyspace.open("system");
        ColumnFamilyStore hintStore = systemKeyspace.getColumnFamilyStore(SystemKeyspace.HINTS_CF);
        hintStore.clearUnsafe();
        Keyspace ks = Keyspace.open(KEYSPACE4);
        ks.createReplicationStrategy(KSMetaData.testMetadata(KEYSPACE4, SimpleStrategy.class, ImmutableMap.of("replication_factor", "1")));

        // Prepare metadata with injected stale endpoint.
        TokenMetadata tokenMeta = StorageService.instance.getTokenMetadata();
        tokenMeta.clearUnsafe();
        UUID otherId = SystemKeyspace.getLocalHostId();
        InetAddress otherpoint = InetAddress.getByName("1.1.1.2");
        UUID targetId = UUID.randomUUID();
        InetAddress endpoint = InetAddress.getByName("1.1.1.1");
        tokenMeta.updateHostId(targetId, endpoint);
        tokenMeta.updateNormalTokens(BootStrapper.getRandomTokens(tokenMeta, 1), otherpoint);
        tokenMeta.updateHostId(otherId, otherpoint);

        // Inject mock gossip state for endpoint.
        EndpointState localState = Gossiper.instance.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddress());
        Gossiper.instance.initializeNodeUnsafe(endpoint, targetId, 0);
        Gossiper.instance.injectApplicationState(endpoint, ApplicationState.SCHEMA, localState.getApplicationState(ApplicationState.SCHEMA));
        Gossiper.instance.initializeNodeUnsafe(otherpoint, otherId, 0);
        Gossiper.instance.injectApplicationState(otherpoint, ApplicationState.SCHEMA, localState.getApplicationState(ApplicationState.SCHEMA));
        FailureDetector.instance.report(endpoint);

        // insert 1 hint
        Mutation rm = new Mutation(KEYSPACE4, ByteBufferUtil.bytes(1));
        rm.add(STANDARD1_CF, Util.cellname(COLUMN1), ByteBufferUtil.EMPTY_BYTE_BUFFER, System.currentTimeMillis());

        HintedHandOffManager.instance.hintFor(rm,
                                              System.currentTimeMillis(),
                                              HintedHandOffManager.calculateHintTTL(rm),
                                              targetId)
                                     .apply();

        assert getNoOfHints() == 1;

        HintedHandOffManager.instance.scheduleHintDelivery(endpoint);
        
        // Should remove invalid hint and write a new for non-existing replica.

        while(getNoOfHintsForTarget(targetId) == 1)
        {
            assert getNoOfHints() >= 1;
            Thread.sleep(100);
        }
        assert getNoOfHintsForTarget(otherId) == 1;
        assert getNoOfHints() == 1;
        HintedHandOffManager.instance.deleteHintsForEndpoint(otherpoint);
    }

    @Test(timeout = 6000)
    public void testChangedTopologyHintSkipped() throws Exception
    {
        Keyspace systemKeyspace = Keyspace.open("system");
        ColumnFamilyStore hintStore = systemKeyspace.getColumnFamilyStore(SystemKeyspace.HINTS_CF);
        hintStore.clearUnsafe();
        Keyspace ks = Keyspace.open(KEYSPACE4);
        ks.createReplicationStrategy(KSMetaData.testMetadata(KEYSPACE4, SimpleStrategy.class, ImmutableMap.of("replication_factor", "1")));

        // Prepare metadata with injected stale endpoint.
        TokenMetadata tokenMeta = StorageService.instance.getTokenMetadata();
        tokenMeta.clearUnsafe();
        UUID otherId = SystemKeyspace.getLocalHostId();
        InetAddress otherpoint = InetAddress.getByName("1.1.1.2");
        UUID targetId = UUID.randomUUID();
        InetAddress endpoint = InetAddress.getByName("1.1.1.1");
        tokenMeta.updateHostId(targetId, endpoint);
        tokenMeta.updateNormalTokens(BootStrapper.getRandomTokens(tokenMeta, 1), otherpoint);
        tokenMeta.updateHostId(otherId, otherpoint);

        // Inject mock gossip state for endpoint.
        EndpointState localState = Gossiper.instance.getEndpointStateForEndpoint(FBUtilities.getBroadcastAddress());
        Gossiper.instance.initializeNodeUnsafe(endpoint, targetId, 0);
        Gossiper.instance.injectApplicationState(endpoint, ApplicationState.SCHEMA, localState.getApplicationState(ApplicationState.SCHEMA));
        Gossiper.instance.initializeNodeUnsafe(otherpoint, otherId, 0);
        Gossiper.instance.injectApplicationState(otherpoint, ApplicationState.SCHEMA, localState.getApplicationState(ApplicationState.SCHEMA));
        FailureDetector.instance.report(endpoint);
        try
        {
            DatabaseDescriptor.setHintedHandoffEnabled(false);
    
            // insert 1 hint
            Mutation rm = new Mutation(KEYSPACE4, ByteBufferUtil.bytes(1));
            rm.add(STANDARD1_CF, Util.cellname(COLUMN1), ByteBufferUtil.EMPTY_BYTE_BUFFER, System.currentTimeMillis());
    
            HintedHandOffManager.instance.hintFor(rm,
                                                  System.currentTimeMillis(),
                                                  HintedHandOffManager.calculateHintTTL(rm),
                                                  targetId)
                                         .apply();
    
            assert getNoOfHints() == 1;
    
            HintedHandOffManager.instance.scheduleHintDelivery(endpoint);
            
            // Should delete invalid hint as hints are no longer enabled.
    
            while(getNoOfHintsForTarget(targetId) == 1)
            {
                Thread.sleep(100);
            }
            assert getNoOfHintsForTarget(otherId) == 0;
            assert getNoOfHints() == 0;
        }
        finally
        {
            DatabaseDescriptor.setHintedHandoffEnabled(true);
            // Clear unplayed count.
            HintedHandOffManager.instance.metrics.log();
        }
    }

    private int getNoOfHints()
    {
        String req = "SELECT * FROM system.%s";
        UntypedResultSet resultSet = executeInternal(String.format(req, SystemKeyspace.HINTS_CF));
        return resultSet.size();
    }

    private int getNoOfHintsForTarget(UUID hostId)
    {
        final ByteBuffer hostIdBytes = ByteBuffer.wrap(UUIDGen.decompose(hostId));
        DecoratedKey epkey =  StorageService.getPartitioner().decorateKey(hostIdBytes);
        long now = System.currentTimeMillis();
        ColumnFamily hints = Keyspace.open(Keyspace.SYSTEM_KS).getColumnFamilyStore(SystemKeyspace.HINTS_CF)
                .getColumnFamily(epkey, Composites.EMPTY, Composites.EMPTY, false, 10, now);
        hints = ColumnFamilyStore.removeDeleted(hints, (int) (now / 1000));
        return hints == null ? 0 : hints.getColumnCount();
    }
}
