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
package org.apache.cassandra.db;

import java.io.DataInputStream;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.*;
import com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.concurrent.DebuggableScheduledThreadPoolExecutor;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.WriteResponseHandler;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.WrappedRunnable;

import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternalWithPaging;

public class BatchlogManager implements BatchlogManagerMBean
{
    private static final String MBEAN_NAME = "org.apache.cassandra.db:type=BatchlogManager";
    private static final long REPLAY_INTERVAL = 60 * 1000; // milliseconds
    private static final int DEFAULT_PAGE_SIZE = 128;

    private static final Logger logger = LoggerFactory.getLogger(BatchlogManager.class);
    public static final BatchlogManager instance = new BatchlogManager();

    private long totalBatchesReplayed = 0; // no concurrency protection necessary as only written by replay thread.
    private UUID lastReplayedUuid = UUIDGen.minTimeUUID(0);

    private Runnable replayBatchlogRunnable = new WrappedRunnable()
    {
        public void runMayThrow() throws ExecutionException, InterruptedException
        {
            replayAllFailedBatches();
        }
    };

    // Single-thread executor service for scheduling and serializing log replay.
    public static final ScheduledExecutorService batchlogTasks = new DebuggableScheduledThreadPoolExecutor("BatchlogTasks");

    public void start()
    {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        try
        {
            mbs.registerMBean(this, new ObjectName(MBEAN_NAME));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }

        batchlogTasks.schedule(getInitialReplayRunnable(), StorageService.RING_DELAY, TimeUnit.MILLISECONDS);

        batchlogTasks.scheduleWithFixedDelay(replayBatchlogRunnable,
                                             StorageService.RING_DELAY + REPLAY_INTERVAL,
                                             REPLAY_INTERVAL,
                                             TimeUnit.MILLISECONDS);
    }

    private WrappedRunnable getInitialReplayRunnable()
    {
        return new WrappedRunnable() {
            @Override
            public void runMayThrow() throws ExecutionException, InterruptedException
            {
                // Initial run must take care of non-time-uuid batches as written by Version 1.2.
                convertOldBatchEntries();

                replayAllFailedBatches();
            }
        };
    }

    public int countAllBatches()
    {
        String query = String.format("SELECT count(*) FROM %s.%s", Keyspace.SYSTEM_KS, SystemKeyspace.BATCHLOG_CF);
        UntypedResultSet results = executeInternal(query);
        if (results.isEmpty())
            return 0;
        return (int) results.one().getLong("count");
    }

    public long getTotalBatchesReplayed()
    {
        return totalBatchesReplayed;
    }

    public void forceBatchlogReplay()
    {
        startBatchlogReplay();
    }

    public Future<?> startBatchlogReplay()
    {
        // If a replay is already in progress this request will be executed after it completes.
        return batchlogTasks.submit(replayBatchlogRunnable);
    }

    void performInitialReplay() throws InterruptedException, ExecutionException
    {
        // Invokes initial replay. Used for testing only.
        batchlogTasks.submit(getInitialReplayRunnable()).get();
    }

    public long getBatchlogTimeout()
    {
    // enough time for the actual write + batchlog entry mutation delivery (two separate requests).
        return DatabaseDescriptor.getWriteRpcTimeout() * 2;
    }

    public static Mutation getBatchlogMutationFor(Collection<Mutation> mutations, UUID uuid, int version)
    {
        ColumnFamily cf = ArrayBackedSortedColumns.factory.create(CFMetaData.BatchlogCf);
        CFRowAdder adder = new CFRowAdder(cf, CFMetaData.BatchlogCf.comparator.builder().build(), UUIDGen.unixTimestamp(uuid) * 1000);
        adder.add("data", serializeMutations(mutations, version))
             .add("version", version);
        return new Mutation(Keyspace.SYSTEM_KS, UUIDType.instance.decompose(uuid), cf);
    }

    @VisibleForTesting
    static ByteBuffer serializeMutations(Collection<Mutation> mutations, int version)
    {
        DataOutputBuffer buf = new DataOutputBuffer();

        try
        {
            buf.writeInt(mutations.size());
            for (Mutation mutation : mutations)
                Mutation.serializer.serialize(mutation, buf, version);
        }
        catch (IOException e)
        {
            throw new AssertionError(); // cannot happen.
        }

        return buf.asByteBuffer();
    }

    private void replayAllFailedBatches() throws ExecutionException, InterruptedException
    {
        logger.debug("Started replayAllFailedBatches");

        // rate limit is in bytes per second. Uses Double.MAX_VALUE if disabled (set to 0 in cassandra.yaml).
        // max rate is scaled by the number of nodes in the cluster (same as for HHOM - see CASSANDRA-5272).
        int throttleInKB = DatabaseDescriptor.getBatchlogReplayThrottleInKB() / StorageService.instance.getTokenMetadata().getAllEndpoints().size();
        RateLimiter rateLimiter = RateLimiter.create(throttleInKB == 0 ? Double.MAX_VALUE : throttleInKB * 1024);

        UUID limitUuid = UUIDGen.maxTimeUUID(System.currentTimeMillis() - getBatchlogTimeout());

        int pageSize = calculatePageSize();
        String query = String.format("SELECT id, data, version FROM %s.%s WHERE token(id) > token(?) AND token(id) <= token(?)",
                                     Keyspace.SYSTEM_KS,
                                     SystemKeyspace.BATCHLOG_CF);
        UntypedResultSet batches = executeInternalWithPaging(query, pageSize, lastReplayedUuid, limitUuid);
        processBatchlogEntries(batches, pageSize, rateLimiter);
        lastReplayedUuid = limitUuid;
        logger.debug("Finished replayAllFailedBatches");
    }

    // read less rows (batches) per page if they are very large
    private int calculatePageSize()
    {
        ColumnFamilyStore hintStore = SystemKeyspace.schemaCFS(SystemKeyspace.BATCHLOG_CF);
        long averageRowSize = hintStore.getMeanRowSize();
        if (averageRowSize <= 0)
            return DEFAULT_PAGE_SIZE;

        return (int) Math.max(1, Math.min(DEFAULT_PAGE_SIZE, 4 * 1024 * 1024 / averageRowSize));
    }

    private void deleteBatch(UUID id)
    {
        Mutation mutation = new Mutation(Keyspace.SYSTEM_KS, UUIDType.instance.decompose(id));
        mutation.delete(SystemKeyspace.BATCHLOG_CF, FBUtilities.timestampMicros());
        mutation.apply();
    }

    private void processBatchlogEntries(UntypedResultSet batches, int pageSize, RateLimiter rateLimiter)
    {
        int positionInPage = 0;
        ArrayList<Batch> unfinishedBatches = new ArrayList<>(pageSize);

        // Sending out batches for replay without waiting for them, so that one stuck batch doesn't affect others
        for (UntypedResultSet.Row row : batches)
        {
            UUID id = row.getUUID("id");

            int version = row.getInt("version");
            Batch batch = new Batch(id, row.getBytes("data"), version);
            try
            {
                if (batch.replay(rateLimiter) > 0)
                {
                    unfinishedBatches.add(batch);
                }
                else
                {
                    deleteBatch(id); // no write mutations were sent (either expired or all CFs involved truncated).
                    ++totalBatchesReplayed;
                }
            }
            catch (IOException e)
            {
                logger.warn("Skipped batch replay of {} due to {}", id, e);
                deleteBatch(id);
            }

            if (++positionInPage == pageSize) {
                // We have reached the end of a batch. To avoid keeping more than a page of mutations in memory,
                // finish processing the page before requesting the next row.
                finishAndClearBatches(unfinishedBatches);
                positionInPage = 0;
            }
        }
        finishAndClearBatches(unfinishedBatches);
    }

    private void finishAndClearBatches(ArrayList<Batch> batches)
    {
        // schedule hints for timed out deliveries
        for (Batch batch : batches)
        {
            batch.finish();
            deleteBatch(batch.id);
        }
        totalBatchesReplayed += batches.size();
        batches.clear();
    }

    private static class Batch
    {
        private final UUID id;
        private final long writtenAt;
        private final ByteBuffer data;
        private final int version;

        private List<ReplayWriteResponseHandler> replayHandlers;

        public Batch(UUID id, ByteBuffer data, int version)
        {
            this.id = id;
            this.writtenAt = UUIDGen.unixTimestamp(id);
            this.data = data;
            this.version = version;
        }

        public int replay(RateLimiter rateLimiter) throws IOException
        {
            logger.debug("Replaying batch {}", id);

            List<Mutation> mutations = replayingMutations();

            if (mutations.isEmpty())
                return 0;

            int ttl = calculateHintTTL(mutations);
            if (ttl <= 0)
                return 0;

            replayHandlers = sendReplays(mutations, writtenAt, ttl);

            rateLimiter.acquire(data.remaining()); // acquire afterwards, to not mess up ttl calculation.

            return replayHandlers.size();
        }

        public void finish()
        {
            for (int i = 0; i < replayHandlers.size(); i++)
            {
                ReplayWriteResponseHandler handler = replayHandlers.get(i);
                try
                {
                    handler.get();
                }
                catch (WriteTimeoutException e)
                {
                    logger.debug("Timed out replaying a batched mutation to a node, will write a hint");
                    // writing hints for the rest to hints, starting from i
                    writeHintsForUndeliveredEndpoints(i);
                    return;
                }
            }
        }

        private List<Mutation> replayingMutations() throws IOException
        {
            DataInputStream in = new DataInputStream(ByteBufferUtil.inputStream(data));
            int size = in.readInt();
            List<Mutation> mutations = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
            {
                Mutation mutation = Mutation.serializer.deserialize(in, version);

                // Remove CFs that have been truncated since. writtenAt and SystemTable#getTruncatedAt() both return millis.
                // We don't abort the replay entirely b/c this can be considered a success (truncated is same as delivered then
                // truncated.
                for (UUID cfId : mutation.getColumnFamilyIds())
                    if (writtenAt <= SystemKeyspace.getTruncatedAt(cfId))
                        mutation = mutation.without(cfId);

                if (!mutation.isEmpty())
                    mutations.add(mutation);
            }
            return mutations;
        }

        private void writeHintsForUndeliveredEndpoints(int startFrom)
        {
            try
            {
                // Here we deserialize mutations 2nd time from byte buffer.
                // but this is ok, because timeout on batch direct delivery is rare
                // (it can happen only several seconds until node is marked dead)
                // so trading some cpu to keep less objects
                List<Mutation> replayingMutations = replayingMutations();
                for (int i = startFrom; i < replayHandlers.size(); i++)
                {
                    Mutation undeliveredMutation = replayingMutations.get(i);
                    int ttl = calculateHintTTL(replayingMutations);
                    ReplayWriteResponseHandler handler = replayHandlers.get(i);

                    if (ttl > 0 && handler != null)
                        for (InetAddress endpoint : handler.undelivered)
                            StorageProxy.writeHintForMutation(undeliveredMutation, writtenAt, ttl, endpoint);
                }
            }
            catch (IOException e)
            {
                logger.error("Cannot schedule hints for undelivered batch", e);
            }
        }

        private List<ReplayWriteResponseHandler> sendReplays(List<Mutation> mutations, long writtenAt, int ttl)
        {
            List<ReplayWriteResponseHandler> handlers = new ArrayList<>(mutations.size());
            for (Mutation mutation : mutations)
            {
                ReplayWriteResponseHandler handler = sendSingleReplayMutation(mutation, writtenAt, ttl);
                if (handler != null)
                    handlers.add(handler);
            }
            return handlers;
        }

        /**
         * We try to deliver the mutations to the replicas ourselves if they are alive and only resort to writing hints
         * when a replica is down or a write request times out.
         *
         * @return direct delivery handler to wait on or null, if no live nodes found
         */
        private ReplayWriteResponseHandler sendSingleReplayMutation(final Mutation mutation, long writtenAt, int ttl)
        {
            Set<InetAddress> liveEndpoints = new HashSet<>();
            String ks = mutation.getKeyspaceName();
            Token<?> tk = StorageService.getPartitioner().getToken(mutation.key());

            for (InetAddress endpoint : Iterables.concat(StorageService.instance.getNaturalEndpoints(ks, tk),
                                                         StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, ks)))
            {
                if (endpoint.equals(FBUtilities.getBroadcastAddress()))
                    mutation.apply();
                else if (FailureDetector.instance.isAlive(endpoint))
                    liveEndpoints.add(endpoint); // will try delivering directly instead of writing a hint.
                else
                    StorageProxy.writeHintForMutation(mutation, writtenAt, ttl, endpoint);
            }

            if (liveEndpoints.isEmpty())
                return null;

            ReplayWriteResponseHandler handler = new ReplayWriteResponseHandler(liveEndpoints);
            MessageOut<Mutation> message = mutation.createMessage();
            for (InetAddress endpoint : liveEndpoints)
                MessagingService.instance().sendRR(message, endpoint, handler, false);
            return handler;
        }

        /*
         * Calculate ttl for the mutations' hints (and reduce ttl by the time the mutations spent in the batchlog).
         * This ensures that deletes aren't "undone" by an old batch replay.
         */
        private int calculateHintTTL(Collection<Mutation> mutations)
        {
            int unadjustedTTL = Integer.MAX_VALUE;
            for (Mutation mutation : mutations)
                unadjustedTTL = Math.min(unadjustedTTL, HintedHandOffManager.calculateHintTTL(mutation));
            return unadjustedTTL - (int) TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - writtenAt);
        }

        private static class ReplayWriteResponseHandler extends WriteResponseHandler
        {
            private final Set<InetAddress> undelivered = Collections.newSetFromMap(new ConcurrentHashMap<InetAddress, Boolean>());

            public ReplayWriteResponseHandler(Collection<InetAddress> writeEndpoints)
            {
                super(writeEndpoints, Collections.<InetAddress>emptySet(), null, null, null, WriteType.UNLOGGED_BATCH);
                undelivered.addAll(writeEndpoints);
            }

            @Override
            protected int totalBlockFor()
            {
                return this.naturalEndpoints.size();
            }

            @Override
            public void response(MessageIn m)
            {
                boolean removed = undelivered.remove(m.from);
                assert removed;
                super.response(m);
            }
        }
    }

    private void convertOldBatchEntries() throws ExecutionException, InterruptedException
    {
        logger.debug("Started convertOldBatchEntries");

        int pageSize = calculatePageSize();
        // Random UUIDs compare greater than time UUIDs. Looking for anything greater than now should give us only them.
        String query = String.format("SELECT id, data, written_at, version FROM %s.%s WHERE token(id) > token(?)",
                                     Keyspace.SYSTEM_KS,
                                     SystemKeyspace.BATCHLOG_CF);
        UntypedResultSet batches = executeInternalWithPaging(query, pageSize, UUIDGen.maxTimeUUID(System.currentTimeMillis()));
        int convertedBatches = 0;
        for (UntypedResultSet.Row row : batches)
        {
            UUID id = row.getUUID("id");
            if (id.version() != 1) // not a time-based uuid
            {
                long timestamp = row.getLong("written_at");
                int version = row.has("version") ? row.getInt("version") : MessagingService.VERSION_12;
                ColumnFamily cf = ArrayBackedSortedColumns.factory.create(CFMetaData.BatchlogCf);
                CFRowAdder adder = new CFRowAdder(cf, CFMetaData.BatchlogCf.comparator.builder().build(), FBUtilities.timestampMicros());
                adder.add("data", row.getBytes("data"))
                     .add("version", version);
                Mutation addRow = new Mutation(Keyspace.SYSTEM_KS,
                                               ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes(timestamp, convertedBatches++)),
                                               cf);
                deleteBatch(id);
                addRow.apply();
            } else {
                logger.warn("skipping " + id);
            }
        }
        if (convertedBatches > 0)
            cleanup();
        logger.debug("Finished convertOldBatchEntries");
    }

    // force flush + compaction to reclaim space from the replayed batches
    private void cleanup() throws ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = Keyspace.open(Keyspace.SYSTEM_KS).getColumnFamilyStore(SystemKeyspace.BATCHLOG_CF);
        cfs.forceBlockingFlush();
        Collection<Descriptor> descriptors = new ArrayList<>();
        for (SSTableReader sstr : cfs.getSSTables())
            descriptors.add(sstr.descriptor);
        if (!descriptors.isEmpty()) // don't pollute the logs if there is nothing to compact.
            CompactionManager.instance.submitUserDefined(cfs, descriptors, Integer.MAX_VALUE).get();
    }

    public static class EndpointFilter
    {
        private final String localRack;
        private final Multimap<String, InetAddress> endpoints;

        public EndpointFilter(String localRack, Multimap<String, InetAddress> endpoints)
        {
            this.localRack = localRack;
            this.endpoints = endpoints;
        }

        /**
         * @return list of candidates for batchlog hosting. If possible these will be two nodes from different racks.
         */
        public Collection<InetAddress> filter()
        {
            // special case for single-node data centers
            if (endpoints.values().size() == 1)
                return endpoints.values();

            // strip out dead endpoints and localhost
            ListMultimap<String, InetAddress> validated = ArrayListMultimap.create();
            for (Map.Entry<String, InetAddress> entry : endpoints.entries())
                if (isValid(entry.getValue()))
                    validated.put(entry.getKey(), entry.getValue());

            if (validated.size() <= 2)
                return validated.values();

            if (validated.size() - validated.get(localRack).size() >= 2)
            {
                // we have enough endpoints in other racks
                validated.removeAll(localRack);
            }

            if (validated.keySet().size() == 1)
            {
                // we have only 1 `other` rack
                Collection<InetAddress> otherRack = Iterables.getOnlyElement(validated.asMap().values());
                return Lists.newArrayList(Iterables.limit(otherRack, 2));
            }

            // randomize which racks we pick from if more than 2 remaining
            Collection<String> racks;
            if (validated.keySet().size() == 2)
            {
                racks = validated.keySet();
            }
            else
            {
                racks = Lists.newArrayList(validated.keySet());
                Collections.shuffle((List<String>) racks);
            }

            // grab a random member of up to two racks
            List<InetAddress> result = new ArrayList<>(2);
            for (String rack : Iterables.limit(racks, 2))
            {
                List<InetAddress> rackMembers = validated.get(rack);
                result.add(rackMembers.get(getRandomInt(rackMembers.size())));
            }

            return result;
        }

        @VisibleForTesting
        protected boolean isValid(InetAddress input)
        {
            return !input.equals(FBUtilities.getBroadcastAddress()) && FailureDetector.instance.isAlive(input);
        }

        @VisibleForTesting
        protected int getRandomInt(int bound)
        {
            return ThreadLocalRandom.current().nextInt(bound);
        }
    }
}
