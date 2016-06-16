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
package org.apache.cassandra.hints;

import java.net.InetAddress;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MockMessagingService;
import org.apache.cassandra.net.MockMessagingSpy;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.Util.dk;
import static org.apache.cassandra.net.MockMessagingService.verb;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HintsServiceTest
{
    private static final String KEYSPACE = "hints_service_test";
    private static final String TABLE = "table";

    private final MockFailureDetector failureDetector = new MockFailureDetector();

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        StorageService.instance.initServer();
        SchemaLoader.createKeyspace(KEYSPACE,
                KeyspaceParams.simple(1),
                SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    @Before
    public void reinstanciateService() throws ExecutionException, InterruptedException
    {
        MessagingService.instance().clearMessageSinks();

        if (!HintsService.instance.isShutDown())
        {
            HintsService.instance.shutdownBlocking();
            HintsService.instance.deleteAllHints();
        }

        failureDetector.isAlive = true;
        HintsService.instance = new HintsService(failureDetector);
        HintsService.instance.startDispatch();
    }

    @Test
    public void testDispatchHints() throws InterruptedException
    {
        // create spy for hint messages
        MockMessagingSpy spy = sendHintsAndResponses(100, -1);

        // metrics should have been updated with number of create hints
        assertEquals(100, StorageMetrics.totalHints.getCount());

        // wait until hints have been send
        spy.receiveN(100).receiveNoMsg(500, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testPauseAndResume() throws InterruptedException
    {
        HintsService.instance.pauseDispatch();

        // create spy for hint messages
        MockMessagingSpy spy = sendHintsAndResponses(100, -1);

        // we should not send any hints while paused
        spy.receiveNoMsg(15, TimeUnit.SECONDS);

        // resume and wait until hints have been send
        HintsService.instance.resumeDispatch();
        spy.receiveN(100).receiveNoMsg(500, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testPageRetry() throws InterruptedException
    {
        // create spy for hint messages, but only create responses for 5 hints
        MockMessagingSpy spy = sendHintsAndResponses(20, 5);

        // the dispatcher will always send all hints within the current page
        // and only wait for the acks before going to the next page
        spy.receiveN(20).receiveNoMsg(1, TimeUnit.SECONDS);

        // next tick will trigger a retry of the same page as we only replied with 5/20 acks
        spy.receiveN(20, 20, TimeUnit.SECONDS).receiveNoMsg(1, TimeUnit.SECONDS);

        // retry would normally go on each tick until the failure detector marks the node as dead
        failureDetector.isAlive = false;
        spy.receiveNoMsg(20, TimeUnit.SECONDS);
    }

    @Test
    public void testPageSeek() throws InterruptedException
    {
        // create spy for hint messages, stop replying after 12k (should be on 3rd page)
        MockMessagingSpy spy = sendHintsAndResponses(20000, 12000);

        // At this point the dispatcher will constantly retry the page we stopped acking,
        // thus we receive the same hints from the page multiple times and in total more than
        // all written hints. Lets just consume them for a while and then pause the dispatcher.
        spy.receiveN(22000);
        HintsService.instance.pauseDispatch();
        Thread.sleep(1000);

        // verify that we have a dispatch offset set for the page we're currently stuck at
        HintsStore store = HintsService.instance.getCatalog().get(StorageService.instance.getLocalHostUUID());
        Optional<Long> dispatchOffset = store.getDispatchOffset(store.poll());
        assertTrue(dispatchOffset.isPresent());
        assertTrue(dispatchOffset.get() > 0);
    }

    private MockMessagingSpy sendHintsAndResponses(int noOfHints, int noOfResponses)
    {
        // create spy for hint messages, but only create responses for noOfResponses hints
        MessageIn<HintResponse> messageIn = MessageIn.create(FBUtilities.getBroadcastAddress(),
                HintResponse.instance,
                Collections.emptyMap(),
                MessagingService.Verb.REQUEST_RESPONSE,
                MessagingService.current_version,
                MessageIn.createTimestamp());

        MockMessagingSpy spy;
        if (noOfResponses != -1)
        {
            spy = MockMessagingService.when(verb(MessagingService.Verb.HINT)).respondN(messageIn, noOfResponses);
        }
        else
        {
            spy = MockMessagingService.when(verb(MessagingService.Verb.HINT)).respond(messageIn);
        }

        // create and write noOfHints using service
        UUID hostId = StorageService.instance.getLocalHostUUID();
        for (int i = 0; i < noOfHints; i++)
        {
            long now = System.currentTimeMillis();
            Mutation mutation = new Mutation(KEYSPACE, dk(String.valueOf(i)));
            new RowUpdateBuilder(Schema.instance.getCFMetaData(KEYSPACE, TABLE), now, mutation)
                    .clustering("column0")
                    .add("val", "value0")
                    .build();
            Hint hint = Hint.create(mutation, now);
            HintsService.instance.write(hostId, hint);
        }
        return spy;
    }

    private static class MockFailureDetector implements IFailureDetector
    {
        private boolean isAlive = true;

        public boolean isAlive(InetAddress ep)
        {
            return isAlive;
        }

        public void interpret(InetAddress ep)
        {
            throw new UnsupportedOperationException();
        }

        public void report(InetAddress ep)
        {
            throw new UnsupportedOperationException();
        }

        public void registerFailureDetectionEventListener(IFailureDetectionEventListener listener)
        {
            throw new UnsupportedOperationException();
        }

        public void unregisterFailureDetectionEventListener(IFailureDetectionEventListener listener)
        {
            throw new UnsupportedOperationException();
        }

        public void remove(InetAddress ep)
        {
            throw new UnsupportedOperationException();
        }

        public void forceConviction(InetAddress ep)
        {
            throw new UnsupportedOperationException();
        }
    }
}
