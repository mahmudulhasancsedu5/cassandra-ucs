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

package org.apache.cassandra.service.paxos.cleanup;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

import io.netty.util.concurrent.ImmediateExecutor;
import org.apache.cassandra.locator.InetAddressAndPort;

public class PaxosCleanupCoordinatorTest
{
    private static final InetAddressAndPort EP1;
    private static final InetAddressAndPort EP2;
    private static final InetAddressAndPort EP3;

    static
    {
        try
        {
            EP1 = InetAddressAndPort.getByAddress(InetAddress.getByName("127.0.0.1"));
            EP2 = InetAddressAndPort.getByAddress(InetAddress.getByName("127.0.0.2"));
            EP3 = InetAddressAndPort.getByAddress(InetAddress.getByName("127.0.0.3"));
        }
        catch (UnknownHostException e)
        {
            throw new AssertionError(e);
        }
    }

    static void assertInFlight(PaxosCleanupScheduler scheduler, int ep1, int ep2, int ep3)
    {
        Assert.assertEquals(ep1, scheduler.inFlightForEndpoint(EP1));
        Assert.assertEquals(ep2, scheduler.inFlightForEndpoint(EP2));
        Assert.assertEquals(ep3, scheduler.inFlightForEndpoint(EP3));
    }

    static class Task extends PaxosCleanupScheduler.Operation<Void>
    {
        boolean executed = false;
        public void run()
        {
            executed = true;
        }


        public Task(Collection<InetAddressAndPort> endpoints)
        {
            super(endpoints, ImmediateExecutor.INSTANCE);
        }

        void finish()
        {
            trySuccess(null);
        }

        void fail()
        {
            tryFailure(new RuntimeException());
        }
    }

    static Task[] tasks(Collection<InetAddressAndPort>... epSets)
    {
        Task[] ts = new Task[epSets.length];
        for (int i=0; i<epSets.length; i++)
            ts[i] = new Task(epSets[i]);
        return ts;
    }

    static Collection<InetAddressAndPort> eps(InetAddressAndPort... endpoints)
    {
        return Sets.newHashSet(endpoints);
    }

    @Test
    public void uncontested()
    {
        Task[] tasks = tasks(eps(EP1, EP2), eps(EP2, EP3), eps(EP1, EP2));
        PaxosCleanupScheduler scheduler = new PaxosCleanupScheduler(2);

        scheduler.schedule(tasks[0]);
        Assert.assertTrue(tasks[0].executed);
        assertInFlight(scheduler, 1, 1, 0);

        scheduler.schedule(tasks[1]);
        Assert.assertTrue(tasks[1].executed);
        assertInFlight(scheduler, 1, 2, 1);

        // should be blocked, since we can't sent ep2 any more tasks
        scheduler.schedule(tasks[2]);
        Assert.assertFalse(tasks[2].executed);
        assertInFlight(scheduler, 1, 2, 1);

        // completing task[0] should cause task[2] to be immediately scheduled
        tasks[0].finish();
        Assert.assertTrue(tasks[2].executed);
        assertInFlight(scheduler, 1, 2, 1);

        // check things empty out properly
        tasks[1].finish();
        assertInFlight(scheduler, 1, 1, 0);

        tasks[2].finish();
        assertInFlight(scheduler, 0, 0, 0);
    }

    // task failures should also free up space
    @Test
    public void failures()
    {
        Task[] tasks = tasks(eps(EP1, EP2));
        PaxosCleanupScheduler scheduler = new PaxosCleanupScheduler(2);

        scheduler.schedule(tasks[0]);
        Assert.assertTrue(tasks[0].executed);
        assertInFlight(scheduler, 1, 1, 0);

        tasks[0].fail();
        assertInFlight(scheduler, 0, 0, 0);
    }
}
