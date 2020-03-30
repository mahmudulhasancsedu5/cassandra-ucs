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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.concurrent.SyncFuture;

/**
 * In clusters with many tables and/or vnodes, multiple paxos cleanup operations can saturate the MISC stage, so this
 * class limits the number of concurrent operations sent to a given node
 * we limit the number of sessions
 * we're starting of finishing for a specific node
 * TODO: consider applying this to other MISC stage operations
 */
class PaxosCleanupScheduler
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosCleanupScheduler.class);

    static abstract class Operation<T> extends SyncFuture<T> implements Runnable
    {
        private final Collection<InetAddressAndPort> endpoints;
        private final Executor executor;

        Operation(Collection<InetAddressAndPort> endpoints, Executor executor)
        {
            this.endpoints = endpoints;
            this.executor = executor;
        }

        final Collection<InetAddressAndPort> endpoints()
        {
            return endpoints;
        }

        final Executor executor()
        {
            return executor;
        }

        final void execute()
        {
            try
            {
                executor().execute(this);
            }
            catch (Throwable t)
            {
                logger.error("Exception thrown executing operation:", t);
                tryFailure(t);
            }
        }
    }

    private final List<Operation<?>> queued = new ArrayList<>();
    private final Map<InetAddressAndPort, Set<Operation<?>>> endpointOperations = new HashMap<>();

    private volatile int paralellism;

    PaxosCleanupScheduler(int paralellism)
    {
        this.paralellism = paralellism;
    }

    private boolean canRun(Operation<?> operation)
    {
        if (paralellism < 1)
            return true;

        for (InetAddressAndPort endpoint : operation.endpoints())
        {
            Set<Operation<?>> operations = endpointOperations.computeIfAbsent(endpoint, e -> new HashSet<>());
            if (!operations.contains(operation) && operations.size() >= paralellism)
                return false;
        }
        return true;
    }

    private void markAndSubmit(Operation<?> operation)
    {
        for (InetAddressAndPort endpoint : operation.endpoints())
            endpointOperations.computeIfAbsent(endpoint, e -> new HashSet<>()).add(operation);

        operation.execute();
    }

    private void maybeStartQueued()
    {
        for (int i=0; i<queued.size();)
        {
            Operation<?> operation = queued.get(i);
            if (canRun(operation))
            {
                markAndSubmit(operation);
                queued.remove(i);
            }
            else
            {
                i++;
            }
        }
    }

    private synchronized <T> void onComplete(Operation<T> operation)
    {
        for (InetAddressAndPort endpoint : operation.endpoints())
            endpointOperations.computeIfAbsent(endpoint, e -> new HashSet<>()).remove(operation);

        maybeStartQueued();
    }

    synchronized <T> void schedule(Operation<T> operation)
    {
        operation.addListener(() -> onComplete(operation));

        if (canRun(operation))
        {
            markAndSubmit(operation);
        }
        else
        {
            queued.add(operation);
        }
    }

    synchronized int inFlightForEndpoint(InetAddressAndPort ep)
    {
        Set<Operation<?>> operations = endpointOperations.get(ep);
        return operations != null ? operations.size() : 0;
    }

    public int getParalellism()
    {
        return paralellism;
    }

    public synchronized void setParalellism(int paralellism)
    {
        this.paralellism = paralellism;
        maybeStartQueued();
    }
}
