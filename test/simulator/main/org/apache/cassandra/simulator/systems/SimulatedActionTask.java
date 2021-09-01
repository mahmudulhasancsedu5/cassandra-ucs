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

package org.apache.cassandra.simulator.systems;

import java.util.Map;
import java.util.function.Consumer;

import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor.SerializableRunnable;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.simulator.OrderOn;
import org.apache.cassandra.simulator.systems.InterceptedExecution.InterceptedTaskExecution;

import static org.apache.cassandra.simulator.systems.SimulatedAction.Kind.TASK;

public class SimulatedActionTask extends SimulatedAction
{
    final InterceptedExecution task;

    public SimulatedActionTask(Object description, Modifiers self, Modifiers transitive, SimulatedSystems simulated, InterceptedExecution task)
    {
        this(description, TASK, OrderOn.NONE, self, transitive, null, simulated, task);
    }

    public SimulatedActionTask(Object description, Kind kind, OrderOn orderOn, Modifiers self, Modifiers transitive, Map<Verb, Modifiers> verbModifiers, SimulatedSystems simulated, InterceptedExecution task)
    {
        super(description, kind, orderOn, self, transitive, verbModifiers, simulated);
        this.task = task;
    }

    public SimulatedActionTask(Object description, Modifiers self, Modifiers children, SimulatedSystems simulated, IInvokableInstance on, SerializableRunnable run)
    {
        super(description, self, children, simulated);
        this.task = unsafeAsTask(on, asSafeRunnable(on, run), simulated.failures);
    }

    protected static Runnable asSafeRunnable(IInvokableInstance on, SerializableRunnable run)
    {
        return () -> on.unsafeRunOnThisThread(run);
    }

    protected static InterceptedTaskExecution unsafeAsTask(IInvokableInstance on, Runnable runnable, Consumer<? super Throwable> onFailure)
    {
        return new InterceptedTaskExecution((InterceptingExecutor) on.executor())
        {
            public void run()
            {
                // we'll be invoked on the node's executor, but we need to ensure the task is loaded on its classloader
                try { runnable.run(); }
                catch (Throwable t) { onFailure.accept(t); }
            }
        };
    }

    @Override
    protected InterceptedExecution task()
    {
        return task;
    }
}
