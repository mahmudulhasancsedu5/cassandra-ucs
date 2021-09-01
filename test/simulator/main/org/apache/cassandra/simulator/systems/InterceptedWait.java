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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.utils.Shared;
import org.apache.cassandra.utils.concurrent.Condition;
import org.apache.cassandra.utils.concurrent.Threads;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.simulator.SimulatorUtils.failWithOOM;
import static org.apache.cassandra.utils.Shared.Recursive.INTERFACES;
import static org.apache.cassandra.utils.Shared.Scope.SIMULATION;

/**
 * A general abstraction for intercepted thread wait events, either
 * generated by the program execution or our nemesis system.
 */
@Shared(scope = SIMULATION, inner = INTERFACES)
public interface InterceptedWait extends NotifyThreadPaused
{
    enum Kind { SLEEP, TIMED_WAIT, UNBOUNDED_WAIT, NEMESIS }

    interface TriggerListener
    {
        /**
         * Invoked when the wait is triggered, permitting any dependent Action to be invalidated.
         * This is particularly useful for thread timeouts, which are often logically invalidated
         * but may otherwise hold up scheduling of further events until their scheduled time passes.
         * @param triggered the wait that has been triggered, and is no longer valid
         */
        void onTrigger(InterceptedWait triggered);
    }

    /**
     * The kind of simulated wait
     */
    Kind kind();

    /**
     * true if the signal has already been triggered by another simulation action
     */
    boolean isTriggered();

    /**
     * Signal the waiter immediately, and have the caller wait until its simulation has terminated.
     *
     * @param interceptor the interceptor to relay events to
     * @param isTimeout propagate the signal to the wrapped condition we are waiting on
     */
    void triggerAndAwaitDone(InterceptorOfConsequences interceptor, boolean isTimeout);

    /**
     * Signal all waiters immediately, bypassing the simulation
     */
    void triggerBypass();

    /**
     * Add a trigger listener, to notify the wait is no longer valid
     */
    void addListener(TriggerListener onTrigger);

    Thread waiting();

    /**
     * A general purpose superclass for implementing an intercepted/simulated thread wait event.
     * All share this implementation except for monitor waits, which must use the monitor they are waiting on
     * in order to release its lock.
     */
    class InterceptedConditionWait extends NotInterceptedSyncCondition implements InterceptedWait
    {
        static final Logger logger = LoggerFactory.getLogger(InterceptedConditionWait.class);

        final Kind kind;
        final InterceptibleThread waiting;
        final CaptureSites captureSites;
        final InterceptorOfConsequences interceptedBy;
        final Condition propagateSignal;
        final List<TriggerListener> onTrigger = new ArrayList<>(3);
        boolean isTriggered;
        boolean isDone;
        boolean hasExited;

        public InterceptedConditionWait(Kind kind, InterceptibleThread waiting, CaptureSites captureSites, Condition propagateSignal)
        {
            this.kind = kind;
            this.waiting = waiting;
            this.captureSites = captureSites;
            this.interceptedBy = waiting.interceptedBy();
            this.propagateSignal = propagateSignal;
        }

        public synchronized void triggerAndAwaitDone(InterceptorOfConsequences interceptor, boolean isTimeout)
        {
            if (isTriggered)
                return;

            if (hasExited)
            {
                logger.error("{} exited without trigger {}", waiting, captureSites == null ? new CaptureSites(waiting, true) : captureSites);
                failWithOOM();
            }

            waiting.beforeInvocation(interceptor, this);
            isTriggered = true;
            onTrigger.forEach(listener -> listener.onTrigger(this));
            super.signal();
            if (!isTimeout && propagateSignal != null)
                propagateSignal.signal();

            try
            {
                while (!isDone)
                    wait();
            }
            catch (InterruptedException ie)
            {
                throw new UncheckedInterruptedException(ie);
            }
        }

        public synchronized void triggerBypass()
        {
            if (isTriggered)
                return;

            isTriggered = true;
            super.signal();
            if (propagateSignal != null)
                propagateSignal.signal();
        }

        @Override
        public void addListener(TriggerListener onTrigger)
        {
            this.onTrigger.add(onTrigger);
        }

        @Override
        public Thread waiting()
        {
            return waiting;
        }

        @Override
        public synchronized void notifyThreadPaused()
        {
            isDone = true;
            notifyAll();
        }

        @Override
        public Kind kind()
        {
            return kind;
        }

        public boolean isTriggered()
        {
            return isSignalled();
        }

        // ignore return value; always false as can only represent artificial (intercepted) signaled status
        public boolean await(long time, TimeUnit unit) throws InterruptedException
        {
            try
            {
                super.await();
            }
            finally
            {
                hasExited = true;
            }
            return false;
        }

        // ignore return value; always false as can only represent artificial (intercepted) signaled status
        public boolean awaitUntil(long until) throws InterruptedException
        {
            try
            {
                super.await();
            }
            finally
            {
                hasExited = true;
            }
            return false;
        }

        // ignore return value; always false as can only represent artificial (intercepted) signaled status
        public boolean awaitUntilUninterruptibly(long until)
        {
            try
            {
                super.awaitUninterruptibly();
            }
            finally
            {
                hasExited = true;
            }
            return false;
        }

        public Condition await() throws InterruptedException
        {
            try
            {
                super.await();
            }
            finally
            {
                hasExited = true;
            }
            return this;
        }

        public Condition awaitUninterruptibly()
        {
            try
            {
                super.awaitUninterruptibly();
            }
            finally
            {
                hasExited = true;
            }
            return this;
        }

        public String toString()
        {
            return captureSites == null ? "" : captureSites.toString();
        }
    }

    // debug the place at which a thread waits or is signalled
    @Shared(scope = SIMULATION)
    public static class CaptureSites
    {
        final Thread waiting;
        final StackTraceElement[] waitSite;
        final boolean printNowTrace;
        @SuppressWarnings("unused") Thread waker;
        StackTraceElement[] wakeupSite;

        public CaptureSites(Thread waiting, StackTraceElement[] waitSite, boolean printNowTrace)
        {
            this.waiting = waiting;
            this.waitSite = waitSite;
            this.printNowTrace = printNowTrace;
        }

        public CaptureSites(Thread waiting, boolean printNowTrace)
        {
            this.waiting = waiting;
            this.waitSite = waiting.getStackTrace();
            this.printNowTrace = printNowTrace;
        }

        void registerWakeup(Thread waking)
        {
            this.waker = waking;
            this.wakeupSite = waking.getStackTrace();
        }

        public String toString()
        {
            String tail;
            if (wakeupSite != null)
                tail = Threads.prettyPrint(wakeupSite, true, printNowTrace ? "]# by[" : waitSite != null ? " by[" : "by[", "; ", "]");
            else if (printNowTrace)
                tail = "]#";
            else
                tail = "";
            if (printNowTrace)
                tail = Threads.prettyPrintStackTrace(waiting, true, waitSite != null ? " #[" : "#[", "; ", tail);
            if (waitSite != null)
                tail =Threads.prettyPrint(waitSite, true, "", "; ", tail);
            return tail;
        }
    }

}
