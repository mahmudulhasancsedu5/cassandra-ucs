/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.Callable;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.MergeIterator.Reducer;

public class MergeIteratorLongTest
{
    static int ITERATOR_COUNT = 15;
    static int LIST_LENGTH = 200000;
    static boolean BENCHMARK = true;
    
    @Test
    public void testRandomInts() throws Exception
    {
        final Random r = new Random();
        Comparator<Integer> comparator = Ordering.natural();
        Reducer<Integer, Counted<Integer>> reducer = new Counter<Integer>();

        List<List<Integer>> lists = generateLists(ITERATOR_COUNT, LIST_LENGTH, new Callable<Integer>() {
            @Override
            public Integer call() throws Exception
            {
                return r.nextInt(5 * LIST_LENGTH);
            }
        }, comparator);
        testMergeIterator(comparator, reducer, lists);
    }
    
    @Test
    public void testNonOverlapInts() throws Exception
    {
        Comparator<Integer> comparator = Ordering.natural();
        Reducer<Integer, Counted<Integer>> reducer = new Counter<Integer>();

        List<List<Integer>> lists = generateLists(ITERATOR_COUNT, LIST_LENGTH, new Callable<Integer>() {
            int next = 1;
            @Override
            public Integer call() throws Exception
            {
                return next++;
            }
        }, comparator);
        testMergeIterator(comparator, reducer, lists);
    }

    @Test
    public void testCombinationInts() throws Exception
    {
        final Random r = new Random();
        Comparator<Integer> comparator = Ordering.natural();
        Reducer<Integer, Counted<Integer>> reducer = new Counter<Integer>();

        List<List<Integer>> lists = generateLists(ITERATOR_COUNT, LIST_LENGTH, new Callable<Integer>() {
            int next = 1;
            @Override
            public Integer call() throws Exception
            {
                return r.nextBoolean() ? r.nextInt(5 * LIST_LENGTH) : next++;
            }
        }, comparator);
        testMergeIterator(comparator, reducer, lists);
    }
    
    @Test
    public void testRandomStrings() throws Exception
    {
        final Random r = new Random();
        Comparator<String> comparator = Ordering.natural();
        Reducer<String, Counted<String>> reducer = new Counter<String>();

        List<List<String>> lists = generateLists(ITERATOR_COUNT, LIST_LENGTH, new Callable<String>() {
            @Override
            public String call() throws Exception
            {
                return "longish_prefix_" + r.nextInt(5 * LIST_LENGTH);
            }
        }, comparator);
        testMergeIterator(comparator, reducer, lists);
    }
    
    @Test
    public void testNonOverlapStrings() throws Exception
    {
        Comparator<String> comparator = Ordering.natural();
        Reducer<String, Counted<String>> reducer = new Counter<String>();

        List<List<String>> lists = generateLists(ITERATOR_COUNT, LIST_LENGTH, new Callable<String>() {
            int next = 1;
            @Override
            public String call() throws Exception
            {
                return "longish_prefix_" + next++;
            }
        }, comparator);
        testMergeIterator(comparator, reducer, lists);
    }

    @Test
    public void testCombinationStrings() throws Exception
    {
        final Random r = new Random();
        Comparator<String> comparator = Ordering.natural();
        Reducer<String, Counted<String>> reducer = new Counter<String>();

        List<List<String>> lists = generateLists(ITERATOR_COUNT, LIST_LENGTH, new Callable<String>() {
            int next = 1;
            @Override
            public String call() throws Exception
            {
                return "longish_prefix_" + (r.nextBoolean() ? r.nextInt(5 * LIST_LENGTH) : next++);
            }
        }, comparator);
        testMergeIterator(comparator, reducer, lists);
    }

    @Test
    public void testTimeUuids() throws Exception
    {
        Comparator<UUID> comparator = Ordering.natural();
        Reducer<UUID, Counted<UUID>> reducer = new Counter<UUID>();

        List<List<UUID>> lists = generateLists(ITERATOR_COUNT, LIST_LENGTH, new Callable<UUID>() {
            @Override
            public UUID call() throws Exception
            {
                return UUIDGen.getTimeUUID();
            }
        }, comparator);
        testMergeIterator(comparator, reducer, lists);
    }

    @Test
    public void testRandomUuids() throws Exception
    {
        Comparator<UUID> comparator = Ordering.natural();
        Reducer<UUID, Counted<UUID>> reducer = new Counter<UUID>();

        List<List<UUID>> lists = generateLists(ITERATOR_COUNT, LIST_LENGTH, new Callable<UUID>() {
            @Override
            public UUID call() throws Exception
            {
                return UUID.randomUUID();
            }
        }, comparator);
        testMergeIterator(comparator, reducer, lists);
    }

    @Test
    public void testTimeUuidType() throws Exception
    {
        final AbstractType<UUID> comparator = TimeUUIDType.instance;
        Reducer<ByteBuffer, Counted<ByteBuffer>> reducer = new Counter<ByteBuffer>();

        List<List<ByteBuffer>> lists = generateLists(ITERATOR_COUNT, LIST_LENGTH, new Callable<ByteBuffer>() {
            @Override
            public ByteBuffer call() throws Exception
            {
                return comparator.decompose(UUIDGen.getTimeUUID());
            }
        }, comparator);
        testMergeIterator(comparator, reducer, lists);
    }

    @Test
    public void testUuidType() throws Exception
    {
        final AbstractType<UUID> comparator = UUIDType.instance;
        Reducer<ByteBuffer, Counted<ByteBuffer>> reducer = new Counter<ByteBuffer>();

        List<List<ByteBuffer>> lists = generateLists(ITERATOR_COUNT, LIST_LENGTH, new Callable<ByteBuffer>() {
            @Override
            public ByteBuffer call() throws Exception
            {
                return comparator.decompose(UUID.randomUUID());
            }
        }, comparator);
        testMergeIterator(comparator, reducer, lists);
    }

    public <T> List<List<T>> generateLists(int itcount, int length, Callable<T> generator,
            Comparator<T> comparator) throws Exception
    {
        List<List<T>> lists = Lists.newArrayList();
        for (int i=0; i<itcount; ++i) {
            List<T> l = Lists.newArrayList();
            for (int j=0; j<length; ++j) {
                l.add(generator.call());
            }
            Collections.sort(l, comparator);
            lists.add(l);
        }
        return lists;
    }

    public <T> void testMergeIterator(Comparator<T> comparator, Reducer<T, Counted<T>> reducer, List<List<T>> lists)
    {
        IMergeIterator<T,Counted<T>> tested = MergeIterator.get(closeableIterators(lists), comparator, reducer);
        IMergeIterator<T,Counted<T>> base = new MergeIteratorPQ<>(closeableIterators(lists), comparator, reducer);
        // If test fails, try the version below for improved reporting:
        //   Assert.assertArrayEquals(Iterators.toArray(tested, Counted.class), Iterators.toArray(base, Counted.class));
        Assert.assertTrue(Iterators.elementsEqual(tested, base));
        if (!BENCHMARK)
            return;

        System.out.println();
        for (int i=0; i<10; ++i) {
            benchmarkIterator(MergeIterator.get(closeableIterators(lists), comparator, reducer));
            benchmarkIterator(new MergeIterator.ManyToOne<>(closeableIterators(lists), comparator, reducer));
            benchmarkIterator(new MergeIteratorPQ<>(closeableIterators(lists), comparator, reducer));
        }
    }
    
    public <T> void benchmarkIterator(IMergeIterator<T, Counted<T>> it)
    {
        System.out.print("Testing " + it.getClass().getSimpleName() + "... ");
        long time = System.currentTimeMillis();
        Counted<T> value = null;
        while (it.hasNext())
            value = it.next();
        time = System.currentTimeMillis() - time;
        System.out.println("type " + value.item.getClass().getSimpleName() + " time " + time + "ms");
    }

    public <T> List<CloseableIterator<T>> closeableIterators(List<List<T>> iterators)
    {
        return Lists.transform(iterators, new Function<List<T>, CloseableIterator<T>>() {

            @Override
            public CloseableIterator<T> apply(List<T> arg)
            {
                // Go through LinkedHashSet to remove duplicates.
                return new CLI<T>(new LinkedHashSet<T>(arg).iterator());
            }
            
        });
    }

    static class Counted<T> {
        T item;
        int count;
        
        Counted(T item) {
            this.item = item;
            count = 0;
        }

        public boolean equals(Object obj)
        {
            if (obj == null || !(obj instanceof Counted))
                return false;
            Counted<?> c = (Counted<?>) obj;
            return Objects.equal(item, c.item) && count == c.count;
        }

        @Override
        public String toString()
        {
            return item.toString() + "x" + count;
        }
    }
    
    static class Counter<T> extends Reducer<T, Counted<T>> {
        Counted<T> current = null;

        @Override
        public void reduce(T next)
        {
            if (current == null)
                current = new Counted<T>(next);
            ++current.count;
        }

        @Override
        protected void onKeyChange()
        {
            current = null;
        }

        @Override
        protected Counted<T> getReduced()
        {
            return current;
        }
    }
    
    // closeable list iterator
    public static class CLI<E> extends AbstractIterator<E> implements CloseableIterator<E>
    {
        Iterator<E> iter;
        boolean closed = false;
        public CLI(Iterator<E> items)
        {
            this.iter = items;
        }

        protected E computeNext()
        {
            if (!iter.hasNext()) return endOfData();
            return iter.next();
        }

        public void close()
        {
            assert !this.closed;
            this.closed = true;
        }
    }

    // Old MergeIterator implementation for comparison.
    public class MergeIteratorPQ<In,Out> extends MergeIterator<In,Out> implements IMergeIterator<In, Out>
    {
        // a queue for return: all candidates must be open and have at least one item
        protected final PriorityQueue<Candidate<In>> queue;
        // a stack of the last consumed candidates, so that we can lazily call 'advance()'
        // TODO: if we had our own PriorityQueue implementation we could stash items
        // at the end of its array, so we wouldn't need this storage
        protected final ArrayDeque<Candidate<In>> candidates;
        public MergeIteratorPQ(List<? extends Iterator<In>> iters, Comparator<In> comp, Reducer<In, Out> reducer)
        {
            super(iters, reducer);
            this.queue = new PriorityQueue<>(Math.max(1, iters.size()));
            for (Iterator<In> iter : iters)
            {
                Candidate<In> candidate = new Candidate<>(iter, comp);
                if (candidate.advance() == null)
                    // was empty
                    continue;
                this.queue.add(candidate);
            }
            this.candidates = new ArrayDeque<>(queue.size());
        }

        protected final Out computeNext()
        {
            advance();
            return consume();
        }

        /** Consume values by sending them to the reducer while they are equal. */
        protected final Out consume()
        {
            reducer.onKeyChange();
            Candidate<In> candidate = queue.peek();
            if (candidate == null)
                return endOfData();
            do
            {
                candidate = queue.poll();
                candidates.push(candidate);
                reducer.reduce(candidate.item);
            }
            while (queue.peek() != null && queue.peek().compareTo(candidate) == 0);
            return reducer.getReduced();
        }

        /** Advance and re-enqueue all items we consumed in the last iteration. */
        protected final void advance()
        {
            Candidate<In> candidate;
            while ((candidate = candidates.pollFirst()) != null)
                if (candidate.advance() != null)
                    queue.add(candidate);
        }
    }

}
