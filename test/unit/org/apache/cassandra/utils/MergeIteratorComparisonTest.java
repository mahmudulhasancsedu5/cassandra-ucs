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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.utils.MergeIterator.Reducer;

public class MergeIteratorComparisonTest
{
    static int ITERATOR_COUNT = 15;
    static int LIST_LENGTH = 50000;
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

    
    @Test
    public void testSets() throws Exception
    {
        final Random r = new Random();
        final Comparator<KeyedSet<Integer, UUID>> comparator = Ordering.natural();
        
        Reducer<KeyedSet<Integer, UUID>, KeyedSet<Integer, UUID>> reducer = new Union<Integer, UUID>();

        List<List<KeyedSet<Integer, UUID>>> lists = generateLists(ITERATOR_COUNT, LIST_LENGTH, new Callable<KeyedSet<Integer, UUID>>() {
            @Override
            public KeyedSet<Integer, UUID> call() throws Exception
            {
                return new KeyedSet<>(r.nextInt(5 * LIST_LENGTH), UUIDGen.getTimeUUID());
            }
        }, comparator);
        testMergeIterator(comparator, reducer, lists);
    }
    /* */

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

    public <T> void testMergeIterator(Comparator<T> comparator, Reducer<T, ?> reducer, List<List<T>> lists)
    {
        IMergeIterator<T,?> tested = MergeIterator.get(closeableIterators(lists), comparator, reducer);
        IMergeIterator<T,?> tested2 = new MergeIteratorNoEqual<>(closeableIterators(lists), comparator, reducer);
        IMergeIterator<T,?> tested3 = new MergeIteratorAllEqual<>(closeableIterators(lists), comparator, reducer);
        IMergeIterator<T,?> base = new MergeIteratorPQ<>(closeableIterators(lists), comparator, reducer);
        // If test fails, try the version below for improved reporting:
        Object[] basearr = Iterators.toArray(base, Object.class);
           Assert.assertArrayEquals(basearr, Iterators.toArray(tested, Object.class));
           Assert.assertArrayEquals(basearr, Iterators.toArray(tested2, Object.class));
           Assert.assertArrayEquals(basearr, Iterators.toArray(tested3, Object.class));
           //Assert.assertTrue(Iterators.elementsEqual(base, tested));
        if (!BENCHMARK)
            return;

        System.out.println();
        for (int i=0; i<10; ++i) {
            benchmarkIterator(MergeIterator.get(closeableIterators(lists), comparator, reducer));
            benchmarkIterator(new MergeIteratorNoEqual<>(closeableIterators(lists), comparator, reducer));
            benchmarkIterator(new MergeIteratorAllEqual<>(closeableIterators(lists), comparator, reducer));
            benchmarkIterator(new MergeIteratorPQ<>(closeableIterators(lists), comparator, reducer));
        }
    }
    
    public <T> void benchmarkIterator(IMergeIterator<T, ?> it)
    {
        System.out.format("Testing %30s... ", it.getClass().getSimpleName());
        long time = System.currentTimeMillis();
        Object value = null;
        while (it.hasNext())
            value = it.next();
        time = System.currentTimeMillis() - time;
        String type = "";
        if (value instanceof Counted<?>)
        {
            type = "type " + ((Counted<?>)value).item.getClass().getSimpleName();
        }
        System.out.format("%15s time %5dms\n", type, time);
    }

    public <T> List<CloseableIterator<T>> closeableIterators(List<List<T>> iterators)
    {
        return Lists.transform(iterators, new Function<List<T>, CloseableIterator<T>>() {

            @Override
            public CloseableIterator<T> apply(List<T> arg)
            {
                return new CLI<T>(arg.iterator());
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
        boolean read = true;

        @Override
        public void reduce(T next)
        {
            if (current == null)
                current = new Counted<T>(next);
            assert current.item.equals(next);
            ++current.count;
        }

        @Override
        protected void onKeyChange()
        {
            assert read;
            current = null;
            read = false;
        }

        @Override
        protected Counted<T> getReduced()
        {
            assert current != null;
            read = true;
            return current;
        }
    }
    
    static class KeyedSet<K extends Comparable<? super K>, V> extends Pair<K, Set<V>> implements Comparable<KeyedSet<K, V>>
    {
        protected KeyedSet(K left, V right)
        {
            super(left, ImmutableSet.of(right));
        }
        
        protected KeyedSet(K left, Collection<V> right)
        {
            super(left, Sets.newHashSet(right));
        }

        @Override
        public int compareTo(KeyedSet<K, V> o)
        {
            return left.compareTo(o.left);
        }
    }
    
    static class Union<K extends Comparable<K>, V> extends Reducer<KeyedSet<K, V>, KeyedSet<K, V>> {
        KeyedSet<K, V> current = null;
        boolean read = true;

        @Override
        public void reduce(KeyedSet<K, V> next)
        {
            if (current == null)
                current = new KeyedSet<>(next.left, next.right);
            else {
                assert current.left.equals(next.left);
                current.right.addAll(next.right);
            }
        }

        @Override
        protected void onKeyChange()
        {
            assert read;
            current = null;
            read = false;
        }

        @Override
        protected KeyedSet<K, V> getReduced()
        {
            assert current != null;
            read = true;
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
        protected final PriorityQueue<CandidatePQ<In>> queue;
        // a stack of the last consumed candidates, so that we can lazily call 'advance()'
        // TODO: if we had our own PriorityQueue implementation we could stash items
        // at the end of its array, so we wouldn't need this storage
        protected final ArrayDeque<CandidatePQ<In>> candidates;
        public MergeIteratorPQ(List<? extends Iterator<In>> iters, Comparator<In> comp, Reducer<In, Out> reducer)
        {
            super(iters, reducer);
            this.queue = new PriorityQueue<>(Math.max(1, iters.size()));
            for (Iterator<In> iter : iters)
            {
                CandidatePQ<In> candidate = new CandidatePQ<>(iter, comp);
                if (!candidate.advance())
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
            CandidatePQ<In> candidate = queue.peek();
            if (candidate == null)
                return endOfData();
            reducer.onKeyChange();
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
            CandidatePQ<In> candidate;
            while ((candidate = candidates.pollFirst()) != null)
                if (candidate.advance())
                    queue.add(candidate);
        }
    }

    // Holds and is comparable by the head item of an iterator it owns
    protected static final class CandidatePQ<In> implements Comparable<CandidatePQ<In>>
    {
        private final Iterator<In> iter;
        private final Comparator<In> comp;
        In item;

        public CandidatePQ(Iterator<In> iter, Comparator<In> comp)
        {
            this.iter = iter;
            this.comp = comp;
        }

        /** @return true if our iterator had an item, and it is now available */
        protected boolean advance()
        {
            if (!iter.hasNext())
                return false;
            item = iter.next();
            return true;
        }

        public int compareTo(CandidatePQ<In> that)
        {
            return comp.compare(this.item, that.item);
        }
    }
}
