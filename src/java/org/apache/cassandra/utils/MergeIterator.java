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
package org.apache.cassandra.utils;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

import com.google.common.collect.AbstractIterator;

/** Merges sorted input iterators which individually contain unique items. */
public abstract class MergeIterator<In,Out> extends AbstractIterator<Out> implements IMergeIterator<In, Out>
{
    protected final Reducer<In,Out> reducer;
    protected final List<? extends Iterator<In>> iterators;

    protected MergeIterator(List<? extends Iterator<In>> iters, Reducer<In, Out> reducer)
    {
        this.iterators = iters;
        this.reducer = reducer;
    }

    public static <In, Out> IMergeIterator<In, Out> get(List<? extends Iterator<In>> sources,
                                                        Comparator<In> comparator,
                                                        Reducer<In, Out> reducer)
    {
        if (sources.size() == 1)
        {
            return reducer.trivialReduceIsTrivial()
                 ? new TrivialOneToOne<>(sources, reducer)
                 : new OneToOne<>(sources, reducer);
        }
        return new ManyToOneNoCand<>(sources, comparator, reducer);
    }

    public Iterable<? extends Iterator<In>> iterators()
    {
        return iterators;
    }

    public void close()
    {
        for (Iterator<In> iterator : this.iterators)
        {
            try
            {
                ((Closeable)iterator).close();
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }

        reducer.close();
    }

    /** A MergeIterator that consumes multiple input values per output value. */
    static final class ManyToOne<In,Out> extends MergeIterator<In,Out>
    {
        protected final Candidate<In>[] heap;
        int size;

        public ManyToOne(List<? extends Iterator<In>> iters, Comparator<In> comp, Reducer<In, Out> reducer)
        {
            super(iters, reducer);

            @SuppressWarnings("unchecked")
            Candidate<In>[] heap = new Candidate[iters.size()];
            this.heap = heap;
            size = 0;
            for (Iterator<In> iter : iters)
            {
                Candidate<In> candidate = new Candidate<>(iter, comp).advance();
                if (candidate != null)
                    heap[size++] = candidate;
            }
            
            for (int i = size - 1; i >= 0; --i)
            {
                replaceAndSink(heap[i], i);
            }
            // Sort to prepare the heap for processing. The heap could be prepared more efficiently, but this is a
            // negligible part of the overall iteration time and not worth the extra code.
//            Arrays.sort(heap, 0, size);
        }

        protected final Out computeNext()
        {
            reducer.onKeyChange();
            if (size == 0)
                return endOfData();

            In item = heap[0].item;
            do
            {
                Candidate<In> candidate = heap[0];
                reducer.reduce(candidate.item);
                replaceTop(candidate.advance());
            }
            while (size > 0 && heap[0].equalItem(item));
            return reducer.getReduced();
        }

        /**
         * The number of elements to keep in order before the binary heap starts, exclusive of the top heap element.
         * This section helps the performance in cases where iterators are largely non-overlapping.
         * 
         * The heap is laid out as (example for SORTED_SECTION == 2):
         *                 0
         *                 |
         *                 1
         *                 |
         *                 2
         *               /   \
         *              3     4
         *             / \   / \
         *             5 6   7 8
         *            .. .. .. ..
         * Where each line is a <= relationship. In the sorted section we can advance with a single comparison per level,
         * while advancing a level within the heap requires two (so that we can find the lighter element to pop up).
         * The sorted section adds a constant overhead when data is uniformly distributed among the iterators, but may up
         * to halve the iteration time when one iterator is dominant over sections of the merged data (as is the case with
         * non-overlapping iterators).
         */
        static final int SORTED_SECTION = 2;

        /**
         * Replace the top element with the given and move it down in the heap until it finds its proper position.
         */
        private void replaceTop(Candidate<In> candidate)
        {
            if (candidate == null)
            {
                // Drop iterator by replacing it with the last one in the heap.
                candidate = heap[--size];
                heap[size] = null; // not necessary but helpful for debugging
            }
            replaceAndSink(candidate, 0);
        }

        private void replaceAndSink(Candidate<In> candidate, int currIdx)
        {
            final int size = this.size;
            final int sortedSectionSize = Math.min(size, SORTED_SECTION + 1);
            int nextIdx = currIdx + 1;

            loop:
            {
                // Advance within the sorted section, pulling up items lighter than candidate.
                while (nextIdx < sortedSectionSize)
                {
                    if (candidate.compareTo(heap[nextIdx]) <= 0)
                        break loop;

                    heap[currIdx] = heap[nextIdx];
                    currIdx = nextIdx;
                    nextIdx = currIdx + 1;
                }

                // Advance in the binary heap, pulling up the lighter element from the two at each level.
                while (nextIdx < size)
                {
                    if (nextIdx + 1 < size && heap[nextIdx + 1].compareTo(heap[nextIdx]) < 0)
                        ++nextIdx;

                    if (candidate.compareTo(heap[nextIdx]) <= 0)
                        break loop;

                    heap[currIdx] = heap[nextIdx];
                    currIdx = nextIdx;
                    nextIdx = (nextIdx << 1) - (SORTED_SECTION - 1);
                }
            }
            heap[currIdx] = candidate;
        }
    }


    /** A MergeIterator that consumes multiple input values per output value. */
    static final class ManyToOneNoCand<In,Out> extends MergeIterator<In,Out>
    {
        protected final In[] items;
        protected final Iterator<In>[] iters;
        final Comparator<In> comparator;
        int size;

        public ManyToOneNoCand(List<? extends Iterator<In>> iterators, Comparator<In> comp, Reducer<In, Out> reducer)
        {
            super(iterators, reducer);
            this.comparator = comp;

            @SuppressWarnings("unchecked")
            In[] items = (In[]) new Object[iterators.size()];
            @SuppressWarnings("unchecked")
            Iterator<In>[] iters = new Iterator[iterators.size()];
            size = 0;
            for (Iterator<In> iter : iterators)
                if (iter.hasNext())
                {
                    items[size] = iter.next();
                    iters[size] = iter;
                    ++size;
                }
            this.iters = iters;
            this.items = items;
            
            for (int i = size - 1; i >= 0; --i)
            {
                replaceAndSink(items[i], iters[i], i);
            }
        }

        protected final Out computeNext()
        {
            reducer.onKeyChange();
            if (size == 0)
                return endOfData();

            In item = items[0];
            do
            {
                reducer.reduce(items[0]);
                Iterator<In> it = iters[0];
                if (it.hasNext())
                {
                    replaceAndSink(it.next(), it, 0);
                } else {
                    --size;
                    replaceAndSink(items[size], iters[size], 0);
                    items[size] = null;
                    iters[size] = null;
                }
            }
            while (size > 0 && comparator.compare(items[0], item) == 0);
            return reducer.getReduced();
        }

        /**
         * The number of elements to keep in order before the binary heap starts, exclusive of the top heap element.
         * This section helps the performance in cases where iterators are largely non-overlapping.
         * 
         * The heap is laid out as (example for SORTED_SECTION == 2):
         *                 0
         *                 |
         *                 1
         *                 |
         *                 2
         *               /   \
         *              3     4
         *             / \   / \
         *             5 6   7 8
         *            .. .. .. ..
         * Where each line is a <= relationship. In the sorted section we can advance with a single comparison per level,
         * while advancing a level within the heap requires two (so that we can find the lighter element to pop up).
         * The sorted section adds a constant overhead when data is uniformly distributed among the iterators, but may up
         * to halve the iteration time when one iterator is dominant over sections of the merged data (as is the case with
         * non-overlapping iterators).
         */
        static final int SORTED_SECTION = 2;

        private void replaceAndSink(In item, Iterator<In> iter, int currIdx)
        {
            final int size = this.size;
            final int sortedSectionSize = Math.min(size, SORTED_SECTION + 1);
            int nextIdx = currIdx + 1;

            loop:
            {
                // Advance within the sorted section, pulling up items lighter than candidate.
                while (nextIdx < sortedSectionSize)
                {
                    if (comparator.compare(item, items[nextIdx]) <= 0)
                        break loop;

                    items[currIdx] = items[nextIdx];
                    iters[currIdx] = iters[nextIdx];
                    currIdx = nextIdx;
                    nextIdx = currIdx + 1;
                }

                // Advance in the binary heap, pulling up the lighter element from the two at each level.
                while (nextIdx < size)
                {
                    if (nextIdx + 1 < size && comparator.compare(items[nextIdx + 1], items[nextIdx]) < 0)
                        ++nextIdx;

                    if (comparator.compare(item, items[nextIdx]) <= 0)
                        break loop;

                    items[currIdx] = items[nextIdx];
                    iters[currIdx] = iters[nextIdx];
                    currIdx = nextIdx;
                    nextIdx = (nextIdx << 1) - (SORTED_SECTION - 1);
                }
            }
            items[currIdx] = item;
            iters[currIdx] = iter;
        }
    }

    // Holds and is comparable by the head item of an iterator it owns
    protected static final class Candidate<In> implements Comparable<Candidate<In>>
    {
        private final Iterator<In> iter;
        private final Comparator<In> comp;
        In item;

        public Candidate(Iterator<In> iter, Comparator<In> comp)
        {
            this.iter = iter;
            this.comp = comp;
        }

        /** @return this if our iterator had an item, and it is now available, otherwise null */
        protected Candidate<In> advance()
        {
            if (!iter.hasNext())
                return null;
            item = iter.next();
            return this;
        }

        public int compareTo(Candidate<In> that)
        {
            return comp.compare(this.item, that.item);
        }

        public boolean equalItem(In thatItem)
        {
            return comp.compare(this.item, thatItem) == 0;
        }
    }

    /** Accumulator that collects values of type A, and outputs a value of type B. */
    public static abstract class Reducer<In,Out>
    {
        /**
         * @return true if Out is the same as In for the case of a single source iterator
         */
        public boolean trivialReduceIsTrivial()
        {
            return false;
        }

        /**
         * combine this object with the previous ones.
         * intermediate state is up to your implementation.
         */
        public abstract void reduce(In current);

        /** @return The last object computed by reduce */
        protected abstract Out getReduced();

        /**
         * Called at the beginning of each new key, before any reduce is called.
         * To be overridden by implementing classes.
         */
        protected void onKeyChange() {}

        /**
         * May be overridden by implementations that require cleaning up after use
         */
        public void close() {}
    }

    private static class OneToOne<In, Out> extends MergeIterator<In, Out>
    {
        private final Iterator<In> source;

        public OneToOne(List<? extends Iterator<In>> sources, Reducer<In, Out> reducer)
        {
            super(sources, reducer);
            source = sources.get(0);
        }

        protected Out computeNext()
        {
            if (!source.hasNext())
                return endOfData();
            reducer.onKeyChange();
            reducer.reduce(source.next());
            return reducer.getReduced();
        }
    }

    private static class TrivialOneToOne<In, Out> extends MergeIterator<In, Out>
    {
        private final Iterator<In> source;

        public TrivialOneToOne(List<? extends Iterator<In>> sources, Reducer<In, Out> reducer)
        {
            super(sources, reducer);
            source = sources.get(0);
        }

        @SuppressWarnings("unchecked")
        protected Out computeNext()
        {
            if (!source.hasNext())
                return endOfData();
            return (Out) source.next();
        }
    }
}
