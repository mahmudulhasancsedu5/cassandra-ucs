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
        return new ManyToOne<>(sources, comparator, reducer);
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

    /**
     * A MergeIterator that consumes multiple input values per output value.
     *
     * The most straightforward way to implement this is to use a {@code PriorityQueue} of iterators, {@code poll} it to
     * find the next item to consume, then {@code add} the iterator back after advancing. This is not very efficient as
     * {@code poll} and {@code add} in almost all cases require at least log N comparisons per consumed item, even if
     * the input is suitable for fast iteration.
     *
     * The implementation below makes use of the fact that replacing the top element in a binary heap can be done much
     * more efficiently than separately removing it and placing it back, especially in the cases where the top iterator
     * is to be used again very soon (e.g. when there are large sections of the output where only a limited number of
     * input iterators overlap, which is normally the case in many practically useful situations, e.g. levelled
     * compaction). To further improve this particular scenario, we also use a short sorted section at the start of the
     * queue.
     *
     * The heap is laid out as this (for {@code SORTED_SECTION_SIZE == 2}):
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
     *
     * The iterator is further complicated by the need to avoid advancing the input iterators until an output is
     * actually requested. To achieve this {@code consume} walks the heap to find equal items without advancing the
     * iterators, and {@code advance} moves them and restores the heap structure before any items can be consumed.
     */
    static final class ManyToOne<In,Out> extends MergeIterator<In,Out>
    {
        protected final Candidate<In>[] heap;

        /** Number of non-exhausted iterators. */
        int size;

        /**
         * Number of iterators that may need advancing before we can start consuming. Because advancing changes the
         * values of the items of each iterator, the part of the heap before this position is not in the correct order
         * and needs to be heapified before it can be used.
         */
        int needingAdvance;

        /**
         * The number of elements to keep in order before the binary heap starts, exclusive of the top heap element.
         */
        static final int SORTED_SECTION_SIZE = 4;

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

            // Turn the set of candidates into a heap.
            for (int i = size - 1; i >= 0; --i)
                replaceAndSink(heap[i], i);
            needingAdvance = 0;
        }

        protected final Out computeNext()
        {
            advance();
            return consume();
        }

        /**
         * Advance all iterators that need to be advanced and place them into suitable positions in the heap.
         *
         * By walking the iterators backwards we know that everything after the point being processed already forms
         * correctly ordered subheaps, thus we can build a subheap rooted at the current position by only sinking down
         * the newly advanced iterator. Because all parents of a consumed iterator are also consumed there is no way
         * that we can process one consumed iterator but skip over its parent.
         *
         * The procedure is the same as the one used for the initial building of a heap in the heapsort algorithm and
         * has a maximum number of comparisons {@code (2 * log(size) + SORTED_SECTION_SIZE / 2)} multiplied by the
         * number of iterators whose items were consumed at the previous step, but is also at most linear in the size of
         * the heap if the number of consumed elements is high (as it is in the initial heap construction). With non- or
         * lightly-overlapping iterators the procedure finishes after just one (resp. a couple of) comparisons.
         */
        private void advance()
        {
            // Turn the set of candidates into a heap.
            for (int i = needingAdvance - 1; i >= 0; --i)
            {
                Candidate<In> candidate = heap[i];
                if (candidate.needsAdvance())
                    replaceAndSink(candidate.advance(), i);
            }
        }

        /**
         * Consume all items that sort like the current top of the heap. As we cannot advance the iterators to let
         * equivalent items pop up, we walk the heap to find them and mark them as needing advance.
         *
         * This does at most two comparisons per each consumed item (one while in the sorted section).
         */
        private Out consume()
        {
            reducer.onKeyChange();
            if (size == 0)
                return endOfData();

            In equalItem = heap[0].consume();
            reducer.reduce(equalItem);
            final int size = this.size;
            final int sortedSectionSize = Math.min(size, SORTED_SECTION_SIZE);
            int i;
            consume: {
                for (i = 1; i < sortedSectionSize; ++i)
                {
                    if (!heap[i].equalItem(equalItem))
                        break consume;
                    reducer.reduce(heap[i].consume());
                }
                i = consumeHeap(equalItem, i, i);
            }
            needingAdvance = i;
            return reducer.getReduced();
        }

        /**
         * Recursively consume all items equal to equalItem in the binary subheap rooted at position idx.
         *
         * @param maxIndex Upper bound for the largest index of equal element found so far.
         * @return updated maxIndex reflecting the largest equal index found in this search.
         */
        private int consumeHeap(In equalItem, int idx, int maxIndex)
        {
            if (idx < size && heap[idx].equalItem(equalItem)) 
            {
                reducer.reduce(heap[idx].consume());
                int nextIdx = (idx << 1) - (SORTED_SECTION_SIZE - 1);
                maxIndex = consumeHeap(equalItem, nextIdx, Math.max(maxIndex, idx + 1));
                maxIndex = consumeHeap(equalItem, nextIdx + 1, maxIndex);
            }
            return maxIndex;
        }

        /**
         * Replace an iterator in the heap with the given position and move it down the heap until it finds its proper
         * position, pulling lighter elements up the heap.
         */
        private int replaceAndSink(Candidate<In> candidate, int currIdx)
        {
            if (candidate == null)
            {
                // Drop iterator by replacing it with the last one in the heap.
                candidate = heap[--size];
                heap[size] = null; // not necessary but helpful for debugging
            }

            final int size = this.size;
            final int sortedSectionSize = Math.min(size, SORTED_SECTION_SIZE + 1);
            int nextIdx;

            sink:
            {
                // Advance within the sorted section, pulling up items lighter than candidate.
                while ((nextIdx = currIdx + 1) < sortedSectionSize)
                {
                    if (candidate.compareTo(heap[nextIdx]) <= 0)
                        break sink;

                    heap[currIdx] = heap[nextIdx];
                    currIdx = nextIdx;
                }
                if (nextIdx >= size)
                    break sink;

                // Advance in the binary heap, pulling up the lighter element from the two at each level.
                while ((nextIdx = (currIdx << 1) - (SORTED_SECTION_SIZE - 1)) < size)
                {
                    if (nextIdx + 1 < size && heap[nextIdx + 1].compareTo(heap[nextIdx]) < 0)
                        ++nextIdx;

                    if (candidate.compareTo(heap[nextIdx]) <= 0)
                        break sink;

                    heap[currIdx] = heap[nextIdx];
                    currIdx = nextIdx;
                }
            }
            heap[currIdx] = candidate;
            return currIdx;
        }
    }

    // Holds and is comparable by the head item of an iterator it owns
    protected static final class Candidate<In> implements Comparable<Candidate<In>>
    {
        private final Iterator<In> iter;
        private final Comparator<In> comp;
        private In item;

        public Candidate(Iterator<In> iter, Comparator<In> comp)
        {
            this.iter = iter;
            this.comp = comp;
            item = null;
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
            assert item != null && that.item != null;
            return comp.compare(this.item, that.item);
        }

        public boolean equalItem(In thatItem)
        {
            assert item != null && thatItem != null;
            return comp.compare(this.item, thatItem) == 0;
        }

        public In consume()
        {
            In temp = item;
            item = null;
            assert temp != null;
            return temp;
        }

        public boolean needsAdvance()
        {
            return item == null;
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
