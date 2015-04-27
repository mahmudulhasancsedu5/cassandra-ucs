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

import java.util.*;

/**
 * A MergeIterator that consumes multiple input values per output value.
 *
 * The most straightforward way to implement this is to use a {@code PriorityQueue} of iterators, {@code poll} it to
 * find the next item to consume, then {@code add} the iterator back after advancing. This is not very efficient as
 * {@code poll} and {@code add} in all cases require at least {@code log(size)} comparisons (usually more than
 * {@code 2*log(size)}) per consumed item, even if the input is suitable for fast iteration.
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
 * Where each line is a < relationship. In the sorted section we can advance with a single comparison per level,
 * while advancing a level within the heap requires two (so that we can find the lighter element to pop up).
 * The sorted section adds a constant overhead when data is uniformly distributed among the iterators, but may up
 * to halve the iteration time when one iterator is dominant over sections of the merged data (as is the case with
 * non-overlapping iterators).
 *
 * Each entry in the heap maintains a linked list of equal iterators; equal lists are combined the first time equality
 * is recognized. When this happens an entry in the heap needs to be removed, which is done by replacing it with the
 * entry at the end of the heap and moving it down or up to place it in its correct position.
 *
 * Input iterators are only advanced when an output is actually requested. To achieve this the top elements of the heap
 * between computeNext calls is kept consumed but not advanced. Advancing expands the list of equal iterators, advances
 * each and finds its place: the first one can be pushed down, but the others need to be placed at the end of the heap
 * and pulled up.
 * 
 * This works very well for randomly spaced as well as completely non-overlapping iterators.
 */
final class MergeIteratorAllEqual<In,Out> extends MergeIterator<In,Out>
{
    protected final Candidate<In>[] heap;

    /** Number of non-exhausted iterators. */
    int size;

    /**
     * The number of elements to keep in order before the binary heap starts, exclusive of the top heap element.
     */
    static final int SORTED_SECTION_SIZE = 4;

    public MergeIteratorAllEqual(List<? extends Iterator<In>> iters, Comparator<In> comp, Reducer<In, Out> reducer)
    {
        super(iters, reducer);

        @SuppressWarnings("unchecked")
        Candidate<In>[] heap = new Candidate[iters.size()];
        this.heap = heap;
        size = 0;
        Candidate<In> queue = null;

        for (Iterator<In> iter : iters)
        {
            Candidate<In> candidate = new Candidate<>(iter, comp);
            if (queue == null)
                queue = candidate;
            else
                queue.attachEqual(candidate);
        }
        heap[size++] = queue;
    }

    protected final Out computeNext()
    {
        advance();
        return consume();
    }

    /**
     * Advance all iterators that need to be advanced and place them into suitable positions in the heap.
     *
     * The top of the heap contains a list of equal iterators. Advance each. The first non-exhausted one can be pushed
     * down to find its position in the heap. There is no room to place the others at the top, so they are attached
     * at the end and pulled up instead.
     */
    private void advance()
    {
        Candidate<In> queue = heap[0];
        boolean sunk = false;
        while (queue != null)
        {
            Candidate<In> candidate = queue.advance();
            queue = queue.detach();
            if (candidate != null)
            {
                if (!sunk) {
                    replaceAndSink(candidate, 0);
                } else {
                    // The heap is correctly-formed at this point (replaceAndSink has placed an element on top).
                    addAndSwim(candidate);
                }
                sunk = true;
            }
        }

        if (!sunk)
        {
            replaceAndSink(heap[--size], 0);
            heap[size] = null;
        }
    }

    /**
     * Consume all items at the top of the heap.
     */
    private Out consume()
    {
        if (size == 0)
        {
            return endOfData();
        }

        reducer.onKeyChange();
        heap[0].consume(reducer);
        return reducer.getReduced();
    }

    /**
     * Add an item at the end of the heap and pull it up until it finds its place.
     */
    private void addAndSwim(Candidate<In> candidate)
    {
        if (addAndSwim(candidate, size))
            ++size;
    }

    /**
     * Place an item at the given position and pull it up until it finds its place.
     * If there is an equal item in the parents chain, attach to it as equal and return
     * false (i.e. the target position is still empty).
     *
     * The parents chain of the given position must be correctly formed.
     */
    private boolean addAndSwim(Candidate<In> candidate, int index)
    {
        int currIndex = index;
        int cmp = -1;
        // Find where we should attach or insert candidate.
        while ((currIndex = parentIndex(currIndex)) >= 0)
        {
            cmp = heap[currIndex].compareTo(candidate);
            if (cmp <= 0)
                break;
        }

        if (cmp == 0)
        {
            // Equality means we can simply attach.
            heap[currIndex].attachEqual(candidate);
            return false;
        }
        else
        {
            // Otherwise shift everything downwards.
            int stopIndex = currIndex;
            currIndex = index;
            while (true)
            {
                int nextIndex = parentIndex(currIndex);
                if (nextIndex == stopIndex)
                    break;
                heap[currIndex] = heap[nextIndex];
                currIndex = nextIndex;
            }
            
            heap[currIndex] = candidate;
            return true;
        }
    }

    static private int parentIndex(int currIndex)
    {
        return currIndex >= SORTED_SECTION_SIZE ? (currIndex + SORTED_SECTION_SIZE - 1) >> 1 : currIndex - 1;
    }
    
    /**
     * Replace an item that has been removed from the heap (normally due to equality).
     * Takes elements from the end of the heap, places them at the target position and sends them up or down depending
     * on comparison with parent. May need more than one element to complete (e.g. if one gets attached to an equal
     * element up the parents chain).
     *
     * The parents chain as well as the subheaps rooted at the children of the given target position must be correctly
     * formed. The target position is treated as empty.
     */
    private void replaceDeleted(int index)
    {
        if (index < SORTED_SECTION_SIZE)
        {
            Candidate<In> rep = heap[--size];
            heap[size] = null;
            replaceAndSink(rep, index);
            return;
        }

        int prevIndex = parentIndex(index);
        while (index < --size)
        {
            Candidate<In> parent = heap[prevIndex];
            Candidate<In> rep = heap[size];
            heap[size] = null;
            // Compare to parent to see if it needs to go up or down.
            int cmp = rep.compareTo(parent);
            if (cmp == 0)
                parent.attachEqual(rep);
            else if (cmp > 0)
            {
                replaceAndSink(rep, index);
                return;
            }
            else
            {
                // Already compared parent, continue from one step above.
                if (addAndSwim(rep, prevIndex))
                {
                    heap[index] = parent;
                    return;
                }
            }
        } 
        assert size == index;
        heap[size] = null;
    }

    /**
     * Replace an iterator in the heap with the given position and move it down the heap until it finds its proper
     * position, pulling lighter elements up the heap.
     *
     * The subheaps rooted at the children of the target position must be correctly formed.
     */
    private void replaceAndSink(Candidate<In> candidate, int currIdx)
    {
        int nextIdx;
        Candidate<In> next = null;
        int cmp = -1;

        sink:
        {
            // Advance within the sorted section, pulling up items lighter than candidate.
            while ((nextIdx = currIdx + 1) <= SORTED_SECTION_SIZE)
            {
                if (nextIdx >= size)
                    break sink;
                next = heap[nextIdx];
                cmp = candidate.compareTo(next);
                if (cmp <= 0)
                    break sink;

                heap[currIdx] = next;
                currIdx = nextIdx;
            }

            // Advance in the binary heap, pulling up the lighter element from the two at each level.
            while ((nextIdx = (currIdx << 1) - (SORTED_SECTION_SIZE - 1)) < size)
            {
                next = heap[nextIdx];
                if (nextIdx + 1 < size)
                {
                    cmp = heap[nextIdx + 1].compareTo(next);
                    if (cmp == 0)
                    {
                        // Found equality between siblings. Collapse the two siblings, restore some order in the heap
                        // and place replacements in the voided position.
                        next.attachEqual(heap[nextIdx + 1]);
                        // Can't replace nextIdx + 1 before knowing what should be at currIdx. Compare next with
                        // candidate to decide.
                        cmp = candidate.compareTo(next);
                        if (cmp < 0)
                        {
                            // Easy case, found position for candidate, just replace collapsed entry.
                            heap[currIdx] = candidate;
                            replaceDeleted(nextIdx + 1);
                            return;
                        }

                        // Candidate needs to be processed further, but we know collapsed siblings can be placed at
                        // currIdx, which is enough structure to let replaceDeleted succeed.
                        heap[currIdx] = next;
                        replaceDeleted(nextIdx + 1);
                        if (cmp == 0)
                        {
                            // Candidate needs to be collapsed as well. Replace the voided position.
                            next.attachEqual(candidate);
                            replaceDeleted(nextIdx);
                            return;
                        }
                        
                        currIdx = nextIdx;
                        continue;
                    } else if (cmp < 0)
                        next = heap[++nextIdx];
                }

                cmp = candidate.compareTo(next);
                if (cmp <= 0)
                    break sink;
                heap[currIdx] = next;
                currIdx = nextIdx;
            }
        }
        heap[currIdx] = candidate;
        if (cmp == 0)
        {
            candidate.attachEqual(next);
            replaceDeleted(nextIdx);
        }
    }

    // Holds and is comparable by the head item of an iterator it owns
    protected static final class Candidate<In> implements Comparable<Candidate<In>>
    {
        private final Iterator<In> iter;
        private final Comparator<In> comp;
        private In item;
        private Candidate<In> nextEqual;
        private Candidate<In> lastEqual;

        public Candidate(Iterator<In> iter, Comparator<In> comp)
        {
            this.iter = iter;
            this.comp = comp;
            item = null;
            nextEqual = null;
            lastEqual = this;
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

        public void consume(Reducer<In, ?> reducer)
        {
            Candidate<In> curr = this;
            do
            {
                In temp = curr.item;
                curr.item = null;
                assert temp != null;
                reducer.reduce(temp);
                curr = curr.nextEqual;
            }
            while (curr != null);
        }

        public boolean needsAdvance()
        {
            return item == null;
        }
        
        public void attachEqual(Candidate<In> equalChain)
        {
            Candidate<In> last = this.lastEqual;
            assert last.lastEqual == last;
            assert last.nextEqual == null;
            last.nextEqual = equalChain;
            lastEqual = equalChain.lastEqual;
        }
        
        public Candidate<In> detach()
        {
            Candidate<In> next = nextEqual;
            nextEqual = null;
            lastEqual = this;
            return next;
        }
        
        public String toString()
        {
            int cnt = countLinked();
            String rep = cnt == 1 ? "" : "x" + cnt;

            if (item == null)
                return "consumed " + rep + " " + Integer.toString(hashCode(), 16);
            else
                return item.toString() + rep;
        }

        private int countLinked()
        {
            int cnt = 0;
            Candidate<In> curr = this;
            while (curr != null) {
                ++cnt;
                curr = curr.nextEqual;
            }
            return cnt;
        }
    }
}
