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
package org.apache.cassandra.io.tries;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;

import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.concurrent.LightweightRecycler;
import org.apache.cassandra.utils.concurrent.ThreadLocals;

/**
 * Helper base class for incremental trie builders.
 */
public abstract class IncrementalTrieWriterBase<VALUE, DEST, NODE extends IncrementalTrieWriterBase.BaseNode<VALUE, NODE>>
implements IncrementalTrieWriter<VALUE>
{
    protected final Deque<NODE> stack = new ArrayDeque<>();
    protected final TrieSerializer<VALUE, ? super DEST> serializer;
    protected final DEST dest;
    protected ByteComparable prev = null;
    long count = 0;

    protected IncrementalTrieWriterBase(TrieSerializer<VALUE, ? super DEST> serializer, DEST dest, NODE root)
    {
        this.serializer = serializer;
        this.dest = dest;
        this.stack.addLast(root);
    }

    protected void reset(NODE root)
    {
        this.prev = null;
        this.count = 0;
        this.stack.clear();
        this.stack.addLast(root);
    }


    @Override
    public void close()
    {
        this.prev = null;
        this.count = 0;
        this.stack.clear();
    }

    @Override
    public void add(ByteComparable next, VALUE value) throws IOException
    {
        ++count;
        int stackpos = 0;
        ByteSource sn = next.asComparableBytes(Walker.BYTE_COMPARABLE_VERSION);
        int n = sn.next();

        if (prev != null)
        {
            ByteSource sp = prev.asComparableBytes(Walker.BYTE_COMPARABLE_VERSION);
            int p = sp.next();
            while ( n == p )
            {
                assert n != ByteSource.END_OF_STREAM : String.format("Incremental trie requires unique sorted keys, got equal %s(%s) after %s(%s).",
                                                                     next,
                                                                     next.byteComparableAsString(Walker.BYTE_COMPARABLE_VERSION),
                                                                     prev,
                                                                     prev.byteComparableAsString(Walker.BYTE_COMPARABLE_VERSION));

                ++stackpos;
                n = sn.next();
                p = sp.next();
            }
            assert p < n : String.format("Incremental trie requires sorted keys, got %s(%s) after %s(%s).",
                                         next,
                                         next.byteComparableAsString(Walker.BYTE_COMPARABLE_VERSION),
                                         prev,
                                         prev.byteComparableAsString(Walker.BYTE_COMPARABLE_VERSION));
        }
        prev = next;

        while (stack.size() > stackpos + 1)
            completeLast();

        NODE node = stack.getLast();
        while (n != ByteSource.END_OF_STREAM)
        {
            node = node.addChild((byte) n);
            stack.addLast(node);
            ++stackpos;
            n = sn.next();
        }

        VALUE existingPayload = node.setPayload(value);
        assert existingPayload == null;
    }

    public long complete() throws IOException
    {
        NODE root = stack.getFirst();
        if (root.filePos != -1)
            return root.filePos;

        return performCompletion().filePos;
    }

    NODE performCompletion() throws IOException
    {
        NODE root = null;
        while (!stack.isEmpty())
            root = completeLast();
        stack.addLast(root);
        return root;
    }

    public long count()
    {
        return count;
    }

    protected NODE completeLast() throws IOException
    {
        NODE node = stack.removeLast();
        complete(node);
        return node;
    }

    abstract void complete(NODE value) throws IOException;
    abstract public PartialTail makePartialRoot() throws IOException;

    static class PTail implements PartialTail
    {
        long root;
        long cutoff;
        long count;
        ByteBuffer tail;

        @Override
        public long root()
        {
            return root;
        }

        @Override
        public long cutoff()
        {
            return cutoff;
        }

        @Override
        public ByteBuffer tail()
        {
            return tail;
        }

        @Override
        public long count()
        {
            return count;
        }
    }

    static abstract class BaseNode<VALUE, NODE extends BaseNode<VALUE, NODE>> implements SerializationNode<VALUE>, Iterable<NODE>
    {
        private static final int CHILDREN_LIST_RECYCLER_LIMIT = 1024;
        @SuppressWarnings("rawtypes")
        private static final LightweightRecycler<Object[]> CHILDREN_LIST_RECYCLER = ThreadLocals.createLightweightRecycler(CHILDREN_LIST_RECYCLER_LIMIT);
        @SuppressWarnings("rawtypes")
        private static final Object[] EMPTY_LIST = new Object[0];

        @SuppressWarnings({ "unchecked", "rawtypes" })
        private static Object[] allocateChildrenList()
        {
            return CHILDREN_LIST_RECYCLER.reuseOrAllocate(() -> new Object[4]);
        }

        private static void recycleChildrenList(Object[] children)
        {
            CHILDREN_LIST_RECYCLER.tryRecycle(children);
        }

        VALUE payload;
        Object[] children;
        int childCount;
        final int transition;
        long filePos = -1;

        @SuppressWarnings("unchecked")
        BaseNode(int transition)
        {
            children = null;
            childCount = 0;
            this.transition = transition;
        }

        public VALUE payload()
        {
            return payload;
        }

        public VALUE setPayload(VALUE newPayload)
        {
            VALUE p = payload;
            payload = newPayload;
            return p;
        }

        public NODE addChild(byte b)
        {
            assert childCount == 0 || (transition(childCount - 1) & 0xFF) < (b & 0xFF);
            NODE node = newNode(b);
            if (children == null)
                children = allocateChildrenList();

            if (childCount == children.length)
                children = Arrays.copyOf(children, childCount * 2);
            children[childCount++] = node;
            return node;
        }

        public int childCount()
        {
            return childCount;
        }

        void finalizeWithPosition(long position)
        {
            this.filePos = position;

            // Make sure we are not holding on to pointers to data we no longer need
            // (otherwise we keep the whole trie in memory).
            if (children != null)
                // the recycler will also clear the collection before adding it to the pool
                recycleChildrenList(children);

            children = null;
            payload = null;
        }

        public NODE child(int i)
        {
            return (NODE) children[i];
        }

        public int transition(int i)
        {
            return child(i).transition;
        }

        public Iterator<NODE> iterator()
        {
            return new Iterator<NODE>() {
                int i = 0;

                public boolean hasNext()
                {
                    return i < childCount;
                }

                public NODE next()
                {
                    return child(i++);
                }
            };
        }

        @Override
        public String toString()
        {
            return String.format("%02x", transition);
        }

        abstract NODE newNode(byte transition);
    }
}
