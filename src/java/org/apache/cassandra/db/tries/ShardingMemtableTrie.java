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

package org.apache.cassandra.db.tries;

import java.util.Arrays;

import org.apache.cassandra.db.memtable.TrieMemtable;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class ShardingMemtableTrie<T> implements WritableTrie<T>
{
    static final int DIRECT_MAX = 2;
    static final int SIZE_LIMIT = 32 * 1024;// * 1024;

    volatile LimitedSizeMemtableTrie<T>[] shards;
    volatile Trie<T> merged;
    final MemtableTrie<Integer> top;

    public ShardingMemtableTrie(BufferType bufferType)
    {
        LimitedSizeMemtableTrie<T> first = new LimitedSizeMemtableTrie<>(bufferType, SIZE_LIMIT);
        shards = new LimitedSizeMemtableTrie[1];
        first.index = 0;
        shards[first.index] = first;
        merged = first;
        top = new MemtableTrie<>(bufferType);
        try
        {
            top.putRecursive(ByteComparable.fixedLength(new byte[0]), first.index, (a, b) -> b);
        }
        catch (MemtableTrie.SpaceExhaustedException e)
        {
            throw new AssertionError(); // too small for this to be possible
        }
    }


    public T get(ByteComparable path)
    {
//        if (shardCount <= DIRECT_MAX)
//            return walkInParallel(path);
//        else
            return getFloor(path).get(path);
    }

    private LimitedSizeMemtableTrie<T> getFloor(ByteComparable path)
    {
        if (shards.length == 1)
            return shards[0];

        int node = top.root;
        ByteSource src = path.asComparableBytes(MemtableTrie.BYTE_COMPARABLE_VERSION);
        int lesserPath = MemtableTrie.NONE;
        // Follow path tracking closest lower branch
        while (!MemtableTrie.isNull(node))
        {
            int c = src.next();
            lesserPath = top.lesserChild(node, c, lesserPath);  // also works for c < 0, getting content only
            if (c == ByteSource.END_OF_STREAM)
                break;
            node = top.getChild(node, c);
        }
        // Follow the max of the lower branch.
        while (!MemtableTrie.isNullOrLeaf(lesserPath))
            lesserPath = top.lesserChild(lesserPath, 0x100, Integer.MIN_VALUE);

        assert lesserPath != Integer.MIN_VALUE
            : "Branch without children in trie for " +  path.byteComparableAsString(MemtableTrie.BYTE_COMPARABLE_VERSION);
        assert MemtableTrie.isLeaf(lesserPath)
            : "ConcurrentTrie has no match for " + path.byteComparableAsString(MemtableTrie.BYTE_COMPARABLE_VERSION);

        return shards[top.getContent(~lesserPath)];
    }

    private T walkInParallel(ByteComparable path)
    {
        int shardCount = shards.length;
        ShardWithNode<T>[] nodes = new ShardWithNode[shardCount];
        int nodeCount = 0;
        for (MemtableTrie<T> shard : shards)
        {
            int root = shard.root;
            if (root != MemtableTrie.NONE)
                nodes[nodeCount++] = new ShardWithNode(shard, root);
        }
        ByteSource src = path.asComparableBytes(TrieMemtable.BYTE_COMPARABLE_VERSION);
        int c = src.next();
        while (c != ByteSource.END_OF_STREAM)
        {
            int destCount = 0;
            for (int i = 0; i < nodeCount; ++i)
            {
                int node = nodes[i].trie.getChild(nodes[i].node, c);
                if (node != MemtableTrie.NONE)
                {
                    nodes[i].node = node;
                    nodes[destCount++] = nodes[i];
                }
            }
            if (destCount == 0)
                return null;
            nodeCount = destCount;
            c = src.next();
        }
        for (int i = 0; i < nodeCount; ++i)
        {
            T content = nodes[i].trie.getNodeContent(nodes[i].node);
            if (content != null)
                return content;
        }
        return null;
    }

    static class ShardWithNode<T>
    {
        MemtableTrie<T> trie;
        int node;

        public ShardWithNode(MemtableTrie<T> shard, int root)
        {
            this.trie = shard;
            this.node = root;
        }
    }

    public <R> void putSingleton(ByteComparable key, R value, MemtableTrie.UpsertTransformer<T, ? super R> transformer)
    {
        LimitedSizeMemtableTrie<T> shard = getFloor(key);
        while (true)
        {
            synchronized (shard)
            {
                try
                {
                    // putRecursive is not guaranteed to need more space. Check if the shard has been split while we
                    // were waiting for synchronization
                    if (!shard.markedForRemoval)
                    {
                        shard.putSingleton(key, value, transformer);
                        return;
                    }
                    else
                        shard = getFloor(key);
                }
                catch (MemtableTrie.SpaceExhaustedException e)
                {
                    shard = splitShard(shard, key);
                }
            }
        }
    }

    public <R> void putRecursive(ByteComparable key, R value, MemtableTrie.UpsertTransformer<T, ? super R> transformer)
    {
        LimitedSizeMemtableTrie<T> shard = getFloor(key);
        while (true)
        {
            synchronized (shard)
            {
                try
                {
                    // putRecursive is not guaranteed to need more space. Check if the shard has been split while we
                    // were waiting for synchronization
                    if (!shard.markedForRemoval)
                    {
                        shard.putRecursive(key, value, transformer);
                        return;
                    }
                    else
                        shard = getFloor(key);
                }
                catch (MemtableTrie.SpaceExhaustedException e)
                {
                    shard = splitShard(shard, key);
                }
            }
        }
    }

    private LimitedSizeMemtableTrie<T> splitShard(LimitedSizeMemtableTrie<T> shard, ByteComparable key)
    {
        // This must be called with a lock held on shard
        if (!shard.markForRemoval())
            throw new AssertionError();

        try
        {
            // Split shard
            LimitedSizeMemtableTrie<T> left = new LimitedSizeMemtableTrie<>(shard.bufferType,
                                                                            SIZE_LIMIT,
                                                                            SIZE_LIMIT,
                                                                            shard.valuesCount());
            SizeLimitedSubTrie<T> slst = new SizeLimitedSubTrie<>(shard, left, SIZE_LIMIT / 2);
            left.apply(slst, (a, b) -> b);
            LimitedSizeMemtableTrie<T> right = new LimitedSizeMemtableTrie<>(shard.bufferType,
                                                                             SIZE_LIMIT,
                                                                             SIZE_LIMIT,
                                                                             shard.valuesCount());
            right.apply(shard.subtrie(slst.limit, true, null, false), (a, b) -> b);

            // Update list and merged trie.
            synchronized (this)
            {
                LimitedSizeMemtableTrie<T>[] newShards = Arrays.copyOf(shards, shards.length + 1);
                right.index = shards.length;
                newShards[right.index] = right;

                left.index = shard.index;
                shards = newShards; // concurrent reads might use new shards. This must still contain old shard until we update the trie mapping
                top.putRecursive(slst.limit, right.index, (a, b) -> b);  // we are open to concurrent reads and writes on right
                newShards[left.index] = left;   // we are open to concurrent reads and writes on left
                shards = newShards; // shards is a volatile -- this ensures that getFloor will return the up-to-date value for left.index
                merged = Trie.merge(Arrays.asList(newShards), Trie.throwingResolver()); // concurrent walks now read new shards
            }

            // Return the shard the key belongs to after the split.
            return ByteComparable.compare(key, slst.limit, MemtableTrie.BYTE_COMPARABLE_VERSION) < 0 ? left : right;
        }
        catch (MemtableTrie.SpaceExhaustedException e)
        {
            throw new AssertionError(); // Should never happen.
        }
    }

    static class SizeLimitedSubTrie<T> extends Trie<T>
    {
        final Trie<T> source;
        final MemtableTrie<T> destination; // the thing that grows in size
        final int sizeLimit;
        ByteComparable limit;

        SizeLimitedSubTrie(Trie<T> source, MemtableTrie<T> destination, int sizeLimit)
        {
            this.source = source;
            this.destination = destination;
            this.sizeLimit = sizeLimit;
        }

        protected <L> Node<T, L> root()
        {
            return new SLSTNode<>(source.root(), null);
        }

        ByteComparable makeLimit(SLSTNode<?> node)
        {
            int size = 0;
            for (SLSTNode<?> n = node; n != null; n = n.wrapped.parentLink)
                ++size;

            byte[] path = new byte[size];
            for (SLSTNode<?> n = node; n != null; n = n.wrapped.parentLink)
                path[--size] = (byte) n.currentTransition;
            assert size == 0;

            return ByteComparable.fixedLength(path);
        }

        class SLSTNode<L> extends Node<T, L>
        {
            final Trie.Node<T, SLSTNode<L>> wrapped;

            public SLSTNode(Node<T, SLSTNode<L>> wrapped, L parentLink)
            {
                super(parentLink);
                this.wrapped = wrapped;
            }

            public Remaining startIteration()
            {
                Remaining result = wrapped.startIteration();
                currentTransition = wrapped.currentTransition;
                return result;
            }

            public Remaining advanceIteration()
            {
                if (limit != null)  // we are already stopped
                    return null;

                Remaining result = wrapped.advanceIteration();
                currentTransition = wrapped.currentTransition;

                if (result != null && destination.allocatedSize() >= sizeLimit)
                {
                    limit = makeLimit(this);
                    return null;
                }

                return result;
            }

            public Node<T, L> getCurrentChild(L parentLink)
            {
                Node<T, SLSTNode<L>> child = wrapped.getCurrentChild(this);
                return child != null ? new SLSTNode<L>(child, parentLink) : null;
            }

            public T content()
            {
                return wrapped.content();
            }
        }
    }

    public long sizeOnHeap()
    {
        long size = 0;
        for (MemtableTrie<T> shard : shards)
            size += shard.sizeOnHeap();
        return size;
    }

    public long sizeOffHeap()
    {
        long size = 0;
        for (MemtableTrie<T> shard : shards)
            size += shard.sizeOffHeap();
        return size;
    }

    public boolean reachedAllocatedSizeThreshold()
    {
        // We split when we reached a high size, can't ever hit this.
        return false;
    }

    public int valuesCount()
    {
        int count = 0;
        for (MemtableTrie<T> shard : shards)
            count += shard.valuesCount();
        return count;
    }

    public Trie<T> subtrie(ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight)
    {
        return merged.subtrie(left, includeLeft, right, includeRight);
    }
}
