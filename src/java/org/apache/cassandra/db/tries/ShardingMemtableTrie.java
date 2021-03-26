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

import org.apache.cassandra.config.Config;
import org.apache.cassandra.io.compress.BufferType;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class ShardingMemtableTrie<T> implements WritableTrie<T>
{
    static final int DIRECT_MAX = 2;
    static final int SIZE_LIMIT = Integer.parseInt(System.getProperty(
    Config.PROPERTY_PREFIX + "shard_size", String.valueOf(4))) * 1024 * 1024;

    volatile Trie<T> merged;
    volatile LimitedSizeMemtableTrie<T>[] shards;
    volatile MemtableTrie<LimitedSizeMemtableTrie<T>> top;

    public ShardingMemtableTrie(BufferType bufferType)
    {
        LimitedSizeMemtableTrie<T> first = new LimitedSizeMemtableTrie<>(bufferType, SIZE_LIMIT);
        top = new MemtableTrie<>(bufferType);
        merged = first;
        shards = new LimitedSizeMemtableTrie[]{ first };
        first.index = 0;
        try
        {
            top.putRecursive(ByteComparable.fixedLength(new byte[0]), first, (a, b) -> b);
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
            return getFloor(top, path).get(path);
    }

    private static <V> V getFloor(MemtableTrie<V> trie, ByteComparable path)
    {
        int node = trie.root;
        ByteSource src = path.asComparableBytes(MemtableTrie.BYTE_COMPARABLE_VERSION);
        int lesserPath = MemtableTrie.NONE;
        // Follow path tracking closest lower branch
        while (!MemtableTrie.isNull(node))
        {
            int c = src.next();
            lesserPath = trie.lesserChild(node, c, lesserPath);  // also works for c < 0, getting content only
            if (c == ByteSource.END_OF_STREAM)
                break;
            node = trie.getChild(node, c);
        }
        // Follow the max of the lower branch.
        while (!MemtableTrie.isNullOrLeaf(lesserPath))
            lesserPath = trie.lesserChild(lesserPath, 0x100, Integer.MIN_VALUE);

        assert lesserPath != Integer.MIN_VALUE
            : "Branch without children in trie for " +  path.byteComparableAsString(MemtableTrie.BYTE_COMPARABLE_VERSION);
        assert MemtableTrie.isLeaf(lesserPath)
            : "ConcurrentTrie has no match for " + path.byteComparableAsString(MemtableTrie.BYTE_COMPARABLE_VERSION);

        return trie.getContent(~lesserPath);
    }

////    private T walkInParallel(ByteComparable path)
////    {
////        int shardCount = shards.length;
////        ShardWithNode<T>[] nodes = new ShardWithNode[shardCount];
////        int nodeCount = 0;
////        for (MemtableTrie<T> shard : shards)
////        {
////            int root = shard.root;
////            if (root != MemtableTrie.NONE)
////                nodes[nodeCount++] = new ShardWithNode(shard, root);
////        }
////        ByteSource src = path.asComparableBytes(TrieMemtable.BYTE_COMPARABLE_VERSION);
////        int c = src.next();
////        while (c != ByteSource.END_OF_STREAM)
////        {
////            int destCount = 0;
////            for (int i = 0; i < nodeCount; ++i)
////            {
////                int node = nodes[i].trie.getChild(nodes[i].node, c);
////                if (node != MemtableTrie.NONE)
////                {
////                    nodes[i].node = node;
////                    nodes[destCount++] = nodes[i];
////                }
////            }
////            if (destCount == 0)
////                return null;
////            nodeCount = destCount;
////            c = src.next();
////        }
////        for (int i = 0; i < nodeCount; ++i)
////        {
////            T content = nodes[i].trie.getNodeContent(nodes[i].node);
////            if (content != null)
////                return content;
////        }
////        return null;
////    }
//
//    static class ShardWithNode<T>
//    {
//        MemtableTrie<T> trie;
//        int node;
//
//        public ShardWithNode(MemtableTrie<T> shard, int root)
//        {
//            this.trie = shard;
//            this.node = root;
//        }
//    }

    public <R> AllocatedMemory putConcurrent(ByteComparable key, R value, MemtableTrie.UpsertTransformer<T, ? super R> transformer, boolean useRecursive)
    {
        LimitedSizeMemtableTrie<T> shard = getFloor(top, key);
        AllocatedMemory allocated = null;
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
                        return add(allocated, shard.putConcurrent(key, value, transformer, useRecursive));
                    }
                }
                catch (MemtableTrie.SpaceExhaustedException e)
                {
                    allocated = add(allocated, splitShard(shard, key));
                }
            }
            // called on markedForRemoval
            shard = getFloor(top, key);
        }
    }

    AllocatedMemory add(AllocatedMemory prev, AllocatedMemory curr)
    {
        if (prev == null)
            return curr;
        return new AllocatedMemory(prev.onHeap + curr.onHeap, prev.offHeap + curr.offHeap);
    }


    private AllocatedMemory splitShard(LimitedSizeMemtableTrie<T> shard, ByteComparable key)
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
            left.copyInto(slst);
            LimitedSizeMemtableTrie<T> right = new LimitedSizeMemtableTrie<>(shard.bufferType,
                                                                             SIZE_LIMIT,
                                                                             SIZE_LIMIT,
                                                                             shard.valuesCount());
            right.copyInto(shard.subtrie(slst.limit, true, null, false));

            // Update list and merged trie.
            synchronized (this)
            {
                LimitedSizeMemtableTrie<T>[] newShards = Arrays.copyOf(shards, shards.length + 1);
                right.index = shards.length;
                newShards[right.index] = right;
                left.index = shard.index;
                newShards[left.index] = left;

                long topOnHeap = top.sizeOnHeap();
                long topOffHeap = top.sizeOffHeap();
                MemtableTrie<LimitedSizeMemtableTrie<T>> newTop = top.duplicateAdjustingContent(c -> c == shard ? left : c);
                newTop.putRecursive(slst.limit, right, (a, b) -> b);

                AllocatedMemory allocatedMemory = new AllocatedMemory(
                left.sizeOnHeap() + right.sizeOnHeap() + top.sizeOnHeap() - shard.sizeOnHeap() - topOnHeap,
                left.sizeOffHeap() + right.sizeOffHeap() + top.sizeOffHeap() - shard.sizeOffHeap() - topOffHeap);

                top = newTop; // makes the split visible to put and get
                merged = Trie.merge(Arrays.asList(newShards), Trie.throwingResolver()); // makes the split visible to iteration
                shards = newShards; // makes the split visible to sizing
//                System.err.println(String.format("Trie size %s into %s + %s top size %s",
//                                                 FBUtilities.prettyPrintMemory(shard.allocatedSize()),
//                                                 FBUtilities.prettyPrintMemory(left.allocatedSize()),
//                                                 FBUtilities.prettyPrintMemory(right.allocatedSize()),
//                                                 FBUtilities.prettyPrintMemory(top.allocatedSize())));
                return allocatedMemory;
            }

            // Return the shard the key belongs to after the split.
            //ByteComparable.compare(key, slst.limit, MemtableTrie.BYTE_COMPARABLE_VERSION) < 0 ? left : right;
        }
        catch (MemtableTrie.SpaceExhaustedException e)
        {
            throw new AssertionError(e); // Should never happen.
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
