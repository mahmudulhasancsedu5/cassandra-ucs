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

import org.apache.cassandra.utils.bytecomparable.ByteComparable;

public interface WritableTrie<T>
{
    public T get(ByteComparable path);

    class AllocatedMemory
    {
        public final long onHeap;
        public final long offHeap;

        public AllocatedMemory(long onHeap, long offHeap)
        {
            this.onHeap = onHeap;
            this.offHeap = offHeap;
        }
    }

    public <R> AllocatedMemory putConcurrent(ByteComparable key,
                                             R value,
                                             MemtableTrie.UpsertTransformer<T, ? super R> transformer,
                                             boolean useRecursive)
    throws MemtableTrie.SpaceExhaustedException;

    public long sizeOnHeap();
    public long sizeOffHeap();

    public boolean reachedAllocatedSizeThreshold();

    public int valuesCount();

    default public boolean isEmpty()
    {
        return valuesCount() == 0;
    }

    default public Trie<T> asTrie()
    {
        return subtrie(null, false, null, false);
    }
    public Trie<T> subtrie(ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight);
}
