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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

import com.google.common.collect.ImmutableList;

import org.agrona.concurrent.UnsafeBuffer;
import org.apache.cassandra.utils.bytecomparable.ByteComparable;

/**
 * Base class for tries.
 *
 * Normal users of tries will only use the public transformation methods, which various transformations of the trie
 * and conversion of its content to other formats (e.g. iterable of values).
 *
 * For any unimplemented data extraction operations one can rely on the TrieWalker (to aggregate value) and TrieIterator
 * (to iterator) base classes, which provide the necessary mechanisms to handle walking the trie.
 *
 * The internal representation of tries using this interface is defined in the Node interface.
 *
 * Its design is largely defined by the requirement for iteratively retrieving content of the trie, for which it needs
 * to be able to represent and save the state of any traversal efficiently, so that it can be preserved while a consumer
 * is operating on an item. This also enables a possible future extension to support asynchronous retrieval of nodes.
 *
 * To enable that efficient state representation, the nodes that are used to retrieve the internal state of the trie are
 * light stateful objects and always contain a link to some parent state. The role of parent state can often be played
 * by the parent node, because its current state defines the transition that was taken to obtain the child, and it also
 * has a reference to its own parent, effectively building a stack of nodes left to process each holding its own state.
 * It is also possible to skip some levels of the descent in the state description, if e.g. there are no other paths to
 * examine at those levels to continue the traversal (see getUniqueDescendant). Some traversal types may require more
 * information (e.g. position in a character array or list of nodes being merged). The type of parent state link is
 * defined by the consumer through the generic parameter L and it is supplied by the consumer as an argument to the
 * getCurrentChild call -- that parent state is presented by the child in its parentLink field.
 *
 *
 * To begin traversal over a trie, one must retrieve the root node of the trie by calling root(). Because the nodes are
 * stateful, the traversal must always proceed from one thread. Should concurrent reads be required, separate calls to
 * root() must be made.
 *
 * Once a node is available, one can retrieve any associated content and list the children of the node along with their
 * associated transition byte by:
 * - calling startIteration to set the node's state to its first child
 * - retrieving the associated transition byte using the node's currentTransition field
 * - optionally retrieving the child using getCurrentChild giving it something you can use to restore your state
 *   to continue processing the rest of the children of this node
 * - when processing the child is complete/skipped or child is null, request the next child using advanceIteration and
 *   repeat
 * - if start/advanceIteration return null, there are no further children of the node
 * - if they return Remaining.ONE, this is the last child of the node (the inverse is not always true, nodes will try
 *   but do not guarantee they will report ONE on their last child)
 * - when the children are exhausted, use the node's parent link to restore your state to what it was when the relevant
 *   parent was being processed
 * For an example of simple traversal, see TrieWalker. For a more complex traversal example, refer to TrieValuesIterator
 * and TrieEntriesIterator.
 *
 * Note: This model only supports depth-first traversals. We do not currently have a need for breadth-first walks.
 *
 * @param <T> The content type of the trie. Content is only allowed on leaf nodes.
 */
public abstract class Trie<T>
{
    /**
     * Enum used to indicate the presence of more children during the iteration of a node.
     * Generally iteration will return null or MULTIPLE, but it can return ONE if it is known that there are no further
     * children to optimize walks.
     */
    protected enum Remaining
    {
        ONE, MULTIPLE
    }

    /**
     * Used by {@link Cursor#advanceMultiple} to feed the transitions taken.
     */
    protected interface TransitionsReceiver
    {
        /** Add a single byte to the path. */
        void add(int t);
        /** Add the count bytes from position pos at the given buffer. */
        void add(UnsafeBuffer b, int pos, int count);
    }

    /**
     * Used by {@link Cursor#advanceToContent} to track the transitions and backtracking taken.
     */
    interface ResettingTransitionsReceiver extends TransitionsReceiver
    {
        /** Delete all bytes beyond the given length. */
        void reset(int newLength);
    }

    // Cursor-style walks
    interface Cursor<T>
    {
        /**
         * Advance one position.
         * This can be either:
         * - descending one level
         * - ascending to the closest parent that has remaining children, and then descending one level
         * @return level (can be prev+1 or <=prev), -1 means done
         */
        int advance();

        /**
         * Advance, descending multiple levels if that does not require extra work (e.g. chain nodes)
         * Receiver will be given all transitions taken except the last; i.e. on an ascend it will not receive any
         *
         * @param receiver
         * @return
         */
        default int advanceMultiple(TransitionsReceiver receiver)
        {
            return advance();
        }

        default T advanceToContent(ResettingTransitionsReceiver receiver) // advances all the way (to next content)
        {
            int prevLevel = level();
            while (true)
            {
                int currLevel = advanceMultiple(receiver);
                if (currLevel <= 0)
                    return null;
                if (receiver != null)
                {
                    if (currLevel <= prevLevel)
                        receiver.reset(currLevel - 1);
                    receiver.add(incomingTransition());
                }
                T content = content();
                if (content != null)
                    return content;
                prevLevel = currLevel;
            }
        }

        /**
         * ignore the remaining children at this level or below and ascend to parent and advance
         */
        int ascend(); // ignore the remaining children at this level or below and ascend to parent and advance

        int level(); // return current state; if just starting / on root, return 0
        int incomingTransition(); // return the last transition taken; if just starting / on root, return -1
        T content(); // return content -- may be non-null on root

    }

    protected abstract Cursor<T> cursor();

    // Version of the byte comparable conversion to use for all operations
    static final ByteComparable.Version BYTE_COMPARABLE_VERSION = ByteComparable.Version.OSS41;

    public String dump()
    {
        return dump(Object::toString);
    }

    public String dump(Function<T, String> contentToString)
    {
        return TrieDumper.process(contentToString, this);
    }

    /**
     * Returns a singleton trie mapping the given byte path to content.
     */
    public static <T> Trie<T> singleton(ByteComparable b, T v)
    {
        return new SingletonTrie<>(b, v);
    }

    /**
     * Returns a view of the subtrie containing everything in this trie whose keys fall between the given boundaries.
     *
     * This method will throw an assertion error if the bounds provided are not correctly ordered, including with
     * respect to the `includeLeft` and `includeRight` constraints (i.e. subtrie(x, false, x, false) is an invalid call
     * but subtrie(x, true, x, false) is inefficient but fine for an empty subtrie).
     *
     * @param left the left bound for the returned subtrie. If {@code null}, the resulting subtrie is not left-bounded.
     * @param includeLeft whether {@code left} is an inclusive bound of not.
     * @param right the right bound for the returned subtrie. If {@code null}, the resulting subtrie is not right-bounded.
     * @param includeRight whether {@code right} is an inclusive bound of not.
     * @return a view of the subtrie containing all the keys of this trie falling between {@code left} (inclusively if
     * {@code includeLeft}) and {@code right} (inclusively if {@code includeRight}).
     */
    public Trie<T> subtrie(ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight)
    {
        if (left == null && right == null)
            return this;

        return new SetIntersectionTrie<>(this, TrieSet.range(left, includeLeft, right, includeRight));
    }

    /**
     * Returns the ordered entry set of this trie's content as an iterable.
     */
    public Iterable<Map.Entry<ByteComparable, T>> entrySet()
    {
        return this::entryIterator;
    }

    /**
     * Returns the ordered entry set of this trie's content in an iterator.
     */
    public Iterator<Map.Entry<ByteComparable, T>> entryIterator()
    {
        return new TrieEntriesIterator.AsEntries<>(this);
    }

    /**
     * Returns the ordered set of values of this trie as an iterable.
     */
    public Iterable<T> values()
    {

        return this::valueIterator;
    }

    /**
     * Returns the ordered set of values of this trie in an iterator.
     */
    public Iterator<T> valueIterator()
    {
        return new TrieValuesIterator<>(this);
    }

    /**
     * Returns the values in any order. For some tries this is much faster than the ordered iterable.
     */
    public Iterable<T> valuesUnordered()
    {
        return values();
    }

    /**
     * Resolver of content of merged nodes, used for two-source merges (i.e. mergeWith).
     */
    public interface MergeResolver<T>
    {
        // Note: No guarantees about argument order.
        // E.g. during t1.mergeWith(t2, resolver), resolver may be called with t1 or t2's items as first argument.
        T resolve(T b1, T b2);
    }

    /**
     * Constructs a view of the merge of this trie with the given one. The view is live, i.e. any write to any of the
     * sources will be reflected in the merged view.
     *
     * If there is content for a given key in both sources, the resolver will be called to obtain the combination.
     * (The resolver will not be called if there's content from only one source.)
     */
    public Trie<T> mergeWith(Trie<T> other, MergeResolver<T> resolver)
    {
        return new MergeTrie<>(resolver, this, other);
    }

    /**
     * Resolver of content of merged nodes.
     *
     * The resolver's methods are only called if more than one of the merged nodes contain content, and the
     * order in which the arguments are given is not defined. Only present non-null values will be included in the
     * collection passed to the resolving methods.
     *
     * Can also be used as a two-source resolver.
     */
    public interface CollectionMergeResolver<T> extends MergeResolver<T>
    {
        T resolve(Collection<T> contents);

        default T resolve(T c1, T c2)
        {
            return resolve(ImmutableList.of(c1, c2));
        }
    }

    private static final CollectionMergeResolver<Object> THROWING_RESOLVER = new CollectionMergeResolver<Object>()
    {
        public Object resolve(Collection contents)
        {
            throw error();
        }

        private AssertionError error()
        {
            throw new AssertionError("Entries must be distinct.");
        }
    };

    /**
     * Returns a resolver that throws whenever more than one of the merged nodes contains content.
     * Can be used to merge tries that are known to have distinct content paths.
     */
    public static <T> CollectionMergeResolver<T> throwingResolver()
    {
        return (CollectionMergeResolver<T>) THROWING_RESOLVER;
    }

    /**
     * Constructs a view of the merge of multiple tries. The view is live, i.e. any write to any of the
     * sources will be reflected (eventually consistently) in the merged view.
     *
     * If there is content for a given key in more than one sources, the resolver will be called to obtain the combination.
     * (The resolver will not be called if there's content from only one source.)
     */
    public static <T> Trie<T> merge(Collection<? extends Trie<T>> sources, CollectionMergeResolver<T> resolver)
    {
        switch (sources.size())
        {
        case 0:
            return empty();
        case 1:
            return sources.iterator().next();
        case 2:
        {
            Iterator<? extends Trie<T>> it = sources.iterator();
            Trie<T> t1 = it.next();
            Trie<T> t2 = it.next();
            return t1.mergeWith(t2, resolver);
        }
        default:
            return new CollectionMergeTrie<>(sources, resolver);
        }
    }

    /**
     * Constructs a view of the merge of multiple tries, where each source must have distinct keys. The view is live,
     * i.e. any write to any of the sources will be reflected in the merged view.
     *
     * If there is content for a given key in more than one sources, the merge will throw an assertion error.
     */
    public static <T> Trie<T> mergeDistinct(Collection<? extends Trie<T>> sources)
    {
        switch (sources.size())
        {
        case 0:
            return empty();
        case 1:
            return sources.iterator().next();
        case 2:
        {
            Iterator<? extends Trie<T>> it = sources.iterator();
            Trie<T> t1 = it.next();
            Trie<T> t2 = it.next();
            return new MergeTrie.Distinct<>(t1, t2);
        }
        default:
            return new CollectionMergeTrie.Distinct<>(sources);
        }
    }

    private static final Trie<Object> EMPTY = new Trie<Object>()
    {
        protected Cursor<Object> cursor()
        {
            return new Cursor<Object>()
            {
                public int advance()
                {
                    return -1;
                }

                public int ascend()
                {
                    return -1;
                }

                public int level()
                {
                    return -1;
                }

                public Object content()
                {
                    return null;
                }

                public int incomingTransition()
                {
                    return -1;
                }
            };
        }
    };

    @SuppressWarnings("unchecked")
    public static <T> Trie<T> empty()
    {
        return (Trie<T>) EMPTY;
    }
}
