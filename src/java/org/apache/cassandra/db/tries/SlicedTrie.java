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
import org.apache.cassandra.utils.bytecomparable.ByteSource;

public class SlicedTrie<T> extends Trie<T>
{
    private final Trie<T> source;

    /** Left-side boundary. The characters of this are requested as we descend along the left-side boundary. */
    private final ByteComparable left;

    /** Right-side boundary. The characters of this are requested as we descend along the right-side boundary. */
    private final ByteComparable right;

    private final boolean includeLeft;
    private final boolean includeRight;

    public SlicedTrie(Trie<T> source, ByteComparable left, boolean includeLeft, ByteComparable right, boolean includeRight)
    {
        this.source = source;
        this.left = left;
        this.right = right;
        this.includeLeft = includeLeft;
        this.includeRight = includeRight;
    }

    @Override
    protected Cursor<T> cursor()
    {
        return new SlicedCursor(this);
    }

    private enum State
    {
        BEFORE_LEFT,
        INSIDE,
        AFTER_RIGHT;
    }

    private static class SlicedCursor<T> implements Cursor<T>
    {
        private final ByteSource left;
        private final ByteSource right;
        private final boolean includeLeft;
        private final boolean excludeRight;
        private final Cursor<T> source;

        State state;
        int leftNext;
        int leftLevel;
        int rightNext;
        int rightLevel;

        public SlicedCursor(SlicedTrie<T> slicedTrie)
        {
            source = slicedTrie.source.cursor();
            if (slicedTrie.left != null)
            {
                left = slicedTrie.left.asComparableBytes(ByteComparable.Version.OSS41);
                includeLeft = slicedTrie.includeLeft;
                leftNext = left.next();
                leftLevel = 1;
                if (leftNext == ByteSource.END_OF_STREAM && includeLeft)
                    state = State.INSIDE;
                else
                    state = State.BEFORE_LEFT;
            }
            else
            {
                left = null;
                includeLeft = true;
                state = State.INSIDE;
            }

            if (slicedTrie.right != null)
            {
                right = slicedTrie.right.asComparableBytes(ByteComparable.Version.OSS41);
                excludeRight = !slicedTrie.includeRight;
                rightNext = right.next();
                rightLevel = 1;
                if (rightNext == ByteSource.END_OF_STREAM && excludeRight)
                    state = State.AFTER_RIGHT;
            }
            else
            {
                right = null;
                excludeRight = true;
                rightLevel = 0;
            }
        }

        @Override
        public int advance()
        {
            if (state == State.AFTER_RIGHT)
                return -1;

            int newLevel = source.advance();
            int transition = source.incomingTransition();

            if (state == State.BEFORE_LEFT)
            {
                // Skip any transitions before the left bound
                while (newLevel == leftLevel && transition < leftNext)
                {
                    newLevel = source.ascend();
                    transition = source.incomingTransition();
                }

                // Check if we are still following the left bound
                if (newLevel == leftLevel && transition == leftNext)
                {
                    assert leftNext != ByteSource.END_OF_STREAM;
                    leftNext = left.next();
                    ++leftLevel;
                    if (leftNext == ByteSource.END_OF_STREAM && includeLeft)
                        state = State.INSIDE; // report the content on the left bound
                }
                else // otherwise we are beyond it
                    state = State.INSIDE;
            }

            return checkRightBound(newLevel, transition);
        }

        private int markDone()
        {
            state = State.AFTER_RIGHT;
            return -1;
        }

        private int checkRightBound(int newLevel, int transition)
        {
            // Cursor positions compare by level descending and transition ascending.
            if (newLevel > rightLevel)
                return newLevel;
            if (newLevel < rightLevel)
                return markDone();
            // newLevel == rightLevel
            if (transition < rightNext)
                return newLevel;
            if (transition > rightNext)
                return markDone();

            // Following right bound
            rightNext = right.next();
            ++rightLevel;
            if (rightNext == ByteSource.END_OF_STREAM && excludeRight)
                return markDone();  // do not report any content on the right bound
            return newLevel;
        }

        @Override
        public int advanceMultiple(TransitionsReceiver receiver)
        {
            switch (state)
            {
                case AFTER_RIGHT:
                    return -1;
                case BEFORE_LEFT:
                    return advance();   // walk one byte at a time to skip items before left boundary
                case INSIDE:
                    int level = source.level();
                    if (level < rightLevel)
                        return advance();   // need to check next byte against right boundary
                    int newLevel = source.advanceMultiple(receiver);
                    if (newLevel > level)
                        return newLevel;    // successfully advanced
                    // we ascended, check if we are still within boundaries
                    return checkRightBound(newLevel, source.incomingTransition());
                default:
                    throw new AssertionError();
            }
        }

        @Override
        public int ascend()
        {
            if (state == State.AFTER_RIGHT)
                return -1;

            // We are either inside or following the left bound. In the latter case ascend takes us beyond it.
            state = State.INSIDE;
            return checkRightBound(source.ascend(), source.incomingTransition());
        }

        @Override
        public int level()
        {
            return state == State.AFTER_RIGHT ? -1 : source.level();
        }

        @Override
        public int incomingTransition()
        {
            return source.incomingTransition();
        }

        @Override
        public T content()
        {
            return state == State.INSIDE ? source.content() : null;
        }
    }
}
