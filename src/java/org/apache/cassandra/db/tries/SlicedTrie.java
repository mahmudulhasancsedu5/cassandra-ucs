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
        private final Cursor<T> source;

        State state;
        int leftNext;
        int leftLevel;
        int rightNext;
        int rightLevel;

        public SlicedCursor(SlicedTrie<T> slicedTrie)
        {
            source = slicedTrie.source.cursor();
            ByteSource leftSource = null;
            if (slicedTrie.left != null)
            {
                leftSource = slicedTrie.left.asComparableBytes(ByteComparable.Version.OSS41);
                if (!slicedTrie.includeLeft)
                    leftSource = ByteSource.nextKey(leftSource);
                leftNext = leftSource.next();
            }
            else
                leftNext = ByteSource.END_OF_STREAM;
            left = leftSource;
            leftLevel = 1;

            ByteSource rightSource = null;
            if (slicedTrie.right != null)
            {
                rightSource = slicedTrie.right.asComparableBytes(ByteComparable.Version.OSS41);
                if (slicedTrie.includeRight)
                    rightSource = ByteSource.nextKey(rightSource);
                rightNext = rightSource.next();
            }
            else
                rightNext = 256;
            right = rightSource;
            rightLevel = 1;

            if (rightNext == ByteSource.END_OF_STREAM)
                state = State.AFTER_RIGHT;
            else if (leftNext == ByteSource.END_OF_STREAM)
                state = State.INSIDE;
            else
                state = State.BEFORE_LEFT;
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
                // Check left bound
                while (newLevel == leftLevel && transition < leftNext)
                {
                    newLevel = source.ascend();
                    transition = source.incomingTransition();
                }

                if (newLevel == leftLevel && transition == leftNext)
                {
                    // Still following the left bound
                    assert leftNext != ByteSource.END_OF_STREAM;
                    leftNext = left.next();
                    ++leftLevel;
                    if (leftNext == ByteSource.END_OF_STREAM)
                        state = State.INSIDE; // include the left bound
                }
                else
                    state = State.INSIDE;
            }

            return checkRightBound(newLevel, transition);
        }

        private int done()
        {
            state = State.AFTER_RIGHT;
            return -1;
        }

        private int checkRightBound(int newLevel, int transition)
        {
            if (newLevel > rightLevel)
                return newLevel;
            if (newLevel < rightLevel)
                return done();
            // newLevel == rightLevel
            if (transition < rightNext)
                return newLevel;
            if (transition > rightNext)
                return done();

            // Following right bound
            rightNext = right.next();
            ++rightLevel;
            if (rightNext == ByteSource.END_OF_STREAM)
                return done();  // exclude the right bound
            return newLevel;
        }

        @Override
        public int ascend()
        {
            if (state == State.AFTER_RIGHT)
                return -1;

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
