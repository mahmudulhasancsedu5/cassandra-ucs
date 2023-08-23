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

public class IntersectionTrie<T> extends Trie<T>
{
    enum SetState
    {
        PREFIX, // no content()
        IN_SET; // content() == true
        // outside of set: no node
    }

    final Trie<T> trie;
    final Trie<Boolean> set;

    public IntersectionTrie(Trie<T> trie, Trie<Boolean> set)
    {
        this.trie = trie;
        this.set = set;
    }

    @Override
    protected Cursor<T> cursor()
    {
        return new IntersectionCursor(trie.cursor(), set.cursor());
    }

    private static class IntersectionCursor<T> implements Cursor<T>
    {
        final Cursor<T> source;
        final Cursor<Boolean> set;

        public IntersectionCursor(Cursor<T> source, Cursor<Boolean> set)
        {
            this.source = source;
            this.set = set;
        }

        @Override
        public int depth()
        {
            return source.depth();
        }

        @Override
        public int incomingTransition()
        {
            return source.incomingTransition();
        }

        @Override
        public T content()
        {
            return set.content() != null ? source.content() : null;
        }

        @Override
        public int advance()
        {
            // we are starting with both positioned on the same node
            int depth = source.advance();
            int transition = source.incomingTransition();
            return advanceToIntersection(depth, transition);
        }

        private int advanceToIntersection(int depth, int transition)
        {
            int setDepth = set.skipTo(depth, transition);
            int setTransition = set.incomingTransition();
            while (setDepth != depth || (depth != -1 && setTransition != transition))
            {
                depth = source.skipTo(setDepth, setTransition);
                transition = source.incomingTransition();
                if (setDepth != depth || (depth != -1 && setTransition != transition))
                {
                    setDepth = set.skipTo(depth, transition);
                    setTransition = set.incomingTransition();
                }
            }
            return depth;
        }

        @Override
        public int skipTo(int skipDepth, int skipTransition)
        {
            int depth = source.skipTo(skipDepth, skipTransition);
            int transition = source.incomingTransition();
            return advanceToIntersection(depth, transition);
        }
    }
}
