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

package org.apache.cassandra.db.compaction;

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public class ShardManagerTrivial implements ShardManager
{
    private final IPartitioner partitioner;

    public ShardManagerTrivial(IPartitioner partitioner)
    {
        this.partitioner = partitioner;
    }

    @Override
    public double rangeSpanned(Range<Token> tableRange)
    {
        return 1;
    }

    @Override
    public double localSpaceCoverage()
    {
        return 1;
    }

    ShardIterator iterator = new ShardIterator()
    {
        @Override
        public Token shardStart()
        {
            return partitioner.getMinimumToken();
        }

        @Override
        public Token shardEnd()
        {
            return partitioner.getMinimumToken();
        }

        @Override
        public Range<Token> shardSpan()
        {
            return new Range<>(partitioner.getMinimumToken(), partitioner.getMinimumToken());
        }

        @Override
        public double shardSpanSize()
        {
            return 1;
        }

        @Override
        public boolean advanceTo(Token nextToken)
        {
            return false;
        }

        @Override
        public int count()
        {
            return 1;
        }

        @Override
        public double fractionInShard(Range<Token> targetSpan)
        {
            return 1;
        }

        @Override
        public double rangeSpanned(PartitionPosition first, PartitionPosition last)
        {
            return 1;
        }

        @Override
        public int shardIndex()
        {
            return 0;
        }
    };

    @Override
    public ShardIterator boundaries(int count)
    {
        return iterator;
    }
}
