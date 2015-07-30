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
package org.apache.cassandra.db;

import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.db.filter.DataLimits;

public class Counting
{
    public static class FilteredPartitionCounter extends PartitionCounter implements Transformer.PartitionFunction
    {
        public FilteredPartitionCounter(DataLimits.Counter counter)
        {
            super(counter);
        }

        public RowIterator applyToPartition(RowIterator partition)
        {
            counter.newPartition(partition.partitionKey(), partition.staticRow());
            return Transformer.apply(partition, rowCounter);
        }
    }

    public static class UnfilteredPartitionCounter extends PartitionCounter implements Transformer.UnfilteredPartitionFunction
    {
        public UnfilteredPartitionCounter(DataLimits.Counter counter)
        {
            super(counter);
        }

        public UnfilteredRowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            counter.newPartition(partition.partitionKey(), partition.staticRow());
            return Transformer.apply(partition, rowCounter);
        }
    }

    private static abstract class PartitionCounter implements Transformer.EarlyTermination
    {
        protected final DataLimits.Counter counter;
        final RowCounter rowCounter;

        PartitionCounter(DataLimits.Counter counter)
        {
            this.counter = counter;
            this.rowCounter = new RowCounter(counter);
        }

        public boolean terminate()
        {
            return counter.isDone();
        }
    }

    public static class RowCounter implements Transformer.RowFunction, Transformer.EarlyTermination, Transformer.RunOnClose
    {
        final DataLimits.Counter counter;

        RowCounter(DataLimits.Counter counter)
        {
            this.counter = counter;
        }

        public DataLimits.Counter counter()
        {
            return counter;
        }

        @Override
        public boolean terminate()
        {
            return counter.isDoneForPartition();
        }

        @Override
        public Row applyToRow(Row row)
        {
            counter.newRow(row);
            return row;
        }

        @Override
        public void runOnClose()
        {
            counter.endOfPartition();
        }
    }

    public static PartitionIterator count(PartitionIterator iter, DataLimits.Counter counter)
    {
        return Transformer.apply(iter, new FilteredPartitionCounter(counter));
    }

    public static PartitionIterator count(PartitionIterator iter, DataLimits limits, int nowInSecs)
    {
        return count(iter, limits.newCounter(nowInSecs, true));
    }

    public static UnfilteredPartitionIterator count(UnfilteredPartitionIterator iter, DataLimits.Counter counter)
    {
        return Transformer.apply(iter, new UnfilteredPartitionCounter(counter));
    }

    public static UnfilteredPartitionIterator count(UnfilteredPartitionIterator iter, DataLimits limits, int nowInSecs)
    {
        return count(iter, limits.newCounter(nowInSecs, false));
    }

    public static RowIterator count(RowIterator iter, DataLimits.Counter counter)
    {
        return Transformer.apply(iter, new RowCounter(counter));
    }

    public static RowIterator count(RowIterator iter, DataLimits limits, int nowInSecs)
    {
        return count(iter, limits.newCounter(nowInSecs, false));
    }

    public static UnfilteredRowIterator count(UnfilteredRowIterator iter, DataLimits.Counter counter)
    {
        return Transformer.apply(iter, new RowCounter(counter));
    }

    public static UnfilteredRowIterator count(UnfilteredRowIterator iter, DataLimits limits, int nowInSecs)
    {
        return count(iter, limits.newCounter(nowInSecs, false));
    }
}
