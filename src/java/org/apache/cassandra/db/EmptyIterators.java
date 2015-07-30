/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.util.NoSuchElementException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;

public class EmptyIterators
{

    private static class EmptyBasePartitionIterator<R extends BaseRowIterator<?>> implements BasePartitionIterator<R>
    {
        EmptyBasePartitionIterator()
        {
        }

        public void close()
        {
        }

        public boolean hasNext()
        {
            return false;
        }

        public R next()
        {
            throw new NoSuchElementException();
        }
    }

    private static class EmptyUnfilteredPartitionIterator extends EmptyBasePartitionIterator<UnfilteredRowIterator> implements UnfilteredPartitionIterator
    {
        final CFMetaData metadata;
        final boolean isForThrift;

        public EmptyUnfilteredPartitionIterator(CFMetaData metadata, boolean isForThrift)
        {
            this.metadata = metadata;
            this.isForThrift = isForThrift;
        }

        public boolean isForThrift()
        {
            return isForThrift;
        }

        public CFMetaData metadata()
        {
            return metadata;
        }
    }

    private static class EmptyPartitionIterator extends EmptyBasePartitionIterator<RowIterator> implements PartitionIterator
    {
        public static final EmptyPartitionIterator instance = new EmptyPartitionIterator();
        private EmptyPartitionIterator()
        {
            super();
        }
    }

    private static class EmptyBaseRowIterator<U extends Unfiltered> implements BaseRowIterator<U>
    {
        final CFMetaData metadata;
        final DecoratedKey partitionKey;
        final boolean isReverseOrder;

        public EmptyBaseRowIterator(CFMetaData metadata, DecoratedKey partitionKey, boolean isReverseOrder)
        {
            this.metadata = metadata;
            this.partitionKey = partitionKey;
            this.isReverseOrder = isReverseOrder;
        }

        public CFMetaData metadata()
        {
            return metadata;
        }

        public boolean isReverseOrder()
        {
            return isReverseOrder;
        }

        public PartitionColumns columns()
        {
            return PartitionColumns.NONE;
        }

        public DecoratedKey partitionKey()
        {
            return partitionKey;
        }

        public Row staticRow()
        {
            return Rows.EMPTY_STATIC_ROW;
        }

        public void close()
        {
        }

        public boolean isEmpty()
        {
            return true;
        }

        public boolean hasNext()
        {
            return false;
        }

        public U next()
        {
            throw new NoSuchElementException();
        }
    }

    private static class EmptyUnfilteredRowIterator extends EmptyBaseRowIterator<Unfiltered> implements UnfilteredRowIterator
    {
        public EmptyUnfilteredRowIterator(CFMetaData metadata, DecoratedKey partitionKey, boolean isReverseOrder)
        {
            super(metadata, partitionKey, isReverseOrder);
        }

        public DeletionTime partitionLevelDeletion()
        {
            return DeletionTime.LIVE;
        }

        public EncodingStats stats()
        {
            return EncodingStats.NO_STATS;
        }
    }

    private static class EmptyRowIterator extends EmptyBaseRowIterator<Row> implements RowIterator
    {
        public EmptyRowIterator(CFMetaData metadata, DecoratedKey partitionKey, boolean isReverseOrder)
        {
            super(metadata, partitionKey, isReverseOrder);
        }
        public EmptyRowIterator(EmptyBaseRowIterator<?> iterator)
        {
            this(iterator.metadata, iterator.partitionKey, iterator.isReverseOrder);
        }
    }

    public static UnfilteredPartitionIterator unfilteredPartition(CFMetaData metadata)
    {
        // TODO: is false always correct for isThrift?
        return new EmptyUnfilteredPartitionIterator(metadata, false);
    }

    public static PartitionIterator partition()
    {
        return EmptyPartitionIterator.instance;
    }

    public static UnfilteredRowIterator unfilteredRow(CFMetaData metadata, DecoratedKey partitionKey, boolean isReverseOrder)
    {
        return new EmptyUnfilteredRowIterator(metadata, partitionKey, isReverseOrder);
    }

    public static RowIterator row(CFMetaData metadata, DecoratedKey partitionKey, boolean isReverseOrder)
    {
        return new EmptyRowIterator(metadata, partitionKey, isReverseOrder);
    }

    public static boolean isEmpty(UnfilteredPartitionIterator iterator)
    {
        return iterator instanceof EmptyUnfilteredPartitionIterator;
    }

    public static boolean isEmpty(PartitionIterator iterator)
    {
        return iterator instanceof EmptyPartitionIterator;
    }

    public static boolean isEmpty(UnfilteredRowIterator iterator)
    {
        return iterator instanceof EmptyUnfilteredRowIterator;
    }

    public static boolean isEmpty(RowIterator iterator)
    {
        return iterator instanceof EmptyRowIterator;
    }
}
