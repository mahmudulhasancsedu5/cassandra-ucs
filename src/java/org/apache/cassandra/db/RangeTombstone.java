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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.memory.AbstractAllocator;


/**
 * A range tombstone is a tombstone that covers a slice/range of rows.
 * <p>
 * Note that in most of the storage engine, a range tombstone is actually represented by its separated
 * opening and closing bound, see {@link RangeTombstoneMarker}. So in practice, this is only used when
 * full partitions are materialized in memory in a {@code Partition} object, and more precisely through
 * the use of a {@code RangeTombstoneList} in a {@code DeletionInfo} object.
 */
public class RangeTombstone
{
    private final Slice slice;
    private final DeletionTime deletion;

    public RangeTombstone(Slice slice, DeletionTime deletion)
    {
        this.slice = slice;
        this.deletion = deletion;
    }

    /**
     * The slice of rows that is deleted by this range tombstone.
     *
     * @return the slice of rows that is deleted by this range tombstone.
     */
    public Slice deletedSlice()
    {
        return slice;
    }

    /**
     * The deletion time for this (range) tombstone.
     *
     * @return the deletion time for this range tombstone.
     */
    public DeletionTime deletionTime()
    {
        return deletion;
    }

    public String toString(ClusteringComparator comparator)
    {
        return slice.toString(comparator) + '@' + deletion;
    }

    @Override
    public boolean equals(Object other)
    {
        if(!(other instanceof RangeTombstone))
            return false;

        RangeTombstone that = (RangeTombstone)other;
        return this.deletedSlice().equals(that.deletedSlice())
            && this.deletionTime().equals(that.deletionTime());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(deletedSlice(), deletionTime());
    }

    /**
     * The bound of a range tombstone.
     * <p>
     * This is the same than for a slice but it includes "boundaries" between ranges. A boundary simply condensed
     * a close and an opening "bound" into a single object. There is 2 main reasons for these "shortcut" boundaries:
     *   1) When merging multiple iterators having range tombstones (that are represented by their start and end markers),
     *      we need to know when a range is close on an iterator, if it is reopened right away. Otherwise, we cannot
     *      easily produce the markers on the merged iterators within risking to fail the sorting guarantees of an
     *      iterator. See this comment for more details: https://goo.gl/yyB5mR.
     *   2) This saves some storage space.
     */
    public static abstract class Bound extends AbstractBufferClusteringPrefix
    {
        public static final Serializer serializer = new Serializer();

        protected Bound(Kind kind, ByteBuffer[] values)
        {
            super(kind, values);
            assert values.length > 0 || !kind.isBoundary();
        }

        public static Bound create(Kind kind, ByteBuffer[] values)
        {
            return kind.isBoundary()
                    ? new Boundary(kind, values)
                    : new Slice.Bound(kind, values);
        }

        public boolean isBoundary()
        {
            return kind.isBoundary();
        }

        public boolean isOpen(boolean reversed)
        {
            return kind.isOpen(reversed);
        }

        public boolean isClose(boolean reversed)
        {
            return kind.isClose(reversed);
        }

        public static Slice.Bound inclusiveOpen(boolean reversed, ByteBuffer[] boundValues)
        {
            return new Slice.Bound(reversed ? Kind.INCL_END_BOUND : Kind.INCL_START_BOUND, boundValues);
        }

        public static Slice.Bound exclusiveOpen(boolean reversed, ByteBuffer[] boundValues)
        {
            return new Slice.Bound(reversed ? Kind.EXCL_END_BOUND : Kind.EXCL_START_BOUND, boundValues);
        }

        public static Slice.Bound inclusiveClose(boolean reversed, ByteBuffer[] boundValues)
        {
            return new Slice.Bound(reversed ? Kind.INCL_START_BOUND : Kind.INCL_END_BOUND, boundValues);
        }

        public static Slice.Bound exclusiveClose(boolean reversed, ByteBuffer[] boundValues)
        {
            return new Slice.Bound(reversed ? Kind.EXCL_START_BOUND : Kind.EXCL_END_BOUND, boundValues);
        }

        public static Boundary inclusiveCloseExclusiveOpen(boolean reversed, ByteBuffer[] boundValues)
        {
            return new Boundary(reversed ? Kind.EXCL_END_INCL_START_BOUNDARY : Kind.INCL_END_EXCL_START_BOUNDARY, boundValues);
        }

        public static Boundary exclusiveCloseInclusiveOpen(boolean reversed, ByteBuffer[] boundValues)
        {
            return new Boundary(reversed ? Kind.INCL_END_EXCL_START_BOUNDARY : Kind.EXCL_END_INCL_START_BOUNDARY, boundValues);
        }

        public RangeTombstone.Bound copy(AbstractAllocator allocator)
        {
            ByteBuffer[] newValues = new ByteBuffer[size()];
            for (int i = 0; i < size(); i++)
                newValues[i] = allocator.clone(get(i));
            return create(kind(), newValues);
        }

        public String toString(CFMetaData metadata)
        {
            return toString(metadata.comparator);
        }

        public String toString(ClusteringComparator comparator)
        {
            StringBuilder sb = new StringBuilder();
            sb.append(kind()).append('(');
            for (int i = 0; i < size(); i++)
            {
                if (i > 0)
                    sb.append(", ");
                sb.append(comparator.subtype(i).getString(get(i)));
            }
            return sb.append(')').toString();
        }

        /**
         * Returns the inverse of the current bound.
         * <p>
         * This invert both start into end (and vice-versa) and inclusive into exclusive (and vice-versa).
         *
         * @return the invert of this bound. For instance, if this bound is an exlusive start, this return
         * an inclusive end with the same values.
         */
        public abstract Bound invert();

        public static class Serializer
        {
            public void serialize(RangeTombstone.Bound bound, DataOutputPlus out, int version, List<AbstractType<?>> types) throws IOException
            {
                out.writeByte(bound.kind().ordinal());
                out.writeShort(bound.size());
                ClusteringPrefix.serializer.serializeValuesWithoutSize(bound, out, version, types);
            }

            public long serializedSize(RangeTombstone.Bound bound, int version, List<AbstractType<?>> types)
            {
                return 1 // kind ordinal
                     + TypeSizes.sizeof((short)bound.size())
                     + ClusteringPrefix.serializer.valuesWithoutSizeSerializedSize(bound, version, types);
            }

            public RangeTombstone.Bound deserialize(DataInputPlus in, int version, List<AbstractType<?>> types) throws IOException
            {
                Kind kind = Kind.values()[in.readByte()];
                return deserializeValues(in, kind, version, types);
            }

            public RangeTombstone.Bound deserializeValues(DataInputPlus in, Kind kind, int version,
                    List<AbstractType<?>> types) throws IOException
            {
                int size = in.readUnsignedShort();
                if (size == 0)
                    return kind.isStart() ? Slice.Bound.BOTTOM : Slice.Bound.TOP;

                ByteBuffer[] values = ClusteringPrefix.serializer.deserializeValuesWithoutSize(in, size, version, types);
                return create(kind, values);
            }
        }
    }

    public static class Boundary extends Bound
    {
        protected Boundary(Kind kind, ByteBuffer[] values)
        {
            super(kind, values);
        }

        public static Boundary create(Kind kind, ByteBuffer[] values)
        {
            assert kind.isBoundary();
            return new Boundary(kind, values);
        }

        @Override
        public Boundary invert()
        {
            return create(kind().invert(), values);
        }

        @Override
        public Boundary copy(AbstractAllocator allocator)
        {
            return (Boundary) super.copy(allocator);
        }

        public Slice.Bound openBound(boolean reversed)
        {
            return Slice.Bound.create(kind.openBoundOfBoundary(reversed), values);
        }

        public Slice.Bound closeBound(boolean reversed)
        {
            return Slice.Bound.create(kind.closeBoundOfBoundary(reversed), values);
        }
    }
}
