package org.apache.cassandra.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.memory.AbstractAllocator;

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
public abstract class AbstractClusteringBound extends AbstractBufferClusteringPrefix
{
    public static final AbstractClusteringBound.Serializer serializer = new Serializer();

    protected AbstractClusteringBound(Kind kind, ByteBuffer[] values)
    {
        super(kind, values);
        assert values.length > 0 || !kind.isBoundary();
    }

    public static AbstractClusteringBound create(Kind kind, ByteBuffer[] values)
    {
        return kind.isBoundary()
                ? new ClusteringBoundary(kind, values)
                : new ClusteringBound(kind, values);
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

    public static ClusteringBound inclusiveOpen(boolean reversed, ByteBuffer[] boundValues)
    {
        return new ClusteringBound(reversed ? Kind.INCL_END_BOUND : Kind.INCL_START_BOUND, boundValues);
    }

    public static ClusteringBound exclusiveOpen(boolean reversed, ByteBuffer[] boundValues)
    {
        return new ClusteringBound(reversed ? Kind.EXCL_END_BOUND : Kind.EXCL_START_BOUND, boundValues);
    }

    public static ClusteringBound inclusiveClose(boolean reversed, ByteBuffer[] boundValues)
    {
        return new ClusteringBound(reversed ? Kind.INCL_START_BOUND : Kind.INCL_END_BOUND, boundValues);
    }

    public static ClusteringBound exclusiveClose(boolean reversed, ByteBuffer[] boundValues)
    {
        return new ClusteringBound(reversed ? Kind.EXCL_START_BOUND : Kind.EXCL_END_BOUND, boundValues);
    }

    public static ClusteringBoundary inclusiveCloseExclusiveOpen(boolean reversed, ByteBuffer[] boundValues)
    {
        return new ClusteringBoundary(reversed ? Kind.EXCL_END_INCL_START_BOUNDARY : Kind.INCL_END_EXCL_START_BOUNDARY, boundValues);
    }

    public static ClusteringBoundary exclusiveCloseInclusiveOpen(boolean reversed, ByteBuffer[] boundValues)
    {
        return new ClusteringBoundary(reversed ? Kind.INCL_END_EXCL_START_BOUNDARY : Kind.EXCL_END_INCL_START_BOUNDARY, boundValues);
    }

    public AbstractClusteringBound copy(AbstractAllocator allocator)
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
    public abstract AbstractClusteringBound invert();

    public static class Serializer
    {
        public void serialize(AbstractClusteringBound bound, DataOutputPlus out, int version, List<AbstractType<?>> types) throws IOException
        {
            out.writeByte(bound.kind().ordinal());
            out.writeShort(bound.size());
            ClusteringPrefix.serializer.serializeValuesWithoutSize(bound, out, version, types);
        }

        public long serializedSize(AbstractClusteringBound bound, int version, List<AbstractType<?>> types)
        {
            return 1 // kind ordinal
                 + TypeSizes.sizeof((short)bound.size())
                 + ClusteringPrefix.serializer.valuesWithoutSizeSerializedSize(bound, version, types);
        }

        public AbstractClusteringBound deserialize(DataInputPlus in, int version, List<AbstractType<?>> types) throws IOException
        {
            Kind kind = Kind.values()[in.readByte()];
            return deserializeValues(in, kind, version, types);
        }

        public AbstractClusteringBound deserializeValues(DataInputPlus in, Kind kind, int version,
                List<AbstractType<?>> types) throws IOException
        {
            int size = in.readUnsignedShort();
            if (size == 0)
                return kind.isStart() ? ClusteringBound.BOTTOM : ClusteringBound.TOP;

            ByteBuffer[] values = ClusteringPrefix.serializer.deserializeValuesWithoutSize(in, size, version, types);
            return create(kind, values);
        }
    }
}