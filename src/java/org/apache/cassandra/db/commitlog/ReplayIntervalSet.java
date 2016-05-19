package org.apache.cassandra.db.commitlog;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.google.common.collect.ImmutableSortedMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.ISerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

public class ReplayIntervalSet
{
    public static final ReplayIntervalSet EMPTY = new ReplayIntervalSet(ImmutableSortedMap.<ReplayPosition, ReplayPosition>of()); 

    final NavigableMap<ReplayPosition, ReplayPosition> ranges;

    private ReplayIntervalSet(ImmutableSortedMap<ReplayPosition, ReplayPosition> ranges)
    {
        this.ranges = ranges;
    }

    public ReplayIntervalSet(ReplayPosition start, ReplayPosition end)
    {
        this(ImmutableSortedMap.of(start, end));
    }

    public boolean contains(ReplayPosition position)
    {
        // replay ranges are inclusive
        Map.Entry<ReplayPosition, ReplayPosition> range = ranges.floorEntry(position);
        return range != null && position.compareTo(range.getValue()) <= 0;
    }

    public boolean isEmpty()
    {
        return ranges.isEmpty();
    }

    public ReplayPosition lowerBound()
    {
        return isEmpty() ? ReplayPosition.NONE : ranges.firstKey();
    }

    public ReplayPosition upperBound()
    {
        return isEmpty() ? ReplayPosition.NONE : ranges.lastEntry().getValue();
    }

    public String toString()
    {
        return ranges.toString();
    }

    @Override
    public int hashCode()
    {
        return ranges.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        return obj instanceof ReplayIntervalSet && ranges.equals(((ReplayIntervalSet) obj).ranges);
    }

    public static final ISerializer<ReplayIntervalSet> serializer = new ISerializer<ReplayIntervalSet>()
    {
        public void serialize(ReplayIntervalSet intervals, DataOutputPlus out) throws IOException
        {
            out.writeInt(intervals.ranges.size());
            for (Map.Entry<ReplayPosition, ReplayPosition> en : intervals.ranges.entrySet())
            {
                ReplayPosition.serializer.serialize(en.getKey(), out);
                ReplayPosition.serializer.serialize(en.getValue(), out);
            }
        }

        public ReplayIntervalSet deserialize(DataInput in) throws IOException
        {
            int count = in.readInt();
            NavigableMap<ReplayPosition, ReplayPosition> ranges = new TreeMap<>();
            for (int i = 0; i < count; ++i)
                ranges.put(ReplayPosition.serializer.deserialize(in), ReplayPosition.serializer.deserialize(in));
            return new ReplayIntervalSet(ImmutableSortedMap.copyOfSorted(ranges));
        }

        public long serializedSize(ReplayIntervalSet intervals, TypeSizes typeSizes)
        {
            long size = typeSizes.sizeof(intervals.ranges.size());
            for (Map.Entry<ReplayPosition, ReplayPosition> en : intervals.ranges.entrySet())
            {
                size += ReplayPosition.serializer.serializedSize(en.getKey(), typeSizes);
                size += ReplayPosition.serializer.serializedSize(en.getValue(), typeSizes);
            }
            return size;
        }
    };

    static public class Builder
    {
        final NavigableMap<ReplayPosition, ReplayPosition> ranges;

        public Builder()
        {
            this.ranges = new TreeMap<>();
        }

        public Builder(ReplayPosition start, ReplayPosition end)
        {
            this();
            assert start.compareTo(end) <= 0;
            ranges.put(start, end);
        }

        public void add(ReplayPosition start, ReplayPosition end)
        {
            assert start.compareTo(end) <= 0;
            // extend ourselves to cover any ranges we overlap
            // record directly preceding our end may extend past us, so take the max of our end and its
            Map.Entry<ReplayPosition, ReplayPosition> extend = ranges.floorEntry(end);
            if (extend != null && extend.getValue().compareTo(end) > 0)
                end = extend.getValue();

            // record directly preceding our start may extend into us; if it does, we take it as our start
            extend = ranges.lowerEntry(start);
            if (extend != null && extend.getValue().compareTo(start) >= 0)
                start = extend.getKey();

            ranges.subMap(start, end).clear();
            ranges.put(start, end);
        }

        public void addAll(ReplayIntervalSet otherSet)
        {
            for (Map.Entry<ReplayPosition, ReplayPosition> en : otherSet.ranges.entrySet())
            {
                add(en.getKey(), en.getValue());
            }
        }

        public ReplayIntervalSet build()
        {
            return new ReplayIntervalSet(ImmutableSortedMap.copyOfSorted(ranges));
        }
    }

}