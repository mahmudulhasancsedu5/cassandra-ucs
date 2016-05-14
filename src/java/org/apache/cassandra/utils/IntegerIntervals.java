package org.apache.cassandra.utils;

import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import com.google.common.primitives.Longs;

public class IntegerIntervals
{

    public static long make(int lower, int upper)
    {
        assert lower <= upper;
        return ((lower & 0xFFFFFFFFL) << 32) | upper & 0xFFFFFFFFL;
    }

    public static int lower(long interval)
    {
        return (int) (interval >>> 32);
    }

    public static int upper(long interval)
    {
        return (int) interval;
    }

    public static void expandToCover(AtomicLong interval, int value)
    {
        long prev;
        int lower;
        int upper;
        do
        {
            prev = interval.get();
            upper = upper(prev);
            lower = lower(prev);
            if (value > upper) // common case
                upper = value;
            else if (value < lower)
                lower = value;
        }
        while (!interval.compareAndSet(prev, make(lower, upper)));
    }

    public static<K> void coverInMap(ConcurrentMap<K, AtomicLong> map, K key, int value)
    {
        AtomicLong i = map.get(key);
        if (i == null)
        {
            i = map.putIfAbsent(key, new AtomicLong(make(value, value)));
            if (i == null)
                // success
                return;
        }
        expandToCover(i, value);
    }

    public static String toString(long interval)
    {
        return "(" + lower(interval) + ";" + upper(interval) + ")";
    }


    public static class Set
    {
        static long[] EMPTY = new long[0];

        private volatile long[] ranges = EMPTY;

        public synchronized void add(int start, int end)
        {
            assert start <= end;

            // extend ourselves to cover any ranges we overlap
            // record directly preceding our end may extend past us, so take the max of our end and its
            int rpos = Arrays.binarySearch(ranges, ((end & 0xFFFFFFFFL) << 32) | 0xFFFFFFFFL);        // floor (i.e. greatest <=) of the end position
            if (rpos < 0)
                rpos = (-1 - rpos) - 1;
            if (rpos >= 0)
            {
                int extend = upper(ranges[rpos]);
                if (extend > end)
                    end = extend;
            }

            // record directly preceding our start may extend into us; if it does, we take it as our start
            int lpos = Arrays.binarySearch(ranges, ((start & 0xFFFFFFFFL) << 32) | 0); // lower (i.e. greatest <) of the start position
            if (lpos < 0)
                lpos = -1 - lpos;
            lpos -= 1;
            if (lpos >= 0)
            {
                if (upper(ranges[lpos]) >= start)
                {
                    start = lower(ranges[lpos]);
                    --lpos;
                }
            }

            long[] newRanges = new long[ranges.length - (rpos - lpos) + 1];
            int dest = 0;
            for (int i = 0; i <= lpos; ++i)
                newRanges[dest++] = ranges[i];
            newRanges[dest++] = make(start, end);
            for (int i = rpos + 1; i < ranges.length; ++i)
                newRanges[dest++] = ranges[i];
            ranges = newRanges;
        }

        public boolean covers(long l)
        {
            return covers(lower(l), upper(l));
        }

        public boolean covers(int start, int end)
        {
            int rpos = Arrays.binarySearch(ranges, ((start & 0xFFFFFFFFL) << 32) | 0xFFFFFFFFL);        // floor (i.e. greatest <=) of the end position
            if (rpos < 0)
                rpos = (-1 - rpos) - 1;
            if (rpos == -1)
                return false;
            return upper(ranges[rpos]) >= end;
        }

        public Integer lowerBound()
        {
            return lower(ranges[0]);
        }

        public Integer upperBound()
        {
            long[] ranges = this.ranges; // take local copy to avoid risk of it changing in the midst of operation
            return upper(ranges[ranges.length - 1]);
        }

        public Collection<Long> intervals()
        {
            return Longs.asList(ranges);
        }

        public String toString()
        {
            return "[" + intervals().stream().map(IntegerIntervals::toString).collect(Collectors.joining(", ")) + "]";
        }
    }
}
