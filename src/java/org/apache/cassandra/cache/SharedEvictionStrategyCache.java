package org.apache.cassandra.cache;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.cache.EvictionStrategy.Entry;
import org.apache.cassandra.cache.EvictionStrategy.RemovalListener;
import org.apache.cassandra.cache.EvictionStrategy.Specials;
import org.apache.cassandra.cache.EvictionStrategy.Weigher;

public class SharedEvictionStrategyCache<Key, Value> implements ICache<Key, Value>, EvictionStrategy.EntryOwner
{

    final ConcurrentMap<Key, Entry> map;
    final EvictionStrategy strategy;

    public static<Key, Value>
    SharedEvictionStrategyCache<Key, Value> create(RemovalListener removalListener,
                                                   Weigher weigher,
                                                   long initialCapacity)
    {
        return create(new EvictionStrategyLirsSync(removalListener, weigher, initialCapacity));
    }

    public static<Key, Value>
    SharedEvictionStrategyCache<Key, Value> create(EvictionStrategy evictionStrategy)
    {
        return new SharedEvictionStrategyCache<Key, Value>
        (new ConcurrentHashMap<>(),
         evictionStrategy);
    }

    private SharedEvictionStrategyCache(ConcurrentMap<Key, Entry> map,
                                        EvictionStrategy strategy)
    {
        assert map.isEmpty();
        this.map = map;
        this.strategy = strategy;
    }

    @Override
    public void put(Key key, Value value)
    {
    main:
        for ( ; ; )
        {
            Entry e = map.get(key);
            if (e == null)
            {
                Entry ne = strategy.elementFor(key, this);
                e = map.putIfAbsent(key, ne);
                if (e == null)
                    e = ne;
            }

            Object old;
            for ( ; ; )
            {
                old = e.value();
                if (old == value)
                    return;     // someone's done our job
                if (old == Specials.DISCARDED)
                {
                    Thread.yield();
                    continue main;   // If the value is in the process of being removed, retry adding as the caller may have seen it as removed.
                }
                if (e.casValue(old, value))
                    break;
                Thread.yield();
            }
            break main;
        }
    }

    @Override
    public boolean putIfAbsent(Key key, Value value)
    {
        Entry ne = strategy.elementFor(key, this);
        Entry e;
        while (true)
        {
            e = map.putIfAbsent(key, ne);
            if (e == null)
                e = ne;

            if (e.casValue(Specials.EMPTY, value))
                break;
            else if (e.value() != Specials.DISCARDED || e == ne)
                return false;

            // If the value is in the process of being removed, retry adding as the caller may have seen it as removed.
            Thread.yield();
        }

        return true;
    }

    @Override
    public boolean replace(Key key, Value old, Value value)
    {
        assert old != null;
        assert value != null;

        Entry e = map.get(key);
        if (e == null)
            return false;

        if (!e.casValue(old, value))
            return false;

        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Value get(Key key)
    {
        Entry e = map.get(key);
        if (e == null)
            return null;
        Object v = e.value();
        if (EvictionStrategy.isSpecial(v))
            return null;
        e.access();
        return (Value) v;
    }

    @Override
    public void remove(Key key)
    {
        Entry e = map.get(key);
        if (e == null)
            return;
        e.remove();
    }

    @Override
    public boolean removeMapping(Entry e)
    {
        assert e.value() == Specials.DISCARDED;
        return map.remove(e.key(), e);
    }

    @Override
    public Iterator<Key> keyIterator()
    {
        return map.keySet().iterator();
    }

    @Override
    public Iterator<Key> hotKeyIterator(int n)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsKey(Key key)
    {
        Entry e = map.get(key);
        if (e == null)
            return false;
        return !EvictionStrategy.isSpecial(e.value());
    }

    @Override
    public void clear()
    {
        for (Entry e : map.values())
            e.remove();
    }

    public String toString()
    {
        return getClass().getSimpleName() + "-" + strategy.getClass().getSimpleName();
    }

    @Override
    public long capacity()
    {
        return strategy.capacity();
    }

    @Override
    public void setCapacity(long capacity)
    {
        strategy.setCapacity(capacity);
    }

    @Override
    public int size()
    {
        return map.size();
    }

    @Override
    public long weightedSize()
    {
        return strategy.weightedSize();
    }
}
