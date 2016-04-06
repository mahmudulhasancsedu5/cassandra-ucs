package org.apache.cassandra.cache;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.cassandra.cache.EvictionStrategy.Entry;
import org.apache.cassandra.cache.EvictionStrategy.Specials;
import org.apache.cassandra.cache.EvictionStrategy.Weigher;

public class SharedEvictionStrategyCache<Key, Value> implements ICache<Key, Value>, EvictionStrategy.EntryOwner
{

    final ConcurrentMap<Key, Entry> map;
    final EvictionStrategy strategy;
    final RemovalListener<Key, Value> removalListener;
    final Class<Key> keyType;
    final Class<Value> valueType;

    interface RemovalListener<Key, Value>
    {
        void remove(Key key, Value value);
    }

    public static<Key, Value>
    SharedEvictionStrategyCache<Key, Value> create(Class<Key> keyType,
                                                   Class<Value> valueType,
                                                   RemovalListener<Key, Value> removalListener,
                                                   Weigher weigher,
                                                   long initialCapacity)
    {
        return create(keyType, valueType,
                      removalListener,
                      new EvictionStrategyFifo(weigher, initialCapacity));
    }

    public static<Key, Value>
    SharedEvictionStrategyCache<Key, Value> create(
            Class<Key> keyType,
            Class<Value> valueType,
            RemovalListener<Key, Value> removalListener,
            EvictionStrategy evictionStrategy)
    {
        return new SharedEvictionStrategyCache<Key, Value>
        (keyType, valueType,
         new ConcurrentHashMap<>(),
         evictionStrategy,
         removalListener);
    }

    private SharedEvictionStrategyCache(Class<Key> keyType,
                                        Class<Value> valueType,
                                        ConcurrentMap<Key, Entry> map,
                                        EvictionStrategy strategy,
                                        RemovalListener<Key, Value> removalListener)
    {
        assert map.isEmpty();
        this.map = map;
        this.strategy = strategy;
        this.removalListener = removalListener;
        this.keyType = keyType;
        this.valueType = valueType;
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
            if (valueType.isInstance(old))
                removalListener.remove(keyType.cast(key), valueType.cast(old));
            break main;
        }
        strategy.maybeEvict();
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

            if (!e.casValue(Specials.EMPTY, value))
            {
                if (e.value() != Specials.DISCARDED)
                    return false;

                // If the value is in the process of being removed, retry adding as the caller may have seen it as removed.
                Thread.yield();
                continue;
            }

            strategy.maybeEvict();
            return true;
        }
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

        removalListener.remove(key, old);
        strategy.maybeEvict();
        return true;
    }

    @Override
    public Value get(Key key)
    {
        Entry e = map.get(key);
        if (e == null)
            return null;
        Object v = e.value();
        if (!valueType.isInstance(v))
            return null;
        e.access();
        return valueType.cast(v);
    }

    @Override
    public void remove(Key key)
    {
        Entry e = map.get(key);
        if (e == null)
            return;
        // as evict below, except cast of key
        Object old = e.remove();
        if (valueType.isInstance(old))
            removalListener.remove(key, valueType.cast(old));
    }

    @Override
    public void evict(Entry e)
    {
        Object old = e.remove();
        if (valueType.isInstance(old))
            removalListener.remove(keyType.cast(e.key()), valueType.cast(old));
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
        return map.containsKey(key);
    }

    @Override
    public void clear()
    {
        strategy.clear();
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
