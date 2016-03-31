package org.apache.cassandra.cache;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import com.google.common.collect.Sets;

import sun.misc.Contended;

public class CacheImpl<Key, Value, Element extends CacheImpl.Entry<Key, Value>> implements ICache<Key, Value>
{
    @Contended
    volatile long remainingSize;
    volatile long capacity;

    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<CacheImpl> remainingSizeUpdater =
            AtomicLongFieldUpdater.newUpdater(CacheImpl.class, "remainingSize");
    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<CacheImpl> capacityUpdater =
            AtomicLongFieldUpdater.newUpdater(CacheImpl.class, "capacity");

    final ConcurrentMap<Key, Element> map;
    final EvictionStrategy<Key, Value, Element> strategy;
    final RemovalListener<Key, Value> removalListener;
    final Weigher<Key, Value> weigher;

    interface Entry<Key, Value>
    {
        Key key();
        Value value();
        boolean casValue(Value old, Value v);
    }

    interface EvictionStrategy<Key, Value, Element> extends Iterable<Element>
    {
        Element elementFor(Key key, Value value);
        void add(Element e);
        void access(Element e);
        boolean remove(Element e);
        Element chooseForEviction();
        Iterator<Key> hotKeyIterator(int n);
    }

    interface RemovalListener<Key, Value>
    {
        void remove(Key key, Value value);
    }

    interface Weigher<Key, Value>
    {
        long weight(Key key, Value value);
    }

    public static<Key, Value>
    CacheImpl<Key, Value, ?> create(RemovalListener<Key, Value> removalListener,
                                    Weigher<Key, Value> weigher,
                                    long initialCapacity)
    {
        return create(removalListener, weigher, initialCapacity,
                      new EvictionStrategyLru<>());
    }

    public static<Key, Value, Element extends Entry<Key, Value>>
    CacheImpl<Key, Value, Element> create(RemovalListener<Key, Value> removalListener,
                                          Weigher<Key, Value> weigher, long initialCapacity,
                                          EvictionStrategy<Key, Value, Element> evictionStrategy)
    {
        return new CacheImpl<Key, Value, Element>
        (new ConcurrentHashMap<>(),
         evictionStrategy,
         removalListener,
         weigher,
         initialCapacity);
    }

    private CacheImpl(ConcurrentMap<Key, Element> map,
                     EvictionStrategy<Key, Value, Element> strategy,
                     RemovalListener<Key, Value> removalListener,
                     Weigher<Key, Value> weigher,
                     long initialCapacity)
    {
        assert map.isEmpty();
        this.map = map;
        this.strategy = strategy;
        this.removalListener = removalListener;
        this.weigher = weigher;
        this.capacity = initialCapacity;
        this.remainingSize = initialCapacity;
    }

    @Override
    public long capacity()
    {
        return capacity;
    }

    @Override
    public void setCapacity(long newCapacity)
    {
        long currentCapacity = capacity;
        if (capacityUpdater.compareAndSet(this, currentCapacity, newCapacity))
        {
            remainingSizeUpdater.addAndGet(this, newCapacity - currentCapacity);
            maybeEvict();
        }
    }

    @Override
    public int size()
    {
        return map.size();
    }

    @Override
    public long weightedSize()
    {
        return capacity - remainingSize;
    }

    @Override
    public void put(Key key, Value value)
    {
    main:
        for ( ; ; )
        {
            Element e = map.get(key);
            if (e == null)
            {
                Element ne = strategy.elementFor(key, value);
                e = map.putIfAbsent(key, ne);
                if (e == null)
                {
                    strategy.add(ne);
                    remainingSizeUpdater.addAndGet(this, -weigher.weight(key, value));
                    break main;
                }
            }

            Value old;
            for ( ; ; )
            {
                old = e.value();
                if (old == value)
                    return;     // someone's done our job
                if (old == null)
                {
                    Thread.yield();
                    continue main;   // If the value is in the process of being removed, retry adding as the caller may have seen it as removed.
                }
                if (e.casValue(old, value))
                    break;
                Thread.yield();
            }
            strategy.access(e);
            remainingSizeUpdater.addAndGet(this, -weigher.weight(key, value) + weigher.weight(key, old));
            removalListener.remove(key, old);
            break main;
        }
        maybeEvict();
    }

    @Override
    public boolean putIfAbsent(Key key, Value value)
    {
        Element ne = strategy.elementFor(key, value);
        Element e;
        do
        {
            e = map.putIfAbsent(key, ne);
            if (e == null)
            {
                strategy.add(ne);
                remainingSizeUpdater.addAndGet(this, -weigher.weight(key, value));
                maybeEvict();
                return true;
            }
            Thread.yield();
        }
        while (e.value() == null); // If the value is in the process of being removed, retry adding as the caller may have seen it as removed.

        return false;
    }

    @Override
    public boolean replace(Key key, Value old, Value value)
    {
        assert old != null;
        assert value != null;

        Element e = map.get(key);
        if (e == null)
            return false;

        strategy.access(e);
        if (!e.casValue(old, value))
            return false;
        remainingSizeUpdater.addAndGet(this, -weigher.weight(key, value) + weigher.weight(key, old));
        removalListener.remove(key, old);
        maybeEvict();
        return true;
    }

    @Override
    public Value get(Key key)
    {
        Element e = map.get(key);
        if (e == null)
            return null;
        strategy.access(e);
        return e.value();
    }

    @Override
    public void remove(Key key)
    {
        Element e = map.get(key);
        if (e == null)
            return;
        remove(e);
    }

    public void remove(Element e)
    {
        Value old;
        for ( ; ; )
        {
            old = e.value();
            if (old == null)
                return; // Someone else removed this as well. We are done.
            if (e.casValue(old, null))
                break;
            Thread.yield();
        }

        Key key = e.key();
        remainingSizeUpdater.addAndGet(this, weigher.weight(key, old));
        map.remove(key, e);
        strategy.remove(e);
        removalListener.remove(key, old);
    }

    @Override
    public Iterator<Key> keyIterator()
    {
        return map.keySet().iterator();
    }

    @Override
    public Iterator<Key> hotKeyIterator(int n)
    {
        return strategy.hotKeyIterator(n);
    }

    @Override
    public boolean containsKey(Key key)
    {
        return map.containsKey(key);
    }

    private void maybeEvict()
    {
        while (remainingSize < 0)
        {
            Element e = strategy.chooseForEviction();
            if (e == null)
                return;
            remove(e);
        }
    }

    @Override
    public void clear()
    {
        while (!map.isEmpty())
            for (Key k : map.keySet())
                remove(k);
    }

    public void checkState()
    {
        // This must be called in isolation.
        Set<Key> ss = new HashSet<>();
        for (Element e : strategy)
        {
            assert e.value() != null;
            assert e == map.get(e.key());
            assert ss.add(e.key());
        }
        assert Sets.difference(map.keySet(), ss).isEmpty();
    }

    public String toString()
    {
        return getClass().getSimpleName() + "-" + strategy.getClass().getSimpleName();
    }
}
