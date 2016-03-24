package org.apache.cassandra.cache;

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class CacheImpl<Key, Value, Element extends CacheImpl.Entry<Key, Value>> implements ICache<Key, Value>
{
    final ConcurrentMap<Key, Element> map;
    final EvictionStrategy<Key, Value, Element> strategy;
    final RemovalListener<Key, Value> removalListener;
    final Weigher<Key, Value> weigher;
    final AtomicLong capacity;
    final AtomicLong remainingSize;

    interface Entry<Key, Value>
    {
        Key key();
        Value value();
        boolean casValue(Value old, Value v);
    }

    interface EvictionStrategy<Key, Value, Element>
    {
        Element elementFor(Key key, Value value);
        void access(Element e);
        void remove(Element e);
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
        return new CacheImpl<Key, Value, LruEvictionStrategy.Entry<Key, Value>>
                            (new ConcurrentHashMap<>(),
                             new LruEvictionStrategy<>(),
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
        this.capacity = new AtomicLong(initialCapacity);
        this.remainingSize = new AtomicLong(initialCapacity);
    }

    @Override
    public long capacity()
    {
        return capacity.get();
    }

    @Override
    public void setCapacity(long newCapacity)
    {
        long currentCapacity = capacity.get();
        if (capacity.compareAndSet(currentCapacity, newCapacity))
        {
            remainingSize.addAndGet(newCapacity - currentCapacity);
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
        return capacity.get() - remainingSize.get();
    }

    @Override
    public void put(Key key, Value value)
    {
        Element e = map.get(key);
        if (e == null)
        {
            Element ne = strategy.elementFor(key, value);
            e = map.putIfAbsent(key, ne);
            if (e == null)
            {
                strategy.access(ne);
                remainingSize.addAndGet(-weigher.weight(key, value));
                maybeEvict();
                return;
            }
            strategy.remove(ne);
        }
        strategy.access(e);
        Value old;
        do
        {
            old = e.value();
            if (old == null)
            {
                // Someone removed entry while we were trying to change it. Ok to fail now, treated as already put before removal.
                removalListener.remove(key, value);
                return;
            }
        }
        while (!e.casValue(old, value));
        remainingSize.addAndGet(-weigher.weight(key, value) + weigher.weight(key, old));
        removalListener.remove(key, old);
        maybeEvict();
    }

    @Override
    public boolean putIfAbsent(Key key, Value value)
    {
        Element ne = strategy.elementFor(key, value);
        Element e = map.putIfAbsent(key, ne);
        if (e == null)
        {
            strategy.access(ne);
            remainingSize.addAndGet(-weigher.weight(key, value));
            maybeEvict();
            return true;
        }

        strategy.remove(ne);
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
        remainingSize.addAndGet(-weigher.weight(key, value) + weigher.weight(key, old));
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
        do
        {
            old = e.value();
            if (old == null)
                return; // Someone else removed this as well. We are done.
        }
        while (!e.casValue(old, null));

        Key key = e.key();
        remainingSize.addAndGet(weigher.weight(key, old));
        map.remove(key, e);
        strategy.remove(e);
        removalListener.remove(key, old);
    }

    @Override
    public void clear()
    {
        while (!map.isEmpty())
            for (Key k : map.keySet())
                remove(k);
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
        while (remainingSize.get() < 0)
            remove(strategy.chooseForEviction());
    }
}
