package org.apache.cassandra.cache;

import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Iterators;

import org.apache.cassandra.cache.CacheImpl.EvictionStrategy;

public class LruEvictionStrategy<Key, Value> implements EvictionStrategy<Key, Value, LruEvictionStrategy.Entry<Key, Value>>
{
    static class Entry<Key, Value> implements CacheImpl.Entry<Key, Value>
    {
        final Key key;
        final AtomicReference<Value> value;

        public Entry(Key key, Value value)
        {
            this.key = key;
            this.value = new AtomicReference<>(value);
        }

        @Override
        public Key key()
        {
            return key;
        }

        @Override
        public Value value()
        {
            return value.get();
        }

        @Override
        public boolean casValue(Value old, Value v)
        {
            return value.compareAndSet(old, v);
        }
    }

    private final Queue<Entry<Key, Value>> lruList = new ConcurrentLinkedQueue<>();

    @Override
    public Entry<Key, Value> elementFor(Key key, Value value)
    {
        return new Entry<>(key, value);
    }

    @Override
    public void access(Entry<Key, Value> e)
    {
        lruList.remove(e);
        lruList.add(e);
    }

    @Override
    public void remove(Entry<Key, Value> e)
    {
        lruList.remove(e);
    }

    @Override
    public Entry<Key, Value> chooseForEviction()
    {
        return lruList.peek();
    }

    @Override
    public Iterator<Key> hotKeyIterator(int n)
    {
        return Iterators.limit(Iterators.transform(lruList.iterator(), Entry::key), n);
    }
}
