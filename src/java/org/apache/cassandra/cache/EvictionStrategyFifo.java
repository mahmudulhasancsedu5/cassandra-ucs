package org.apache.cassandra.cache;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.cache.CacheImpl.EvictionStrategy;

public class EvictionStrategyFifo<Key, Value> implements EvictionStrategy<Key, Value, EvictionStrategyFifo.Entry<Key, Value>>
{
    static class Entry<Key, Value> implements CacheImpl.Entry<Key, Value>
    {
        final Key key;
        final AtomicReference<Value> value;
        AtomicReference<QueueEntry<Entry<Key, Value>>> currentQueueEntry = new AtomicReference<>();

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

    final QueueEntry<Entry<Key, Value>> head = new QueueEntry<>(null);
    volatile QueueEntry<Entry<Key, Value>> tail = head;

    @Override
    public Entry<Key, Value> elementFor(Key key, Value value)
    {
        return new Entry<>(key, value);
    }

    @Override
    public void access(Entry<Key, Value> e)
    {
        // FIFO strategy: do nothing on access, let entries flow through queue.
    }

    @Override
    public void add(Entry<Key, Value> e)
    {
        // Note: e must be new and non-shared
        QueueEntry<Entry<Key, Value>> qe = new QueueEntry<>(e);
        assert e.currentQueueEntry.get() == null;
        e.currentQueueEntry.set(qe);

        tail = qe.addToQueue(tail);
    }

    @Override
    public boolean remove(Entry<Key, Value> e)
    {
        QueueEntry<Entry<Key, Value>> qe;
        do
        {
            qe = e.currentQueueEntry.get();
            if (qe == null)
                return false; // already removed by another thread
        }
        while (!e.currentQueueEntry.compareAndSet(qe, null));
        assert qe.content() == e;

        qe.delete();
        return true;
    }

    @Override
    public Entry<Key, Value> chooseForEviction()
    {
        for ( ; ; )
        {
            QueueEntry<Entry<Key, Value>> first = head.discardNextDeleted();
            if (first == null)
                return null;
            Entry<Key, Value> content = first.content();
            if (content != null)
                return content;
            // something removed entry, get another
        }
    }

    @Override
    public Iterator<Key> hotKeyIterator(int n)
    {
        // FIXME: maybe implement? won't be efficient as it needs walking in the opposite direction
        return Collections.emptyIterator();
    }

    @Override
    public Iterator<Entry<Key, Value>> iterator()
    {
        return head.iterator();
    }
}
