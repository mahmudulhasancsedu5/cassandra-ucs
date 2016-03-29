package org.apache.cassandra.cache;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.cache.CacheImpl.EvictionStrategy;

public class EvictionStrategyLru<Key, Value> implements EvictionStrategy<Key, Value, EvictionStrategyLru.Entry<Key, Value>>
{
    static class Entry<Key, Value> implements CacheImpl.Entry<Key, Value>
    {
        final Key key;
        final AtomicReference<Value> value;
        AtomicReference<QueueEntry<Key, Value>> currentQueueEntry = new AtomicReference<>();

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

    static class QueueEntry<Key, Value>
    {
        AtomicReference<QueueEntry<Key, Value>> next;
        volatile Entry<Key, Value> content;     // set at construction, changes to null to mark deleted

        public QueueEntry(Entry<Key, Value> content)
        {
            this.next = new AtomicReference<>(null);
            this.content = content;
        }
    }
    final QueueEntry<Key, Value> head = new QueueEntry<>(null);
    volatile QueueEntry<Key, Value> tail = head;

    @Override
    public Entry<Key, Value> elementFor(Key key, Value value)
    {
        return new Entry<>(key, value);
    }

    @Override
    public void access(Entry<Key, Value> e)
    {
        QueueEntry<Key, Value> oqe = e.currentQueueEntry.get();
        if (oqe == null)
            return;     // In the process of being removed, or not added yet. Can't access.
        QueueEntry<Key, Value> nqe = new QueueEntry<>(e);
        if (!e.currentQueueEntry.compareAndSet(oqe, nqe))
            return;     // something else moved e at the same time, that's sufficient

        assert oqe.content == e;
        remove(oqe);
        add(nqe);
    }

    @Override
    public void add(Entry<Key, Value> e)
    {
        // Note: e must be new and non-shared
        QueueEntry<Key, Value> qe = new QueueEntry<>(e);
        assert e.currentQueueEntry.get() == null;
        e.currentQueueEntry.set(qe);

        add(qe);
    }

    private void add(QueueEntry<Key, Value> qe)
    {
        QueueEntry<Key, Value> t;
        t = tail;
        do
        {
            QueueEntry<Key, Value> next = t.next.get();
            while (next != null)
            {
                t = next;
                next = next.next.get();
            }
        }
        while (!t.next.compareAndSet(null, qe));
        tail = t;
    }

    @Override
    public boolean remove(Entry<Key, Value> e)
    {
        QueueEntry<Key, Value> qe;
        do
        {
            qe = e.currentQueueEntry.get();
            if (qe == null)
                return false; // already removed by another thread
        }
        while (!e.currentQueueEntry.compareAndSet(qe, null));
        assert qe.content == e;

        remove(qe);
        return true;
    }

    private void remove(QueueEntry<Key, Value> qe)
    {
        // mark deleted
        qe.content = null;

        discardNextDeleted(qe);
    }

    public QueueEntry<Key, Value> discardNextDeleted(QueueEntry<Key, Value> qe)
    {
        // Remove nexts while they don't have content, but make sure to point to a trailing entry to make sure we don't
        // skip over something that is just being added.
        QueueEntry<Key, Value> next = qe.next.get();
        if (next == null)
            return qe;
        if (next.content != null)
            return next;

        QueueEntry<Key, Value> nextnext = next.next.get();
        if (nextnext == null)
            return next;        // still no change wanted

        do
        {
            next = nextnext;
            nextnext = next.next.get();
            if (nextnext == null)
                break;
        }
        while (next.content == null);

        assert next != null;
        qe.next.lazySet(next);
        return next;
    }

    @Override
    public Entry<Key, Value> chooseForEviction()
    {
        for ( ; ; )
        {
            QueueEntry<Key, Value> first = discardNextDeleted(head);
            if (first == null)
                return null;
            Entry<Key, Value> content = first.content;
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
        return new Iter();
    }

    class Iter implements Iterator<Entry<Key, Value>>
    {
        QueueEntry<Key, Value> qe = head.next.get();

        @Override
        public boolean hasNext()
        {
            while (qe != null && qe.content == null)
                qe = qe.next.get();
            return qe != null;
        }

        @Override
        public Entry<Key, Value> next()
        {
            if (!hasNext())
                throw new AssertionError();
            Entry<Key, Value> content = qe.content;
            qe = qe.next.get();
            return content;
        }

    }
}
