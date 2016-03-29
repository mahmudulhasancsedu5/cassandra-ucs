package org.apache.cassandra.cache;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.cache.CacheImpl.RemovalListener;
import org.apache.cassandra.cache.CacheImpl.Weigher;

public class LirsCache<Key, Value> implements ICache<Key, Value>
{
    final AtomicLong remainingSize;
    final AtomicLong remainingLirSize;
    final AtomicLong capacity;
    final ConcurrentMap<Key, Entry<Key, Value>> map;
    final RemovalListener<Key, Value> removalListener;
    final Weigher<Key, Value> weigher;

    enum State
    {
        TRANSITIONING,
        HIR,
        HIR_RESIDENT,
        HIR_RESIDENT_NON_LIR,
        LIR_RESIDENT,
        REMOVED;
    }

    static class Entry<Key, Value> implements CacheImpl.Entry<Key, Value>
    {
        final Key key;
        final AtomicReference<Value> value;
        final AtomicReference<QueueEntry<Key, Value>> hirQueueEntry = new AtomicReference<>();
        final AtomicReference<QueueEntry<Key, Value>> lirQueueEntry = new AtomicReference<>();
        final AtomicReference<State> state = new AtomicReference<>(State.TRANSITIONING);

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

    final QueueEntry<Key, Value> lirHead = new QueueEntry<>(null);
    volatile QueueEntry<Key, Value> lirTail = lirHead;
    final QueueEntry<Key, Value> hirHead = new QueueEntry<>(null);
    volatile QueueEntry<Key, Value> hirTail = hirHead;

    public static<Key, Value>
    LirsCache<Key, Value> create(RemovalListener<Key, Value> removalListener,
                                 Weigher<Key, Value> weigher, long initialCapacity)
    {
        return new LirsCache<Key, Value>
        (new ConcurrentHashMap<>(),
         removalListener,
         weigher,
         initialCapacity);
    }

    private LirsCache(ConcurrentMap<Key, Entry<Key, Value>> map,
                     RemovalListener<Key, Value> removalListener,
                     Weigher<Key, Value> weigher,
                     long initialCapacity)
    {
        assert map.isEmpty();
        this.map = map;
        this.removalListener = removalListener;
        this.weigher = weigher;
        this.capacity = new AtomicLong(initialCapacity);
        this.remainingSize = new AtomicLong(initialCapacity);
        this.remainingLirSize = new AtomicLong(initialCapacity * 9 / 10);
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
        throw new UnsupportedOperationException();
//    main:
//        for ( ; ; )
//        {
//            Entry<Key, Value> e = map.get(key);
//            if (e == null)
//            {
//                Entry<Key, Value> ne = elementFor(key, value);
//                e = map.putIfAbsent(key, ne);
//                if (e == null)
//                {
//                    // Note: e must be new and non-shared
//                    QueueEntry<Key, Value> qe = new QueueEntry<>(ne);
//                    assert ne.hirsQueueEntry.get() == null;
//                    ne.hirsQueueEntry.set(qe);
//
//                    addToHir(qe);
//                    remainingSize.addAndGet(-weigher.weight(key, value));
//                    break main;
//                }
//            }
//
//            Value old;
//            do
//            {
//                old = e.value();
//                if (old == value)
//                    return;     // someone's done our job
//                if (old == null)
//                    continue main;   // If the value is in the process of being removed, retry adding as the caller may have seen it as removed. 
//            }
//            while (!e.casValue(old, value));
//            access(e);
//            remainingSize.addAndGet(-weigher.weight(key, value) + weigher.weight(key, old));
//            removalListener.remove(key, old);
//            break main;
//        }
//        maybeEvict();
    }

    @Override
    public boolean putIfAbsent(Key key, Value value)
    {
        Entry<Key, Value> ne = elementFor(key, value);
        Entry<Key, Value> e;
    putToMap:
        for ( ; ; )
        {
            e = map.putIfAbsent(key, ne);
            if (e == null)
            {
                QueueEntry<Key, Value> qe = new QueueEntry<>(ne);
                ne.hirQueueEntry.set(qe);
                addToHir(qe);
                qe = new QueueEntry<>(ne);
                ne.lirQueueEntry.set(qe);
                addToLir(qe);
                assert ne.state.get() == State.TRANSITIONING;
                ne.state.set(State.HIR_RESIDENT);

                remainingSize.addAndGet(-weigher.weight(key, value));
                maybeEvict();
                return true;
            }

            for ( ; ; )
            {
                Value v = e.value();
                if (v != null)
                    return false;
                switch (e.state.get())
                {
                case HIR_RESIDENT:
                case HIR_RESIDENT_NON_LIR:
                case LIR_RESIDENT:
                    // Not normally expected, but it may have become resident after we got value().
                    return false;
                case HIR:
                    // HIR status means removed. We must add, only fail if concurrently added.
                    if (e.state.compareAndSet(State.HIR, State.TRANSITIONING))
                    {
                        // Successfully claimed slot. Now do the queues transition.
                        assert e.value.get() == null;
                        e.value.set(value);
                        assert e.hirQueueEntry.get() == null;
                        QueueEntry<Key, Value> qe = e.lirQueueEntry.get();
                        qe.content = null;
    
                        qe = new QueueEntry<>(e);
                        e.lirQueueEntry.set(qe);
                        addToLir(qe);
                        assert e.state.get() == State.TRANSITIONING;
                        e.state.set(State.LIR_RESIDENT);
                        long weight = -weigher.weight(key, value);
                        remainingLirSize.addAndGet(weight);
    
                        while (remainingLirSize.get() < 0)
                            demoteFromLirQueue();
    
                        remainingSize.addAndGet(weight);
                        maybeEvict();
                        return true;
                    }
                    // Otherwise wait for new state.
                case TRANSITIONING:
                    continue;
                case REMOVED:
                    // If the value was removed, retry adding as the caller may have seen it as removed.
                    continue putToMap;
                }
            }
        }
    }

    private void demoteFromLirQueue()
    {
        for ( ; ; )
        {
            QueueEntry<Key, Value> qe = discardNextDeleted(lirHead);
            if (qe == null)
                return;

            Entry<Key, Value> en = qe.content;
            if (en == null)
                continue;

            switch (en.state.get())
            {
            case HIR:
                if (!en.state.compareAndSet(State.HIR, State.TRANSITIONING))
                    continue;       // retry

                qe = en.lirQueueEntry.get();    // reload qe, it may have changed before we locked
                assert en.hirQueueEntry.get() == null;

                boolean success = map.remove(en.key, en);     // must succeed
                assert success;
                // fall through
                en.lirQueueEntry.set(null);
                qe.content = null;
                assert en.state.get() == State.TRANSITIONING;
                en.state.set(State.REMOVED);
                continue;
            case HIR_RESIDENT:
                if (!en.state.compareAndSet(State.HIR_RESIDENT, State.TRANSITIONING))
                    continue;
                qe = en.lirQueueEntry.get();    // reload qe, it may have changed before we locked
                assert en.hirQueueEntry.get() != null;

                en.lirQueueEntry.set(null);
                qe.content = null;
                assert en.state.get() == State.TRANSITIONING;
                en.state.set(State.HIR_RESIDENT_NON_LIR);
                continue;
            case TRANSITIONING:
            case HIR_RESIDENT_NON_LIR:
            case REMOVED:
                continue;
            case LIR_RESIDENT:
                if (!en.state.compareAndSet(State.LIR_RESIDENT, State.TRANSITIONING))
                    continue;

                qe = en.lirQueueEntry.get();    // reload qe, it may have changed before we locked
                assert en.hirQueueEntry.get() == null;

                en.lirQueueEntry.set(null);
                qe.content = null;

                qe = new QueueEntry<>(en);
                en.hirQueueEntry.set(qe);
                addToHir(qe);

                assert en.state.get() == State.TRANSITIONING;
                en.state.set(State.HIR_RESIDENT_NON_LIR);

                remainingLirSize.addAndGet(weigher.weight(en.key, en.value()));
                return;
            }
        }
    }

    @Override
    public boolean replace(Key key, Value old, Value value)
    {
        throw new UnsupportedOperationException();
//        assert old != null;
//        assert value != null;
//
//        Entry<Key, Value> e = map.get(key);
//        if (e == null)
//            return false;
//
//        access(e);
//        if (!e.casValue(old, value))
//            return false;
//        remainingSize.addAndGet(-weigher.weight(key, value) + weigher.weight(key, old));
//        removalListener.remove(key, old);
//        maybeEvict();
//        return true;
    }

    @Override
    public Value get(Key key)
    {
        Entry<Key, Value> e = map.get(key);
        if (e == null)
            return null;
        access(e);
        return e.value();
    }

    @Override
    public void remove(Key key)
    {
        Entry<Key, Value> e = map.get(key);
        if (e == null)
            return;
        remove(e);
    }

    public void remove(Entry<Key, Value> e)
    {
        State s;
        for ( ; ; )
        {
            s = e.state.get();
            if (s == State.HIR || s == State.REMOVED)
                return; // Someone else removed this as well. We are done.
            if (s == State.TRANSITIONING)
                continue; // Wait for completion of pending operation.
            if (e.state.compareAndSet(s, State.TRANSITIONING))
                break;
        }

        Value old = e.value.get();
        Key key = e.key;
        assert old != null;
        long weight = weigher.weight(key, old);
        remainingSize.addAndGet(weight);
        e.value.set(null);
        switch (s)
        {
        case LIR_RESIDENT:
            remainingLirSize.addAndGet(weight);
            assert e.hirQueueEntry.get() == null;
            assert e.state.get() == State.TRANSITIONING;
            e.state.set(State.HIR);
            break;
        case HIR_RESIDENT:
            QueueEntry<Key, Value> qe;
            qe = e.hirQueueEntry.get();
            qe.content = null;
            e.hirQueueEntry.set(null);
            assert e.state.get() == State.TRANSITIONING;
            e.state.set(State.HIR);
            break;
        case HIR_RESIDENT_NON_LIR:
            map.remove(key, e);
            qe = e.hirQueueEntry.get();
            qe.content = null;
            e.hirQueueEntry.set(null);
            assert e.lirQueueEntry.get() == null;
            assert e.state.get() == State.TRANSITIONING;
            e.state.set(State.REMOVED);
            break;
        default:
            throw new AssertionError();
        }
        removalListener.remove(key, old);
    }

    @Override
    public Iterator<Key> keyIterator()
    {
        return map.keySet().iterator();
    }

    @Override
    public boolean containsKey(Key key)
    {
        return map.containsKey(key);
    }

    private void maybeEvict()
    {
        while (remainingSize.get() < 0)
        {
            Entry<Key, Value> e = chooseForEviction();
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
//        Set<Key> ss = new HashSet<>();
//        for (Entry<Key, Value> e : strategy)
//        {
//            assert e.value() != null;
//            assert e == map.get(e.key());
//            assert ss.add(e.key());
//        }
//        assert Sets.difference(map.keySet(), ss).isEmpty();
    }
    

    public Entry<Key, Value> elementFor(Key key, Value value)
    {
        return new Entry<>(key, value);
    }

    public void access(Entry<Key, Value> e)
    {
        for ( ; ; )
        {
            switch (e.state.get())
            {
            case TRANSITIONING:
                // Concurrent access. Will now move/has just moved to top anyway.
                return;
            case LIR_RESIDENT:
                if (e.state.compareAndSet(State.LIR_RESIDENT, State.TRANSITIONING))
                {
                    assert e.hirQueueEntry.get() == null;
                    QueueEntry<Key, Value> oqe = e.lirQueueEntry.get();
                    assert oqe != null;
                    assert oqe.content == e;
                    oqe.content = null;

                    QueueEntry<Key, Value> nqe = new QueueEntry<>(e);
                    e.lirQueueEntry.set(nqe);
                    addToLir(nqe);

                    assert e.state.get() == State.TRANSITIONING;
                    e.state.set(State.LIR_RESIDENT);
                    return;
                }
                else
                    continue;
            case HIR_RESIDENT:
                if (e.state.compareAndSet(State.HIR_RESIDENT, State.TRANSITIONING))
                {
                    // Move to LIR.
                    assert e.value.get() != null;
                    QueueEntry<Key, Value> qe = e.hirQueueEntry.get();
                    qe.content = null;
                    e.hirQueueEntry.set(null);
                    qe = e.lirQueueEntry.get();
                    qe.content = null;

                    qe = new QueueEntry<>(e);
                    e.lirQueueEntry.set(qe);
                    addToLir(qe);
                    assert e.state.get() == State.TRANSITIONING;
                    e.state.set(State.LIR_RESIDENT);

                    remainingLirSize.addAndGet(-weigher.weight(e.key, e.value()));

                    while (remainingLirSize.get() < 0)
                        demoteFromLirQueue();

                    return;
                }
                else
                    // status changed. retry
                    continue;
            case HIR_RESIDENT_NON_LIR:
                // need to give it a new LIR entry.
                if (e.state.compareAndSet(State.HIR_RESIDENT_NON_LIR, State.TRANSITIONING))
                {
                    // Move to LIR.
                    assert e.value.get() != null;
                    assert e.lirQueueEntry.get() == null;
                    QueueEntry<Key, Value> qe = e.hirQueueEntry.get();
                    qe.content = null;

                    qe = new QueueEntry<>(e);
                    e.hirQueueEntry.set(qe);
                    addToHir(qe);

                    qe = new QueueEntry<>(e);
                    e.lirQueueEntry.set(qe);
                    addToLir(qe);
                    assert e.state.get() == State.TRANSITIONING;
                    e.state.set(State.HIR_RESIDENT);

                    return;
                }
                else
                    // status changed. retry
                    continue;
            case HIR:
            case REMOVED:
                // We lost entry. Can't fix now.
                return;
            }
        }
    }

    private void addToHir(QueueEntry<Key, Value> qe)
    {
        hirTail = addToQueue(qe, hirTail);
    }

    private void addToLir(QueueEntry<Key, Value> qe)
    {
        lirTail = addToQueue(qe, lirTail);
    }

    private QueueEntry<Key, Value> addToQueue(QueueEntry<Key, Value> entry, QueueEntry<Key, Value> queue)
    {
        do
        {
            QueueEntry<Key, Value> next = queue.next.get();
            while (next != null)
            {
                queue = next;
                next = next.next.get();
            }
        }
        while (!queue.next.compareAndSet(null, entry));
        return queue;
    }

    private void release(QueueEntry<Key, Value> qe)
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

    public Entry<Key, Value> chooseForEviction()
    {
//        // evict all (or as many as necessary) hir entries at the head of the lir queue.
//        // they would be often empty
//        for ( ; ; )
//        {
//            QueueEntry<Key, Value> first = discardNextDeleted(lirHead);
//            if (first == null)
//                break;
//            Entry<Key, Value> content = first.content;
//            if (content != null)
//            {
//                if (content.lir)
//                    break;
//                return content;
//            }
//            // something removed entry, get another
//        }

        // remove first resident hir
        for ( ; ; )
        {
            QueueEntry<Key, Value> first = discardNextDeleted(hirHead);
            if (first == null)
                return null;
            Entry<Key, Value> content = first.content;
            if (content != null)
            {
//                assert !content.lir;
                return content;
            }
            // something removed entry, get another
        }
    }

    @Override
    public Iterator<Key> hotKeyIterator(int n)
    {
        // FIXME: maybe implement? won't be efficient as it needs walking in the opposite direction
        return Collections.emptyIterator();
    }

    class Iter implements Iterator<Entry<Key, Value>>
    {
        QueueEntry<Key, Value> qe = hirHead.next.get();

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
