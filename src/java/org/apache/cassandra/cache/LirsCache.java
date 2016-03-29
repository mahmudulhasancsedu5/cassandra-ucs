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
        LOCKED,
        HIR,
        HIR_RESIDENT,
        HIR_RESIDENT_NON_LIR,
        LIR_RESIDENT,
        REMOVED;
    }

    static class Entry<Key, Value>
    {
        final Key key;
        Value value;
        QueueEntry<Entry<Key, Value>> hirQueueEntry = null;
        QueueEntry<Entry<Key, Value>> lirQueueEntry = null;
        final AtomicReference<State> state = new AtomicReference<>(State.LOCKED);

        public Entry(Key key, Value value)
        {
            this.key = key;
            this.value = value;
        }

        public String toString()
        {
            return state.get() + ":" + key.toString();
        }
    }

    final QueueEntry<Entry<Key, Value>> lirHead = new QueueEntry<>(null);
    volatile QueueEntry<Entry<Key, Value>> lirTail = lirHead;
    final QueueEntry<Entry<Key, Value>> hirHead = new QueueEntry<>(null);
    volatile QueueEntry<Entry<Key, Value>> hirTail = hirHead;

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
//                    QueueEntry<Entry<Key, Value>> qe = new QueueEntry<>(ne);
//                    assert ne.hirsQueueEntry == null;
//                    ne.hirsQueueEntry = qe;
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
                QueueEntry<Entry<Key, Value>> qe = new QueueEntry<>(ne);
                ne.hirQueueEntry = qe;
                addToHir(qe);
                qe = new QueueEntry<>(ne);
                ne.lirQueueEntry = qe;
                addToLir(qe);
                assert ne.state.get() == State.LOCKED;
                ne.state.set(State.HIR_RESIDENT);

                remainingSize.addAndGet(-weigher.weight(key, value));
                maybeEvict();
                return true;
            }

            for ( ; ; )
            {
                Value v = e.value;
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
                    if (e.state.compareAndSet(State.HIR, State.LOCKED))
                    {
                        // Successfully claimed slot. Now do the queues transition.
                        assert e.value == null;
                        e.value = value;
                        assert e.hirQueueEntry == null;
                        QueueEntry<Entry<Key, Value>> qe = e.lirQueueEntry;
                        qe.delete();
    
                        qe = new QueueEntry<>(e);
                        e.lirQueueEntry = qe;
                        addToLir(qe);
                        assert e.state.get() == State.LOCKED;
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
                case LOCKED:
                    Thread.yield();
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
            QueueEntry<Entry<Key, Value>> qe = lirHead.discardNextDeleted();
            if (qe == null)
                return;

            Entry<Key, Value> en = qe.content();
            if (en == null)
                continue;

            switch (en.state.get())
            {
            case LIR_RESIDENT:
                if (!en.state.compareAndSet(State.LIR_RESIDENT, State.LOCKED))
                    break;

                qe = en.lirQueueEntry;    // reload qe, it may have changed before we locked
                assert en.hirQueueEntry == null;

                en.lirQueueEntry = null;
                qe.delete();

                qe = new QueueEntry<>(en);
                en.hirQueueEntry = qe;
                addToHir(qe);

                assert en.state.get() == State.LOCKED;
                en.state.set(State.HIR_RESIDENT_NON_LIR);

                remainingLirSize.addAndGet(weigher.weight(en.key, en.value));
                return;
            case HIR:
                if (!en.state.compareAndSet(State.HIR, State.LOCKED))
                    break;       // retry

                qe = en.lirQueueEntry;    // reload qe, it may have changed before we locked
                assert en.hirQueueEntry == null;

                boolean success = map.remove(en.key, en);     // must succeed
                assert success;
                // fall through
                en.lirQueueEntry = null;
                qe.delete();
                assert en.state.get() == State.LOCKED;
                en.state.set(State.REMOVED);
                continue;
            case HIR_RESIDENT:
                if (!en.state.compareAndSet(State.HIR_RESIDENT, State.LOCKED))
                    break;
                qe = en.lirQueueEntry;    // reload qe, it may have changed before we locked
                assert en.hirQueueEntry != null;

                en.lirQueueEntry = null;
                qe.delete();
                assert en.state.get() == State.LOCKED;
                en.state.set(State.HIR_RESIDENT_NON_LIR);
                continue;
            case LOCKED:
            case HIR_RESIDENT_NON_LIR:
            case REMOVED:
                break;
            }
            Thread.yield();
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
        return e.value;
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
            if (s != State.LOCKED && e.state.compareAndSet(s, State.LOCKED))
                break;
            // Wait for completion of pending operation.
            Thread.yield();
        }

        Value old = e.value;
        Key key = e.key;
        assert old != null;
        long weight = weigher.weight(key, old);
        remainingSize.addAndGet(weight);
        e.value = null;
        switch (s)
        {
        case LIR_RESIDENT:
            remainingLirSize.addAndGet(weight);
            assert e.hirQueueEntry == null;
            assert e.state.get() == State.LOCKED;
            e.state.set(State.HIR);
            break;
        case HIR_RESIDENT:
            QueueEntry<Entry<Key, Value>> qe;
            qe = e.hirQueueEntry;
            qe.delete();
            e.hirQueueEntry = null;
            assert e.state.get() == State.LOCKED;
            e.state.set(State.HIR);
            break;
        case HIR_RESIDENT_NON_LIR:
            map.remove(key, e);
            qe = e.hirQueueEntry;
            qe.delete();
            e.hirQueueEntry = null;
            assert e.lirQueueEntry == null;
            assert e.state.get() == State.LOCKED;
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
            QueueEntry<Entry<Key, Value>> first = hirHead.discardNextDeleted();
            if (first == null)
                return;
            Entry<Key, Value> e = first.content();
            if (e != null)
                remove(e);
            else
                // Another thread is racing against us. Ease off.
                Thread.yield();
        }
    }

    @Override
    public void clear()
    {
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
            case LOCKED:
                // Concurrent access. Will now move/has just moved to top anyway.
                return;
            case LIR_RESIDENT:
                if (e.state.compareAndSet(State.LIR_RESIDENT, State.LOCKED))
                {
                    assert e.hirQueueEntry == null;
                    QueueEntry<Entry<Key, Value>> oqe = e.lirQueueEntry;
                    assert oqe != null;
                    oqe.delete();

                    QueueEntry<Entry<Key, Value>> nqe = new QueueEntry<>(e);
                    e.lirQueueEntry = nqe;
                    addToLir(nqe);

                    assert e.state.get() == State.LOCKED;
                    e.state.set(State.LIR_RESIDENT);
                    return;
                }
                else
                    break;
            case HIR_RESIDENT:
                if (e.state.compareAndSet(State.HIR_RESIDENT, State.LOCKED))
                {
                    // Move to LIR.
                    assert e.value != null;
                    QueueEntry<Entry<Key, Value>> qe = e.hirQueueEntry;
                    qe.delete();
                    e.hirQueueEntry = null;
                    qe = e.lirQueueEntry;
                    qe.delete();

                    qe = new QueueEntry<>(e);
                    e.lirQueueEntry = qe;
                    addToLir(qe);
                    assert e.state.get() == State.LOCKED;
                    e.state.set(State.LIR_RESIDENT);

                    remainingLirSize.addAndGet(-weigher.weight(e.key, e.value));

                    while (remainingLirSize.get() < 0)
                        demoteFromLirQueue();

                    return;
                }
                else
                    // status changed. retry
                    break;
            case HIR_RESIDENT_NON_LIR:
                // need to give it a new LIR entry.
                if (e.state.compareAndSet(State.HIR_RESIDENT_NON_LIR, State.LOCKED))
                {
                    // Move to LIR.
                    assert e.value != null;
                    assert e.lirQueueEntry == null;
                    QueueEntry<Entry<Key, Value>> qe = e.hirQueueEntry;
                    qe.delete();

                    qe = new QueueEntry<>(e);
                    e.hirQueueEntry = qe;
                    addToHir(qe);

                    qe = new QueueEntry<>(e);
                    e.lirQueueEntry = qe;
                    addToLir(qe);
                    assert e.state.get() == State.LOCKED;
                    e.state.set(State.HIR_RESIDENT);

                    return;
                }
                else
                    // status changed. retry
                    break;
            case HIR:
            case REMOVED:
                // We lost entry. Can't fix now.
                return;
            }
            Thread.yield();
        }
    }

    private void addToHir(QueueEntry<Entry<Key, Value>> qe)
    {
        hirTail = qe.addToQueue(hirTail);
    }

    private void addToLir(QueueEntry<Entry<Key, Value>> qe)
    {
        lirTail = qe.addToQueue(lirTail);
    }

    @Override
    public Iterator<Key> hotKeyIterator(int n)
    {
        // FIXME: maybe implement? won't be efficient as it needs walking in the opposite direction
        return Collections.emptyIterator();
    }
}
