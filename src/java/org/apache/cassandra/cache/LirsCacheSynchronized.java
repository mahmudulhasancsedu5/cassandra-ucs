package org.apache.cassandra.cache;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.apache.cassandra.cache.CacheImpl.RemovalListener;
import org.apache.cassandra.cache.CacheImpl.Weigher;
import sun.misc.Contended;

public class LirsCacheSynchronized<Key, Value> implements ICache<Key, Value>
{
    @Contended
    volatile long remainingSize;
    @Contended
    volatile long remainingLirSize;

    volatile long capacity;

    final ConcurrentMap<Key, Entry<Key, Value>> map;
    final RemovalListener<Key, Value> removalListener;
    final Weigher<Key, Value> weigher;

    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<LirsCacheSynchronized> remainingSizeUpdater =
            AtomicLongFieldUpdater.newUpdater(LirsCacheSynchronized.class, "remainingSize");
    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<LirsCacheSynchronized> remainingLirSizeUpdater =
            AtomicLongFieldUpdater.newUpdater(LirsCacheSynchronized.class, "remainingLirSize");
    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<LirsCacheSynchronized> capacityUpdater =
            AtomicLongFieldUpdater.newUpdater(LirsCacheSynchronized.class, "capacity");

    enum State
    {
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
        State state = State.REMOVED;

        public Entry(Key key, Value value)
        {
            this.key = key;
            this.value = value;
        }

        public String toString()
        {
            return state + ":" + key.toString();
        }
    }

    final QueueEntry<Entry<Key, Value>> lirHead = new QueueEntry<>(null);
    volatile QueueEntry<Entry<Key, Value>> lirTail = lirHead;
    final QueueEntry<Entry<Key, Value>> hirHead = new QueueEntry<>(null);
    volatile QueueEntry<Entry<Key, Value>> hirTail = hirHead;

    public static<Key, Value>
    LirsCacheSynchronized<Key, Value> create(RemovalListener<Key, Value> removalListener,
                                 Weigher<Key, Value> weigher, long initialCapacity)
    {
        return new LirsCacheSynchronized<Key, Value>
        (new ConcurrentHashMap<>(),
         removalListener,
         weigher,
         initialCapacity);
    }

    private LirsCacheSynchronized(ConcurrentMap<Key, Entry<Key, Value>> map,
                     RemovalListener<Key, Value> removalListener,
                     Weigher<Key, Value> weigher,
                     long initialCapacity)
    {
        assert map.isEmpty();
        this.map = map;
        this.removalListener = removalListener;
        this.weigher = weigher;
        this.capacity = initialCapacity;
        this.remainingSize = initialCapacity;
        this.remainingLirSize = lirCapacity(initialCapacity);
    }

    protected long lirCapacity(long capacity)
    {
        return capacity * 98 / 100;
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
            remainingLirSizeUpdater.addAndGet(this, lirCapacity(newCapacity) - lirCapacity(currentCapacity));
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
        Entry<Key, Value> ne = elementFor(key, value);
        Entry<Key, Value> e;

        for ( ; ; )
        {
            e = map.putIfAbsent(key, ne);
            if (e == null)
            {
                putNewValue(ne, key, value);
                return;
            }

            if (putValue(e, key, value, true))
                return;
        }
    }

    @Override
    public boolean putIfAbsent(Key key, Value value)
    {
        Entry<Key, Value> ne = elementFor(key, value);
        Entry<Key, Value> e;

        for ( ; ; )
        {
            e = map.putIfAbsent(key, ne);
            if (e == null)
            {
                putNewValue(ne, key, value);
                return true;
            }

            if (e.state != State.REMOVED)       // otherwise need to loop back as caller may have seen entry removed
                return putValue(e, key, value, false);
        }
    }

    private void putNewValue(Entry<Key, Value> entry, Key key, Value value)
    {
        synchronized (entry)
        {
            addToHir(entry);
            addToLir(entry);
            entry.state = State.HIR_RESIDENT;
        }

        remainingSizeUpdater.addAndGet(this, -weigher.weight(key, value));
        maybeEvict();
    }

    private boolean putValue(Entry<Key, Value> entry, Key key, Value value, boolean permitReplacement)
    {
        Value oldValue;
        long sizeAdj;
        long lirSizeAdj;
        synchronized (entry)
        {
            if (entry.state == State.REMOVED)
                return false;

            oldValue = entry.value;
            if (!permitReplacement && oldValue != null)
                return false;

            long oldWeight = oldValue != null ? weigher.weight(key, oldValue) : 0;
            long weight = weigher.weight(key, value);
            sizeAdj = oldWeight - weight;

            State nextState;
            entry.value = value;

            deleteIfSet(entry.hirQueueEntry);
            entry.hirQueueEntry = null;
            deleteIfSet(entry.lirQueueEntry);
            addToLir(entry);
    
            switch (entry.state)
            {
            case HIR:
                assert oldValue == null;
                // fall through
            case HIR_RESIDENT:
                nextState = State.LIR_RESIDENT;
                lirSizeAdj = -weight;
                break;
            case LIR_RESIDENT:
                nextState = State.LIR_RESIDENT;
                lirSizeAdj = sizeAdj;
                break;
            case HIR_RESIDENT_NON_LIR:
                nextState = State.HIR_RESIDENT;
                lirSizeAdj = 0;
                addToHir(entry);
                break;
            default:
                throw new AssertionError();
            }
            entry.state = nextState;
        }
        remainingLirSizeUpdater.addAndGet(this, lirSizeAdj);
        remainingSizeUpdater.addAndGet(this, sizeAdj);
        maybeDemote();
        maybeEvict();
        if (oldValue != null)
            removalListener.remove(key, oldValue);
        return true;
    }

    private void maybeDemote()
    {
        while (remainingLirSize < 0)
            demoteFromLirQueue();
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

            synchronized (en)
            {
                switch (en.state)
                {
                case LIR_RESIDENT:
                    en.lirQueueEntry.delete();
                    en.lirQueueEntry = null;
                    assert en.hirQueueEntry == null;
                    addToHir(en);
                    en.state = State.HIR_RESIDENT_NON_LIR;

                    remainingLirSizeUpdater.addAndGet(this, weigher.weight(en.key, en.value));
                    return;
                case HIR:
                    en.lirQueueEntry.delete();    // reload qe, it may have changed before we locked
                    en.lirQueueEntry = null;
                    assert en.hirQueueEntry == null;

                    boolean success = map.remove(en.key, en);     // must succeed
                    assert success;
                    // fall through
                    en.state = State.REMOVED;
                    continue;
                case HIR_RESIDENT:
                    en.lirQueueEntry.delete();    // reload qe, it may have changed before we locked
                    en.lirQueueEntry = null;
                    assert en.hirQueueEntry != null;

                    en.state = State.HIR_RESIDENT_NON_LIR;
                    continue;
                case HIR_RESIDENT_NON_LIR:
                case REMOVED:
                    break;
                }
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
//        remainingSizeUpdater.addAndGet(this, -weigher.weight(key, value) + weigher.weight(key, old));
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
        Value old;
        Key key = e.key;
        synchronized (e)
        {
            State s = e.state;
            if (s == State.HIR || s == State.REMOVED)
                return; // Someone else removed this as well. We are done.

            old = e.value;
            assert old != null;
            long weight = weigher.weight(key, old);
            remainingSizeUpdater.addAndGet(this, weight);
            e.value = null;
            switch (s)
            {
            case LIR_RESIDENT:
                remainingLirSizeUpdater.addAndGet(this, weight);
                assert e.hirQueueEntry == null;
                e.state = State.HIR;
                break;
            case HIR_RESIDENT:
                e.hirQueueEntry.delete();
                e.hirQueueEntry = null;
                assert e.lirQueueEntry != null;
                e.state = State.HIR;
                break;
            case HIR_RESIDENT_NON_LIR:
                map.remove(key, e);
                e.hirQueueEntry.delete();
                e.hirQueueEntry = null;
                assert e.lirQueueEntry == null;
                e.state = State.REMOVED;
                break;
            default:
                throw new AssertionError();
            }
        }
        removalListener.remove(key, old);
    }

    private void addToLir(Entry<Key, Value> entry)
    {
        QueueEntry<Entry<Key, Value>> qe = new QueueEntry<>(entry);
        entry.lirQueueEntry = qe;
        lirTail = qe.addToQueue(lirTail);
    }

    private void addToHir(Entry<Key, Value> entry)
    {
        QueueEntry<Entry<Key, Value>> qe = new QueueEntry<>(entry);
        entry.hirQueueEntry = qe;
        hirTail = qe.addToQueue(hirTail);
    }

    private void deleteIfSet(QueueEntry<Entry<Key, Value>> qe)
    {
        if (qe != null)
            qe.delete();
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
        while (remainingSize < 0)
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
        synchronized (e)
        {
            switch (e.state)
            {
            case LIR_RESIDENT:
                assert e.hirQueueEntry == null;
                e.lirQueueEntry.delete();
                addToLir(e);
                e.state = State.LIR_RESIDENT;
                return;
            case HIR_RESIDENT:
                // Move to LIR.
                assert e.value != null;
                e.hirQueueEntry.delete();
                e.hirQueueEntry = null;
                e.lirQueueEntry.delete();
                addToLir(e);
                e.state = State.LIR_RESIDENT;

                remainingLirSizeUpdater.addAndGet(this, -weigher.weight(e.key, e.value));

                maybeDemote();

                return;
            case HIR_RESIDENT_NON_LIR:
                // need to give it a new LIR entry.
                assert e.value != null;
                assert e.lirQueueEntry == null;
                e.hirQueueEntry.delete();
                addToHir(e);
                addToLir(e);
                e.state = State.HIR_RESIDENT;

                return;
            case HIR:
            case REMOVED:
                // We lost entry. Can't fix now.
                return;
            }
            Thread.yield();
        }
    }

    @Override
    public Iterator<Key> hotKeyIterator(int n)
    {
        // FIXME: maybe implement? won't be efficient as it needs walking in the opposite direction
        return Collections.emptyIterator();
    }

    public String toString()
    {
        return getClass().getSimpleName();
    }
}
