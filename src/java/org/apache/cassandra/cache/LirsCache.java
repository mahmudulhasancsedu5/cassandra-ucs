package org.apache.cassandra.cache;

import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.cassandra.cache.EvictionStrategy.Weigher;
import org.apache.cassandra.cache.SharedEvictionStrategyCache.RemovalListener;

import sun.misc.Contended;

public class LirsCache<Key, Value> implements ICache<Key, Value>, CacheSize
{
    @Contended
    volatile long remainingSize;
    @Contended
    volatile long remainingLirSize;

    volatile long capacity;

    final ConcurrentMap<Key, Entry<Key, Value>> map;
    final RemovalListener<Key, Value> removalListener;
    final Weigher weigher;

    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<LirsCache> remainingSizeUpdater =
            AtomicLongFieldUpdater.newUpdater(LirsCache.class, "remainingSize");
    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<LirsCache> remainingLirSizeUpdater =
            AtomicLongFieldUpdater.newUpdater(LirsCache.class, "remainingLirSize");
    @SuppressWarnings("rawtypes")
    static final AtomicLongFieldUpdater<LirsCache> capacityUpdater =
            AtomicLongFieldUpdater.newUpdater(LirsCache.class, "capacity");

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
        volatile State state = State.LOCKED;
        final Key key;
        Value value;
        volatile QueueEntry<Entry<Key, Value>> hirQueueEntry = null;
        volatile QueueEntry<Entry<Key, Value>> lirQueueEntry = null;

        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<Entry, State> stateUpdater =
                AtomicReferenceFieldUpdater.newUpdater(Entry.class, State.class, "state");
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<Entry, QueueEntry> hirQueueEntryUpdater =
                AtomicReferenceFieldUpdater.newUpdater(Entry.class, QueueEntry.class, "hirQueueEntry");

        public Entry(Key key, Value value)
        {
            this.key = key;
            this.value = value;
        }

        public boolean casState(State oldState, State newState)
        {
            return stateUpdater.compareAndSet(this, oldState, newState);
        }

        public String toString()
        {
            return state + ":" + key.toString();
        }
    }

    final QueueEntry<Entry<Key, Value>> lirHead = new QueueEntry<>(null);
    @Contended
    volatile QueueEntry<Entry<Key, Value>> lirTail = lirHead;
    final QueueEntry<Entry<Key, Value>> hirHead = new QueueEntry<>(null);
    @Contended
    volatile QueueEntry<Entry<Key, Value>> hirTail = hirHead;

    public static<Key, Value>
    LirsCache<Key, Value> create(RemovalListener<Key, Value> removalListener,
                                 Weigher weigher, long initialCapacity)
    {
        return new LirsCache<Key, Value>
        (new ConcurrentHashMap<>(),
         removalListener,
         weigher,
         initialCapacity);
    }

    private LirsCache(ConcurrentMap<Key, Entry<Key, Value>> map,
                     RemovalListener<Key, Value> removalListener,
                     Weigher weigher,
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
    putToMap:
        for ( ; ; )
        {
            e = map.putIfAbsent(key, ne);
            if (e == null)
            {
                putNewValue(ne, key, value);
                return;
            }

            for ( ; ; )
            {
                State s = e.state;
                switch (s)
                {
                case LIR_RESIDENT:
                    if (e.casState(s, State.LOCKED))
                    {
                        awaitLirAccess(e);
                        putValue(e, s, key, value);
                        return;
                    }
                    Thread.yield();
                    continue;
                case HIR:
                case HIR_RESIDENT_NON_LIR:
                case HIR_RESIDENT:
                    if (e.casState(s, State.LOCKED))
                    {
                        putValue(e, s, key, value);
                        return;
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
                putNewValue(ne, key, value);
                return true;
            }

            for ( ; ; )
            {
                Value v = e.value;
                if (v != null)
                    return false;
                switch (e.state)
                {
                case HIR_RESIDENT:
                case HIR_RESIDENT_NON_LIR:
                case LIR_RESIDENT:
                    // Not normally expected, but it may have become resident after we got value().
                    return false;
                case HIR:
                    // HIR status means removed. We must add, only fail if concurrently added.
                    if (e.casState(State.HIR, State.LOCKED))
                    {
                        putValue(e, State.HIR, key, value);
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

    private void putNewValue(Entry<Key, Value> entry, Key key, Value value)
    {
        addToHir(entry);
        addToLir(entry);
        assert entry.state == State.LOCKED;
        entry.state = State.HIR_RESIDENT;

        remainingSizeUpdater.addAndGet(this, -weigher.weigh(key, value));
        maybeEvict();
    }

    private void putValue(Entry<Key, Value> entry, State prevState, Key key, Value value)
    {
        Value oldValue = entry.value;
        long oldWeight = oldValue != null ? weigher.weigh(key, oldValue) : 0;
        long weight = weigher.weigh(key, value);
        long sizeAdj = oldWeight - weight;
        long lirSizeAdj;
        State nextState;
        entry.value = value;

        QueueEntry<Entry<Key, Value>> hqe = entry.hirQueueEntry;
        if (hqe != null && hqe.tryDelete())
            //TODO: check
            entry.hirQueueEntry = null;
        QueueEntry<Entry<Key, Value>> lqe = entry.lirQueueEntry;
        if (lqe == null || lqe.tryDelete())
            addToLir(entry);

        switch (prevState)
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
        assert entry.state == State.LOCKED;
        entry.state = nextState;
        remainingLirSizeUpdater.addAndGet(this, lirSizeAdj);
        remainingSizeUpdater.addAndGet(this, sizeAdj);
        maybeDemote();
        maybeEvict();
        if (oldValue != null)
            removalListener.remove(key, oldValue);
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

            switch (en.state)
            {
            case LIR_RESIDENT:
                if (!en.casState(State.LIR_RESIDENT, State.LOCKED))
                    break;

                // We may have acted in the middle of an access, with a LIR entry waiting to be put
                awaitLirAccess(en);

                // we have now claimed access
                en.lirQueueEntry = null;
                addToHir(en);

                assert en.state == State.LOCKED;
                en.state = State.HIR_RESIDENT_NON_LIR;

                remainingLirSizeUpdater.addAndGet(this, weigher.weigh(en.key, en.value));
                return;
            case HIR:
                if (!en.casState(State.HIR, State.LOCKED))
                    break;       // retry

                en.lirQueueEntry.delete();    // reload qe, it may have changed before we locked
                en.lirQueueEntry = null;
                assert en.hirQueueEntry == null;

                boolean success = map.remove(en.key, en);     // must succeed
                assert success;
                // fall through
                assert en.state == State.LOCKED;
                en.state = State.REMOVED;
                continue;
            case HIR_RESIDENT:
                if (!en.casState(State.HIR_RESIDENT, State.LOCKED))
                    break;

                en.lirQueueEntry.delete();    // reload qe, it may have changed before we locked
                en.lirQueueEntry = null;
                assert en.hirQueueEntry != null;

                assert en.state == State.LOCKED;
                en.state = State.HIR_RESIDENT_NON_LIR;
                continue;
            case LOCKED:
            case HIR_RESIDENT_NON_LIR:
            case REMOVED:
                break;
            }
            Thread.yield();
        }
    }

    protected QueueEntry<Entry<Key, Value>> awaitLirAccess(Entry<Key, Value> entry)
    {
        QueueEntry<Entry<Key, Value>> lqe = entry.lirQueueEntry;
        while (lqe == null || !lqe.tryDelete())
        {
            Thread.yield();
            lqe = entry.lirQueueEntry;
        }
        return lqe;
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
        State s;
        for ( ; ; )
        {
            s = e.state;
            if (s == State.HIR || s == State.REMOVED)
                return; // Someone else removed this as well. We are done.
            if (s != State.LOCKED && e.casState(s, State.LOCKED))
                break;
            // Wait for completion of pending operation.
            Thread.yield();
        }

        Value old = e.value;
        Key key = e.key;
        assert old != null;
        long weight = weigher.weigh(key, old);
        remainingSizeUpdater.addAndGet(this, weight);
        e.value = null;
        switch (s)
        {
        case LIR_RESIDENT:
            remainingLirSizeUpdater.addAndGet(this, weight);
            assert e.state == State.LOCKED;
            e.state = State.HIR;
            break;
        case HIR_RESIDENT:
            e.hirQueueEntry.delete();
            e.hirQueueEntry = null;
            assert e.lirQueueEntry != null;
            assert e.state == State.LOCKED;
            e.state = State.HIR;
            break;
        case HIR_RESIDENT_NON_LIR:
            map.remove(key, e);
            e.hirQueueEntry.delete();
            e.hirQueueEntry = null;
            assert e.lirQueueEntry == null;
            assert e.state == State.LOCKED;
            e.state = State.REMOVED;
            break;
        default:
            throw new AssertionError();
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
        while (!Entry.hirQueueEntryUpdater.compareAndSet(entry, null, qe))
            // Wait until access sets this to null.
            Thread.yield();
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
        for ( ; ; )
        {
            switch (e.state)
            {
            case LOCKED:
                // Concurrent access. Will now move/has just moved to top anyway.
                return;
            case LIR_RESIDENT:
                {
                    QueueEntry<Entry<Key, Value>> lqe = e.lirQueueEntry;
                    if (lqe != null && lqe.tryDelete())
                        addToLir(e);
                    // otherwise another thread accessed (or deleted etc.) this -- let them win
                    return;
                }
            case HIR_RESIDENT:
                if (e.casState(State.HIR_RESIDENT, State.LIR_RESIDENT))
                {
                    // Move to LIR.
                    QueueEntry<Entry<Key, Value>> lqe = e.lirQueueEntry;
                    if (lqe != null && lqe.tryDelete())
                        addToLir(e);
                    QueueEntry<Entry<Key, Value>> hqe = e.hirQueueEntry;
                    if (hqe != null && hqe.tryDelete())
                        // potential sync problem point: others shouldn't act until hirQueueEntry is null
                        e.hirQueueEntry = null;

                    remainingLirSizeUpdater.addAndGet(this, -weigher.weigh(e.key, e.value));

                    maybeDemote();

                    return;
                }
                else
                    // status changed. retry
                    break;
            case HIR_RESIDENT_NON_LIR:
                // need to give it a new LIR entry.
                if (e.casState(State.HIR_RESIDENT_NON_LIR, State.LOCKED))
                {
                    // Move to LIR.
                    assert e.value != null;
                    assert e.lirQueueEntry == null;
                    QueueEntry<Entry<Key, Value>> qe = e.hirQueueEntry;
                    qe.delete();
                    e.hirQueueEntry = null;

                    addToHir(e);

                    addToLir(e);
                    assert e.state == State.LOCKED;
                    e.state = State.HIR_RESIDENT;

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
