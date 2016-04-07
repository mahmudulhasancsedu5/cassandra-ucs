package org.apache.cassandra.cache;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.apache.cassandra.cache.EvictionStrategy.Specials;

import sun.misc.Contended;

public class EvictionStrategyLirs implements EvictionStrategy
{
    @Contended
    volatile long remainingSize;
    @Contended
    volatile long remainingLirSize;

    volatile long capacity;

    final Weigher weigher;

    static final AtomicLongFieldUpdater<EvictionStrategyLirs> remainingSizeUpdater =
            AtomicLongFieldUpdater.newUpdater(EvictionStrategyLirs.class, "remainingSize");
    static final AtomicLongFieldUpdater<EvictionStrategyLirs> remainingLirSizeUpdater =
            AtomicLongFieldUpdater.newUpdater(EvictionStrategyLirs.class, "remainingLirSize");
    static final AtomicLongFieldUpdater<EvictionStrategyLirs> capacityUpdater =
            AtomicLongFieldUpdater.newUpdater(EvictionStrategyLirs.class, "capacity");

    enum State
    {
        LOCKED,          // in transition
        NO_QUEUES,       // new
        HIRQ_ONLY,       // removed on eviction
        LIRQ_ONLY,       // not resident
        BOTH_QUEUES,     // resident, HIR status
        LIR_STATUS,      // lir status
        REMOVED;
    }

    static final AtomicReferenceFieldUpdater<Element, State> stateUpdater =
            AtomicReferenceFieldUpdater.newUpdater(Element.class, State.class, "state");
    static final AtomicReferenceFieldUpdater<Element, Object> valueUpdater =
            AtomicReferenceFieldUpdater.newUpdater(Element.class, Object.class, "value");

    class Element implements Entry
    {
        volatile State state = State.NO_QUEUES;
        final Object key;
        volatile Object value = Specials.EMPTY;
        final EntryOwner owner;
        volatile QueueEntry<Element> hirQueueEntry = null;
        volatile QueueEntry<Element> lirQueueEntry = null;


        public Element(Object key, EntryOwner owner)
        {
            this.key = key;
            this.owner = owner;
        }

        @Override
        public Object key()
        {
            return key;
        }

        @Override
        public Object value()
        {
            return value;
        }

        @Override
        public boolean casValue(Object old, Object v)
        {
            assert old != Specials.DISCARDED;
            assert v != Specials.DISCARDED;
            assert v != Specials.EMPTY;

            for ( ; ; )
            {
                State s = state;
                if (s == State.LOCKED)
                    continue;
                if (!stateUpdater.compareAndSet(this, s, State.LOCKED))
                    continue;

                if (!valueUpdater.compareAndSet(this, old, v))
                {
                    state = s;
                    return false;
                }

                long remSizeChange = -weigher.weigh(key, v);
                if (old != Specials.EMPTY)
                    remSizeChange += weigher.weigh(key, old);
                remainingSizeUpdater.addAndGet(EvictionStrategyLirs.this, remSizeChange);
                if (s == State.LIR_STATUS)
                    remainingLirSizeUpdater.addAndGet(EvictionStrategyLirs.this, remSizeChange);
                state = s;
                access();
                return true;
            }
        }

        @Override
        public Object remove()
        {
            for ( ; ; )
            {
                State s = state;
                if (s == State.LOCKED)
                    continue;
                if (!stateUpdater.compareAndSet(this, s, State.LOCKED))
                    continue;

                Object old = value;
                if (!valueUpdater.compareAndSet(this, old, Specials.EMPTY))
                {
                    state = s;
                    return Specials.EMPTY;
                }

                if (!(old instanceof Specials))
                {
                    long remSizeChange = weigher.weigh(key, old);
                    remainingSizeUpdater.addAndGet(EvictionStrategyLirs.this, remSizeChange);
                    if (s == State.LIR_STATUS)
                        remainingLirSizeUpdater.addAndGet(EvictionStrategyLirs.this, remSizeChange);
                }
                state = s;
                return old;
            }
        }

        @Override
        public void access()
        {
            for ( ; ; )
            {
                switch (state)
                {
                case LOCKED:
                    // Concurrent access. Will now move/has just moved to top anyway.
                    return;
                case LIR_STATUS:
                {
                    QueueEntry<Element> lqe = lirQueueEntry;
                    if (lqe != null && lqe.tryDelete())
                        addToLir(this);
                    // otherwise another thread accessed (or deleted etc.) this -- let them win
                    return;
                }
                case LIRQ_ONLY:
                    if (stateUpdater.compareAndSet(this, State.LIRQ_ONLY, State.LOCKED))
                    {
                        // Move to LIR.
                        QueueEntry<Element> lqe = lirQueueEntry;
                        if (lqe != null && lqe.tryDelete())
                            addToLir(this);
                        assert hirQueueEntry == null;

                        if (!(value instanceof Specials))
                        {
                            remainingLirSizeUpdater.addAndGet(EvictionStrategyLirs.this, -weigher.weigh(key, value));
                            maybeDemote();
                        }

                        assert state == State.LOCKED;
                        state = State.LIR_STATUS;

                        return;
                    }
                    else
                        // status changed. retry
                        break;
                case BOTH_QUEUES:
                    if (stateUpdater.compareAndSet(this, State.BOTH_QUEUES, State.LOCKED))
                    {
                        // Move to LIR.
                        QueueEntry<Element> lqe = lirQueueEntry;
                        if (lqe != null && lqe.tryDelete())
                            addToLir(this);
                        hirQueueEntry.delete();
                        hirQueueEntry = null;

                        if (!(value instanceof Specials))
                        {
                            remainingLirSizeUpdater.addAndGet(EvictionStrategyLirs.this, -weigher.weigh(key, value));
                            maybeDemote();
                        }

                        assert state == State.LOCKED;
                        state = State.LIR_STATUS;

                        return;
                    }
                    else
                        // status changed. retry
                        break;
                case HIRQ_ONLY:
                    // need to give it a new LIR entry.
                    if (stateUpdater.compareAndSet(this, State.HIRQ_ONLY, State.LOCKED))
                    {
                        // we expect value to be non-null here
                        // Move to LIR.
                        assert lirQueueEntry == null;
                        QueueEntry<Element> qe = hirQueueEntry;
                        qe.delete();
                        hirQueueEntry = null;

                        addToHir(this);
                        addToLir(this);
                        assert state == State.LOCKED;
                        state = State.BOTH_QUEUES;

                        return;
                    }
                    else
                        // status changed. retry
                        break;
                case NO_QUEUES:
                    // need to give it a new LIR entry.
                    if (stateUpdater.compareAndSet(this, State.NO_QUEUES, State.LOCKED))
                    {
                        // we expect value to be non-null here
                        // Move to LIR.
                        assert lirQueueEntry == null;
                        assert hirQueueEntry == null;

                        addToHir(this);
                        addToLir(this);
                        assert state == State.LOCKED;
                        state = State.BOTH_QUEUES;

                        return;
                    }
                    else
                        // status changed. retry
                        break;

                case REMOVED:
                    // We lost entry. Can't fix now.
                    return;
                }
                Thread.yield();
            }
        }

        public String toString()
        {
            return state + ":" + key.toString();
        }

        public boolean evict()
        {
            switch (state)
            {
            case BOTH_QUEUES:
                if (stateUpdater.compareAndSet(this, State.BOTH_QUEUES, State.LOCKED))
                {
                    hirQueueEntry.delete();
                    hirQueueEntry = null;
                    assert state == State.LOCKED;
                    state = State.LIRQ_ONLY;
                    owner.evict(this);
                    return true;
                }
                else
                    break;
            case HIRQ_ONLY:
                if (stateUpdater.compareAndSet(this, State.HIRQ_ONLY, State.LOCKED))
                {
                    hirQueueEntry.delete();
                    hirQueueEntry = null;
                    assert state == State.LOCKED;
                    state = State.NO_QUEUES;
                    owner.evict(this);
                    if (valueUpdater.compareAndSet(this, Specials.EMPTY, Specials.DISCARDED))
                            owner.removeMapping(this);
                    return true;
                }
                else
                    break;
            default:
                // Something changed by another thread. Try again.
                break;
            }
            return false;
        }

        public boolean demote()
        {
            switch (state)
            {
            case LIR_STATUS:
                if (!stateUpdater.compareAndSet(this, State.LIR_STATUS, State.LOCKED))
                    return false;       // yield and retry

                // We may have acted in the middle of an access, with a LIR entry waiting to be put
                awaitLirAccess(this);

                // we have now claimed access
                lirQueueEntry = null;
                addToHir(this);

                assert state == State.LOCKED;
                state = State.HIRQ_ONLY;

                if (!(value instanceof Specials))
                    remainingLirSizeUpdater.addAndGet(EvictionStrategyLirs.this, -weigher.weigh(key, value));
                return true;
            case LIRQ_ONLY:
                if (!stateUpdater.compareAndSet(this, State.LIRQ_ONLY, State.LOCKED))
                    return false;       // yield and retry

                lirQueueEntry.delete();    // reload qe, it may have changed before we locked
                lirQueueEntry = null;
                assert hirQueueEntry == null;

                assert state == State.LOCKED;
                state = State.NO_QUEUES;

                if (valueUpdater.compareAndSet(this, Specials.EMPTY, Specials.DISCARDED))
                        owner.removeMapping(this);
                return true;
            case BOTH_QUEUES:
                if (!stateUpdater.compareAndSet(this, State.BOTH_QUEUES, State.LOCKED))
                    return false;       // yield and retry

                lirQueueEntry.delete();    // reload qe, it may have changed before we locked
                lirQueueEntry = null;
                assert hirQueueEntry != null;

                assert state == State.LOCKED;
                state = State.HIRQ_ONLY;
                return true;
            default:
                return false;       // concurrent change, yield and retry
            }
        }
    }

    final QueueEntry<Element> lirHead = new QueueEntry<>(null);
    @Contended
    volatile QueueEntry<Element> lirTail = lirHead;
    final QueueEntry<Element> hirHead = new QueueEntry<>(null);
    @Contended
    volatile QueueEntry<Element> hirTail = hirHead;

    public EvictionStrategyLirs(Weigher weigher, long initialCapacity)
    {
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
        return (int) weightedSize();
    }

    @Override
    public long weightedSize()
    {
        return capacity - remainingSize;
    }

    protected QueueEntry<Element> awaitLirAccess(Element entry)
    {
        QueueEntry<Element> lqe = entry.lirQueueEntry;
        while (lqe == null || !lqe.tryDelete())
        {
            Thread.yield();
            lqe = entry.lirQueueEntry;
        }
        return lqe;
    }

    private void addToLir(Element entry)
    {
        QueueEntry<Element> qe = new QueueEntry<>(entry);
        entry.lirQueueEntry = qe;
        lirTail = qe.addToQueue(lirTail);
    }

    private void addToHir(Element entry)
    {
        QueueEntry<Element> qe = new QueueEntry<>(entry);
        entry.hirQueueEntry = qe;
        hirTail = qe.addToQueue(hirTail);
    }

    private void maybeDemote()
    {
        while (remainingLirSize < 0 && demoteOne()) {}
    }

    private boolean demoteOne()
    {
        QueueEntry<Element> first = lirHead.discardNextDeleted();
        Element e = first.content();
        if (e != null && e.demote())
            return true;
        if (e == null && first.discardNextDeleted() == first)
            // empty queue
            return false;

        // Another thread is racing against us. Ease off.
        Thread.yield();
        return true;
    }

    public void maybeEvict()
    {
        while (remainingSize < 0 && evictOne()) {}
    }

    private boolean evictOne()
    {
        QueueEntry<Element> first = hirHead.discardNextDeleted();
        Element e = first.content();
        if (e != null && e.evict())
            return true;
        if (e == null && first.discardNextDeleted() == first)
            // empty queue
            return false;

        // Another thread is racing against us. Ease off.
        Thread.yield();
        return true;
    }

    @Override
    public void clear()
    {
        while (demoteOne()) {}
        while (evictOne()) {}
    }

    public Element elementFor(Object key, EntryOwner owner)
    {
        return new Element(key, owner);
    }

    public String toString()
    {
        return getClass().getSimpleName();
    }
}
