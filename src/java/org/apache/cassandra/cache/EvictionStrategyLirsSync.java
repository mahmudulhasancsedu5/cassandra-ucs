package org.apache.cassandra.cache;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import sun.misc.Contended;

public class EvictionStrategyLirsSync implements EvictionStrategy
{
    @Contended
    volatile long remainingSize;
    @Contended
    volatile long remainingLirSize;

    volatile long capacity;

    final Weigher weigher;

    static final AtomicLongFieldUpdater<EvictionStrategyLirsSync> remainingSizeUpdater =
            AtomicLongFieldUpdater.newUpdater(EvictionStrategyLirsSync.class, "remainingSize");
    static final AtomicLongFieldUpdater<EvictionStrategyLirsSync> remainingLirSizeUpdater =
            AtomicLongFieldUpdater.newUpdater(EvictionStrategyLirsSync.class, "remainingLirSize");
    static final AtomicLongFieldUpdater<EvictionStrategyLirsSync> capacityUpdater =
            AtomicLongFieldUpdater.newUpdater(EvictionStrategyLirsSync.class, "capacity");

    enum State
    {
        NO_QUEUES,       // new
        HIRQ_ONLY,       // removed on eviction
        LIRQ_ONLY,       // not resident
        BOTH_QUEUES,     // resident, HIR status
        LIR_STATUS,      // lir status
        REMOVED;
    }

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
        public synchronized boolean casValue(Object old, Object v)
        {
            assert old != Specials.DISCARDED;
            assert v != Specials.DISCARDED;
            assert v != Specials.EMPTY;
            if (value != old)
                return false;
            value = v;

            long remSizeChange = -weigher.weigh(key, v);
            if (old != Specials.EMPTY)
                remSizeChange += weigher.weigh(key, old);
            remainingSizeUpdater.addAndGet(EvictionStrategyLirsSync.this, remSizeChange);
            if (state == State.LIR_STATUS)
                remainingLirSizeUpdater.addAndGet(EvictionStrategyLirsSync.this, remSizeChange);
            access();
            return true;
        }

        @Override
        public synchronized Object remove()
        {
            Object old = value;
            value = Specials.EMPTY;

            if (!EvictionStrategy.isSpecial(old))
            {
                long remSizeChange = weigher.weigh(key, old);
                remainingSizeUpdater.addAndGet(EvictionStrategyLirsSync.this, remSizeChange);
                if (state == State.LIR_STATUS)
                    remainingLirSizeUpdater.addAndGet(EvictionStrategyLirsSync.this, remSizeChange);
            }
            return old;
        }

        @Override
        public synchronized void access()
        {
            switch (state)
            {
            case LIR_STATUS:
                lirQueueEntry.delete();
                addToLir(this);
                return;
            case LIRQ_ONLY:
                lirQueueEntry.delete();
                addToLir(this);

                if (!EvictionStrategy.isSpecial(value))
                {
                    remainingLirSizeUpdater.addAndGet(EvictionStrategyLirsSync.this, -weigher.weigh(key, value));
                    maybeDemote();
                }

                state = State.LIR_STATUS;
                return;
            case BOTH_QUEUES:
                // Move to LIR.
                lirQueueEntry.delete();
                addToLir(this);
                hirQueueEntry.delete();
                hirQueueEntry = null;

                if (!EvictionStrategy.isSpecial(value))
                {
                    remainingLirSizeUpdater.addAndGet(EvictionStrategyLirsSync.this, -weigher.weigh(key, value));
                    maybeDemote();
                }

                state = State.LIR_STATUS;
                return;
            case HIRQ_ONLY:
                // we expect value to be non-null here
                // Move to LIR.
                assert lirQueueEntry == null;
                addToLir(this);
                hirQueueEntry.delete();
                addToHir(this);

                state = State.BOTH_QUEUES;
                return;
            case NO_QUEUES:
                // we expect value to be non-null here
                // Move to LIR.
                assert lirQueueEntry == null;
                addToLir(this);
                assert hirQueueEntry == null;
                addToHir(this);

                state = State.BOTH_QUEUES;
                return;
            case REMOVED:
                // We lost entry. Can't fix now.
                return;
            }
        }

        public String toString()
        {
            return state + ":" + key.toString();
        }

        public synchronized boolean evict()
        {
            switch (state)
            {
            case BOTH_QUEUES:
                hirQueueEntry.delete();
                hirQueueEntry = null;

                state = State.LIRQ_ONLY;
                owner.evict(this);
                return true;
            case HIRQ_ONLY:
                hirQueueEntry.delete();
                hirQueueEntry = null;

                state = State.NO_QUEUES;
                owner.evict(this);
                value = Specials.DISCARDED;
                owner.removeMapping(this);
                return true;
            default:
                return false;       // concurrent change, yield and retry
            }
        }

        public synchronized boolean demote()
        {
            switch (state)
            {
            case LIR_STATUS:
                lirQueueEntry.delete();
                lirQueueEntry = null;
                assert hirQueueEntry == null;
                addToHir(this);

                state = State.HIRQ_ONLY;

                if (!EvictionStrategy.isSpecial(value))
                    remainingLirSizeUpdater.addAndGet(EvictionStrategyLirsSync.this, weigher.weigh(key, value));
                return true;
            case LIRQ_ONLY:
                lirQueueEntry.delete();
                lirQueueEntry = null;
                assert hirQueueEntry == null;

                state = State.NO_QUEUES;
                value = Specials.DISCARDED;
                owner.removeMapping(this);
                return true;
            case BOTH_QUEUES:
                lirQueueEntry.delete();    // reload qe, it may have changed before we locked
                lirQueueEntry = null;
                assert hirQueueEntry != null;

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

    public EvictionStrategyLirsSync(Weigher weigher, long initialCapacity)
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
