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
    final RemovalListener removalListener;

    static final AtomicLongFieldUpdater<EvictionStrategyLirsSync> remainingSizeUpdater =
            AtomicLongFieldUpdater.newUpdater(EvictionStrategyLirsSync.class, "remainingSize");
    static final AtomicLongFieldUpdater<EvictionStrategyLirsSync> remainingLirSizeUpdater =
            AtomicLongFieldUpdater.newUpdater(EvictionStrategyLirsSync.class, "remainingLirSize");
    static final AtomicLongFieldUpdater<EvictionStrategyLirsSync> capacityUpdater =
            AtomicLongFieldUpdater.newUpdater(EvictionStrategyLirsSync.class, "capacity");

    class Element implements Entry
    {
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
            {
                remSizeChange += weigher.weigh(key, old);
                removalListener.onRemove(key, old);
            }
            remainingSizeUpdater.addAndGet(EvictionStrategyLirsSync.this, remSizeChange);
            if (lirQueueEntry != null && hirQueueEntry == null)
                remainingLirSizeUpdater.addAndGet(EvictionStrategyLirsSync.this, remSizeChange);
            access();
            maybeDemote();
            maybeEvict();
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
                removalListener.onRemove(key, old);
                remainingSizeUpdater.addAndGet(EvictionStrategyLirsSync.this, remSizeChange);
                if (lirQueueEntry != null && hirQueueEntry == null)
                    remainingLirSizeUpdater.addAndGet(EvictionStrategyLirsSync.this, remSizeChange);
            }
            return old;
        }

        @Override
        public synchronized void access()
        {
            if (EvictionStrategy.isSpecial(value))
                // Lost value. No point promoting.
                return;

            if (lirQueueEntry != null)
            {
                lirQueueEntry.delete();
                addToLir(this);
                if (hirQueueEntry != null)
                {
                    // HIR-resident-to-LIR promotion
                    hirQueueEntry.delete();
                    hirQueueEntry = null;

                    remainingLirSizeUpdater.addAndGet(EvictionStrategyLirsSync.this, -weigher.weigh(key, value));
                    maybeDemote();
                }
                return;
            }
            else
            {
                if (hirQueueEntry != null)
                    hirQueueEntry.delete();

                // new value (or resident HIR not seen recently) goes to tail of both queues
                addToLir(this);
                addToHir(this);
            }
        }

        public String toString()
        {
            return (lirQueueEntry != null ? "L" : "") +
                   (hirQueueEntry != null ? "H" : "") +
                   (EvictionStrategy.isSpecial(value) ? "" : "R") +
                   ":" + key.toString();
        }

        public synchronized boolean evict()
        {
            if (hirQueueEntry == null)
                return false;       // concurrent change, yield and retry

            remove();
            hirQueueEntry.delete();
            hirQueueEntry = null;
            if (lirQueueEntry == null)
            {
                value = Specials.DISCARDED;
                owner.removeMapping(this);
            }
            return true;
        }

        public synchronized boolean demote()
        {
            if (lirQueueEntry == null)
                return false;       // concurrent change, yield and retry

            lirQueueEntry.delete();
            lirQueueEntry = null;
            if (hirQueueEntry == null)
            {
                if (value != Specials.EMPTY)
                {
                    // LIR-to-HIR transition
                    remainingLirSizeUpdater.addAndGet(EvictionStrategyLirsSync.this, weigher.weigh(key, value));
                    addToHir(this);
                }
                else
                {
                    // HIR non-resident removal
                    value = Specials.DISCARDED;
                    owner.removeMapping(this);
                }
            }
            // else resident hit loses LIRQ
            return true;
        }
    }

    final QueueEntry<Element> lirHead = new QueueEntry<>(null);
    @Contended
    volatile QueueEntry<Element> lirTail = lirHead;
    final QueueEntry<Element> hirHead = new QueueEntry<>(null);
    @Contended
    volatile QueueEntry<Element> hirTail = hirHead;

    public EvictionStrategyLirsSync(RemovalListener removalListener, Weigher weigher, long initialCapacity)
    {
        this.weigher = weigher;
        this.removalListener = removalListener;
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
