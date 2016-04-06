package org.apache.cassandra.cache;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import sun.misc.Contended;

public class EvictionStrategyFifo implements EvictionStrategy
{
    @Contended
    volatile long remainingSize;
    volatile long capacity;

    final Weigher weigher;

    static final AtomicLongFieldUpdater<EvictionStrategyFifo> remainingSizeUpdater =
            AtomicLongFieldUpdater.newUpdater(EvictionStrategyFifo.class, "remainingSize");
    static final AtomicLongFieldUpdater<EvictionStrategyFifo> capacityUpdater =
            AtomicLongFieldUpdater.newUpdater(EvictionStrategyFifo.class, "capacity");

    static final AtomicReferenceFieldUpdater<Element, Object> valueUpdater =
            AtomicReferenceFieldUpdater.newUpdater(Element.class, Object.class, "value");
    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<Element, QueueEntry> currentQueueEntryUpdater =
            AtomicReferenceFieldUpdater.newUpdater(Element.class, QueueEntry.class, "currentQueueEntry");

    class Element implements EvictionStrategy.Entry
    {
        final Object key;
        final EntryOwner owner;
        volatile Object value = Specials.EMPTY;
        volatile QueueEntry<Element> currentQueueEntry = null;


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
            if (!valueUpdater.compareAndSet(this, old, v))
                return false;

            long remSizeChange = -weigher.weigh(key, v);
            if (old == Specials.EMPTY)
                addToQueue(this);
            else
            {
                access();
                remSizeChange += weigher.weigh(key, old);
            }
            remainingSizeUpdater.addAndGet(EvictionStrategyFifo.this, remSizeChange);
            return true;
        }

        @Override
        public void access()
        {
            EvictionStrategyFifo.this.access(this);
        }

        @Override
        public Object remove()
        {
            Object old = value;
            if (!valueUpdater.compareAndSet(this, old, Specials.DISCARDED))
                return null;

            if (!(old instanceof Specials))
                remainingSizeUpdater.addAndGet(EvictionStrategyFifo.this, weigher.weigh(key, old));
            removeFromQueue(this);
            owner.removeMapping(this);
            return old;
        }
    }

    final QueueEntry<Element> head = new QueueEntry<>(null);
    volatile QueueEntry<Element> tail = head;

    public EvictionStrategyFifo(Weigher weigher, long capacity)
    {
        this.capacity = capacity;
        this.remainingSize = capacity;
        this.weigher = weigher;
    }
    
    @Override
    public Element elementFor(Object key, EntryOwner owner)
    {
        return new Element(key, owner);
    }

    public void access(Element e)
    {
        // FIFO strategy: do nothing on access, let entries flow through queue.
    }

    public void addToQueue(Element e)
    {
        QueueEntry<Element> qe = new QueueEntry<>(e);
        assert e.currentQueueEntry == null;
        e.currentQueueEntry = qe;

        tail = qe.addToQueue(tail);
    }

    public boolean removeFromQueue(Element e)
    {
        QueueEntry<Element> qe;
        do
        {
            qe = e.currentQueueEntry;
            if (qe == null)
                return false; // already removed by another thread
        }
        while (!currentQueueEntryUpdater.compareAndSet(e, qe, null));
        assert qe.content() == e;

        qe.delete();
        return true;
    }

    @Override
    public void maybeEvict()
    {
        while (remainingSize < 0)
        {
            Element e = takeFirst();
            if (e == null)
                return;
            e.owner.evict(e);
        }
    }

    public Element takeFirst()
    {
        for ( ; ; )
        {
            QueueEntry<Element> first = head.discardNextDeleted();
            Element content = first.content();
            if (content != null)
                return content;
            // we are either at an empty queue with null sentinel, or something removed our entry and we need to get another
            if (first.discardNextDeleted() == first)
                return null;
        }
    }

    @Override
    public void clear()
    {
        while (true)
        {
            Element e = takeFirst();
            if (e == null)
                return;
            e.owner.evict(e);
        }
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
}
