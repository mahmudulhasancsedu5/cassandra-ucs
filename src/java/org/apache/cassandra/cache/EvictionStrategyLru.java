package org.apache.cassandra.cache;

public class EvictionStrategyLru extends EvictionStrategyFifo
{
    public EvictionStrategyLru(Weigher weigher, long capacity)
    {
        super(weigher, capacity);
    }

    @Override
    public void access(Element e)
    {
        QueueEntry<Element> oqe = e.currentQueueEntry;
        if (oqe == null)
            return;     // In the process of being removed, or not added yet. Can't access.
        QueueEntry<Element> nqe = new QueueEntry<>(e);
        if (!currentQueueEntryUpdater.compareAndSet(e, oqe, nqe))
            return;     // something else moved e at the same time, that's sufficient

        assert oqe.content() == e;
        oqe.delete();
        tail = nqe.addToQueue(tail);
    }
}
