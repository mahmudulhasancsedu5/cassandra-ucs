package org.apache.cassandra.cache;

public class EvictionStrategyLruSync extends EvictionStrategyFifoSync
{
    public EvictionStrategyLruSync(Weigher weigher, long capacity)
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
        e.currentQueueEntry = nqe;

        assert oqe.content() == e;
        oqe.delete();
        tail = nqe.addToQueue(tail);
    }
}
