package org.apache.cassandra.cache;

public class EvictionStrategyLru<Key, Value> extends EvictionStrategyFifo<Key, Value>
{
    @Override
    public void access(Entry<Key, Value> e)
    {
        QueueEntry<Entry<Key, Value>> oqe = e.currentQueueEntry.get();
        if (oqe == null)
            return;     // In the process of being removed, or not added yet. Can't access.
        QueueEntry<Entry<Key, Value>> nqe = new QueueEntry<>(e);
        if (!e.currentQueueEntry.compareAndSet(oqe, nqe))
            return;     // something else moved e at the same time, that's sufficient

        assert oqe.content() == e;
        oqe.delete();
        tail = nqe.addToQueue(tail);
    }
}
