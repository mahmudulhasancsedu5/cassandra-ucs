package org.apache.cassandra.cache;

public class EvictionStrategyLru<Key, Value> extends EvictionStrategyFifo<Key, Value>
{
    @Override
    public void access(Entry<Key, Value> e)
    {
        QueueEntry<Entry<Key, Value>> oqe = e.currentQueueEntry;
        if (oqe == null)
            return;     // In the process of being removed, or not added yet. Can't access.
        QueueEntry<Entry<Key, Value>> nqe = new QueueEntry<>(e);
        if (!Entry.currentQueueEntryUpdater.compareAndSet(e, oqe, nqe))
            return;     // something else moved e at the same time, that's sufficient

        assert oqe.content() == e;
        oqe.delete();
        tail = nqe.addToQueue(tail);
    }
}
