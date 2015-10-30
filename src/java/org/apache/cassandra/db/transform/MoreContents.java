package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;

// a shared internal interface, that is hidden to provide type-safety to the user
abstract class MoreContents<ITER, R extends Unfiltered, P extends BaseRowIterator<R>> extends Transformation<R, P>
{
    public abstract ITER moreContents();

    @Override
    protected Consumer<P> applyAsPartitionConsumer(Consumer<P> nextConsumer)
    {
        return nextConsumer;
    }

    @Override
    protected Consumer<R> applyAsRowConsumer(Consumer<R> nextConsumer)
    {
        return nextConsumer;
    }

    @Override
    protected final P applyToPartition(P partition)
    {
        throw new IllegalStateException("Method should not be called.");
    }

    @Override
    protected final Row applyToRow(Row row)
    {
        throw new IllegalStateException("Method should not be called.");
    }

    @Override
    protected final RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
    {
        throw new IllegalStateException("Method should not be called.");
    }

    @Override
    protected final boolean isDoneForPartition()
    {
        throw new IllegalStateException("Method should not be called.");
    }

    @Override
    protected final boolean isDone()
    {
        throw new IllegalStateException("Method should not be called.");
    }
}

