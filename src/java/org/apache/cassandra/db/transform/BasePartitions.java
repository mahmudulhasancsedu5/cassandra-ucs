package org.apache.cassandra.db.transform;


import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.rows.BaseRowIterator;

import static org.apache.cassandra.utils.Throwables.merge;

public abstract class BasePartitions<OUT extends BaseRowIterator<?>, ITER extends BasePartitionIterator<? extends BaseRowIterator<?>>>
extends BaseIterator<BaseRowIterator<?>, ITER, OUT>
implements BasePartitionIterator<OUT>
{

    public BasePartitions(ITER input, Transformation trans)
    {
        super(input, trans);
        transformation.attachTo(this);
    }

    // *********************************



    @Override
    protected Throwable runOnClose()
    {
        Throwable fail = prev != null ? prev.runOnClose() : null;
        try
        {
            transformation.onClose();
        }
        catch (Throwable t)
        {
            fail = merge(fail, t);
        }
        return fail;
    }

    @Override
    protected Consumer<BaseRowIterator<?>> apply(Consumer<OUT> nextConsumer)
    {
        return value -> {
            @SuppressWarnings("unchecked")
            OUT transformed = (OUT) transformation.applyToPartition(value);
            return transformed == null || nextConsumer.accept(transformed);
        };
    }
}

