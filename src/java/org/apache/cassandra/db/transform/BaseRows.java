package org.apache.cassandra.db.transform;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionColumns;
import org.apache.cassandra.db.rows.*;

import static org.apache.cassandra.utils.Throwables.merge;

public abstract class BaseRows<OUT extends Unfiltered, I extends BaseRowIterator<? extends Unfiltered>>
extends BaseIterator<Unfiltered, I, OUT>
implements BaseRowIterator<OUT>
{

    private Row staticRow;

    public BaseRows(I input, Transformation transformation)
    {
        super(input, transformation);
        staticRow = transformation.applyToStatic(input.staticRow());

        // Clean up
        if (input instanceof BaseRows)
            ((BaseRows<?, ?>) input).staticRow = null;
    }

    public CFMetaData metadata()
    {
        return input.metadata();
    }

    public boolean isReverseOrder()
    {
        return input.isReverseOrder();
    }

    public PartitionColumns columns()
    {
        return input.columns();
    }

    public DecoratedKey partitionKey()
    {
        return input.partitionKey();
    }

    public Row staticRow()
    {
        return staticRow;
    }


    // **************************


    @Override
    protected Throwable runOnClose()
    {
        Throwable fail = prev != null ? prev.runOnClose() : null;
        try
        {
            transformation.onPartitionClose();
        }
        catch (Throwable t)
        {
            fail = merge(fail, t);
        }
        return fail;
    }

    @Override
    protected Consumer<Unfiltered> apply(Consumer<OUT> nextConsumer)
    {
        return value -> {
            @SuppressWarnings("unchecked")
            OUT transformed = (OUT) (value instanceof Row
                ? transformation.applyToRow((Row) value)
                : transformation.applyToMarker((RangeTombstoneMarker) value));
            return (transformed == null || nextConsumer.accept(transformed)) && !transformation.isDoneForPartition();
        };
    }
}
