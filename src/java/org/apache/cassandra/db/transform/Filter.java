package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.DeletionPurger;
import org.apache.cassandra.db.rows.*;

final class Filter extends Transformation<Unfiltered, UnfilteredRowIterator>
{
    private final boolean filterEmpty; // generally maps to !isForThrift, but also false for direct row filtration
    private final int nowInSec;
    public Filter(boolean filterEmpty, int nowInSec)
    {
        this.filterEmpty = filterEmpty;
        this.nowInSec = nowInSec;
    }

    @Override
    public Consumer<UnfilteredRowIterator> applyAsPartitionConsumer(Consumer/*<RowIterator>*/ nextConsumer)
    {
        return iterator ->
        {
            RowIterator filtered = new FilteredRows(iterator, this);

            if (filterEmpty && closeIfEmpty(filtered))
                return true; // skip

            return nextConsumer.accept(filtered);
        };
    }

    @Override
    public Row applyToStatic(Row row)
    {
        if (row.isEmpty())
            return Rows.EMPTY_STATIC_ROW;

        row = row.purge(DeletionPurger.PURGE_ALL, nowInSec);
        return row == null ? Rows.EMPTY_STATIC_ROW : row;
    }

    @Override
    public Consumer<Unfiltered> applyAsRowConsumer(Consumer/*<Row>*/ nextConsumer)
    {
        return unfiltered -> !(unfiltered instanceof Row)
                || nextConsumer.accept(((Row) unfiltered).purge(DeletionPurger.PURGE_ALL, nowInSec));
    }

    private static boolean closeIfEmpty(BaseRowIterator<?> iter)
    {
        if (iter.isEmpty())
        {
            iter.close();
            return true;
        }
        return false;
    }
}
