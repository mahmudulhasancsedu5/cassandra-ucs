package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.rows.*;

/**
 * An interface for providing new row contents for a partition.
 *
 * The new contents are produced as a normal arbitrary RowIterator or UnfilteredRowIterator (as appropriate),
 * with matching staticRow, partitionKey and partitionLevelDeletion.
 *
 * The transforming iterator invokes this method when any current source is exhausted, then then inserts the
 * new contents as the new source.
 *
 * If the new source is itself a product of any transformations, the two transforming iterators are merged
 * so that control flow always occurs at the outermost point
 */
public abstract class MoreRows<R extends Unfiltered, I extends BaseRowIterator<R>> extends MoreContents<I, R, I>
{

    public static UnfilteredRowIterator extend(UnfilteredRowIterator iterator, MoreRows<Unfiltered, ? super UnfilteredRowIterator> more)
    {
        return new UnfilteredRows(iterator, more);
    }

    public static RowIterator extend(RowIterator iterator, MoreRows<Row, ? super RowIterator> more)
    {
        return new FilteredRows(iterator, more);
    }

}

