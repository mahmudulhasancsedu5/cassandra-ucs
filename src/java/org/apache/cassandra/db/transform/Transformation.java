package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;

/**
 * We have a single common superclass for all Transformations to make implementation efficient.
 * we have a shared stack for all transformations, and can share the same transformation across partition and row
 * iterators, reducing garbage. Internal code is also simplified by always having a basic no-op implementation to invoke.
 *
 * Only the necessary methods need be overridden. Early termination is provided by invoking the method's stop or stopInPartition
 * methods, rather than having their own abstract method to invoke, as this is both more efficient and simpler to reason about.
 */
public abstract class Transformation<R extends Unfiltered, P extends BaseRowIterator<R>>
{
    /**
     * Run on the close of any (logical) partitions iterator this function was applied to
     *
     * We stipulate logical, because if applied to a transformed iterator the lifetime of the iterator
     * object may be longer than the lifetime of the "logical" iterator it was applied to; if the iterator
     * is refilled with MoreContents, for instance, the iterator may outlive this function
     */
    protected void onClose() { }

    /**
     * Construct a partition consumer suitable for the transformation. The default version applies
     * applyToPartition and isDone, but can be overridden for better control or performance.
     *
     * The method takes as parameter the next consumer in the chain and returns a consumer which takes
     * values as arguments and returns whether iteration should continue.
     *
     * Typically it would process the input and return the result of calling the next consumer, but it may also:
     *  - decide to throw away a value by not passing it on to the next consumer and returning true.
     *  - decide to terminate iteration by returning false; if done after passing the result to the next consumer
     *    iteration will terminate after that value is consumed, otherwise the value will not be part of the iteration.
     */
    protected Consumer<P> applyAsPartitionConsumer(Consumer<P> nextConsumer)
    {
        return nextConsumer;
    }

    /**
     * Run on the close of any (logical) rows iterator this function was applied to
     *
     * We stipulate logical, because if applied to a transformed iterator the lifetime of the iterator
     * object may be longer than the lifetime of the "logical" iterator it was applied to; if the iterator
     * is refilled with MoreContents, for instance, the iterator may outlive this function
     */
    protected void onPartitionClose() { }

    /**
     * Applied to the static row of any rows iterator.
     *
     * NOTE that this is only applied to the first iterator in any sequence of iterators filled by a MoreContents;
     * the static data for such iterators is all expected to be equal
     */
    protected Row applyToStatic(Row row)
    {
        return row;
    }

    /**
     * Applied to the partition-level deletion of any rows iterator.
     *
     * NOTE that this is only applied to the first iterator in any sequence of iterators filled by a MoreContents;
     * the static data for such iterators is all expected to be equal
     */
    protected DeletionTime applyToDeletion(DeletionTime deletionTime)
    {
        return deletionTime;
    }

    /**
     * Construct a partition consumer suitable for the transformation. The default version applies
     * applyToRow, applyToMarker and isDoneForPartition, but can be overridden for better control or performance.
     *
     * The method takes as parameter the next consumer in the chain and returns a consumer which takes
     * values as arguments and returns whether iteration should continue.
     *
     * Typically it would process the input and return the result of calling the next consumer, but it may also:
     *  - decide to throw away a value by not passing it on to the next consumer and returning true.
     *  - decide to terminate iteration by returning false; if done after passing the result to the next consumer
     *    iteration will terminate after that value is consumed, otherwise the value will not be part of the iteration.
     */
    protected Consumer<R> applyAsRowConsumer(Consumer<R> nextConsumer)
    {
        return nextConsumer;
    }

    //******************************************************
    //          Static Application Methods
    //******************************************************


    public static UnfilteredPartitionIterator apply(UnfilteredPartitionIterator iterator, Transformation<Unfiltered, ? super UnfilteredRowIterator> transformation)
    {
        return new UnfilteredPartitions(iterator, transformation);
    }
    public static PartitionIterator apply(PartitionIterator iterator, Transformation<Row, ? super RowIterator> transformation)
    {
        return new FilteredPartitions(iterator, transformation);
    }
    public static UnfilteredRowIterator apply(UnfilteredRowIterator iterator, Transformation<Unfiltered, ?> transformation)
    {
        return new UnfilteredRows(iterator, transformation);
    }
    public static RowIterator apply(RowIterator iterator, Transformation<Row, ?> transformation)
    {
        return new FilteredRows(iterator, transformation);
    }

    public static void assertInputExhausted(UnfilteredRowIterator iter)
    {
        assert !((BaseIterator<?, ?, ?>) iter).input.hasNext();
    }
}
