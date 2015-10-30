package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;

final class UnfilteredRows extends BaseRows<Unfiltered, UnfilteredRowIterator> implements UnfilteredRowIterator
{
    private DeletionTime partitionLevelDeletion;

    public UnfilteredRows(UnfilteredRowIterator input, Transformation trans)
    {
        super(input, trans);
        partitionLevelDeletion = trans.applyToDeletion(input.partitionLevelDeletion());

        // Clean up.
        if (input instanceof UnfilteredRows)
            ((UnfilteredRows) input).partitionLevelDeletion = null;
    }

    public DeletionTime partitionLevelDeletion()
    {
        return partitionLevelDeletion;
    }

    public EncodingStats stats()
    {
        return input.stats();
    }

    @Override
    public boolean isEmpty()
    {
        return staticRow().isEmpty() && partitionLevelDeletion().isLive() && !hasNext();
    }
}
