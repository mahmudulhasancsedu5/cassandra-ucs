package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;

// a shared internal interface, that is hidden to provide type-safety to the user
abstract class MoreContents<ITER, R extends Unfiltered, P extends BaseRowIterator<R>> extends Transformation<R, P>
{
    public abstract ITER moreContents();
}

