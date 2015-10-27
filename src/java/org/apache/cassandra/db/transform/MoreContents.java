package org.apache.cassandra.db.transform;

import org.apache.cassandra.db.rows.BaseRowIterator;
import org.apache.cassandra.db.rows.Unfiltered;

// a shared internal interface, that is hidden to provide type-safety to the user
abstract class MoreContents<ITER, P extends BaseRowIterator<?>> extends Transformation<P>
{
    public abstract ITER moreContents();
}

