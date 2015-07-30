/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import java.lang.reflect.Array;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import net.nicoulaj.compilecommand.annotations.DontInline;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Throwables;

/**
 * A class for applying multiple transformations to row or partitions iterators.
 * The intention is to reduce the cognitive (and cpu) burden of deep nested method calls, by accumulating a stack of
 * transformations in one outer iterator.
 *
 * This is achieved by detecting the transformation of an already transformed iterator, and simply pushing
 * the new function onto the top of the stack of existing ones.
 *
 * There is a degree of shared functionality between both RowIterator and PartitionIterator, encapsulated in the
 * Abstract class. This is then extended by each of BasePartitions and BaseRows, which perform all of the heavy
 * lifting for each of their class of iterator, with each of their extensions just providing the correct front-end
 * interface implementations.
 *
 * Filtering is performed as another function that is pushed onto the stack, but can only be created internally
 * to this class, as it performs the transition from Unfiltered -> Filtered, which results in the swap of the front-end
 * interface (from UnfilteredRowIterator/UnfilteredPartitionIterator -> RowIterator/PartitionIterator).
 *
 * This permits us to use the exact same iterator for the entire stack.
 *
 * Advanced use permits swapping the contents of the transformer once it has been exhausted; this is useful
 * for concatenation (inc. short-read protection). When such a function is attached, the current snapshot of
 * the function stack is saved, and when it is *executed* this part of the stack is removed (leaving only those
 * functions that were applied later, i.e. on top of the concatenated stream). If the new iterator is itself
 * a transformer, its contents are inserted directly into this stream, and its function stack prefixed to our remaining
 * stack. This sounds complicated, but the upshot is quite straightforward: a single point of control flow even across
 * multiple concatenated streams of information.
 *
 * Note that only the normal iterator stream is affected by a refill; the caller must ensure that everything
 * else is the same for all concatenated streams. Notably static rows, deletions, metadatas are all retained from
 * the first iterator.
 */
public class Transformer
{

    /** DECLARATIONS **/

    /**
     * This is a placeholder to mark a type of Function accepted by this Transformer.
     * We use it to provide a simpler interface, i.e. a single static method call for applying a function
     * to an iterator, and so that an arbitrary selection of functions may be easily implemented by the same class
     */
    public static interface Function {}

    private static interface BasePartitionFunction<I extends BaseRowIterator<?>, O extends BaseRowIterator<?>> extends Function
    {
        O applyToPartition(I partition);
    }

    // transform a partition (RowIterator -> RowIterator)
    public static interface PartitionFunction extends BasePartitionFunction<RowIterator, RowIterator> {}

    // transform an unfiltered partition (UnfilteredRowIterator -> UnfilteredRowIterator)
    public static interface UnfilteredPartitionFunction extends BasePartitionFunction<UnfilteredRowIterator, UnfilteredRowIterator> {}

    // transform a Row (Row -> Row)
    public static interface RowFunction extends Function
    {
        Row applyToRow(Row row);
    }

    // transform a static Row (Row -> Row)
    public static interface StaticRowFunction extends Function
    {
        Row applyToStatic(Row row);
    }

    // transform a RangeTombstoneMarker (RTM -> RTM)
    public static interface MarkerFunction extends Function
    {
        RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker);
    }

    // transform a partition deletion (DeletionTime -> DeletionTime)
    public static interface DeletionFunction
    {
        DeletionTime applyToDeletion(DeletionTime deletionTime);
    }

    // Terminate iteration early; called prior to each next() call on the base iterator
    public static interface EarlyTermination extends Function
    {
        boolean terminate();
    }

    // perform cleanup when the iterator is closed
    public static interface RunOnClose extends Function
    {
        void runOnClose();
    }

    private static interface Refill<I> extends Function
    {
        /**
         * @return null if there are no more contents to produce; otherwise an iterator
         * of the same type as the function is applied to.
         */
        I newContents();
    }

    // TODO: currently it isn't compile-time guaranteed that the right type of refill is provided for a given
    // stage in the pipeline (or even rows vs partitions). Not sure if it's worth the added ugliness.
    public static interface RefillPartitions extends Refill<BasePartitionIterator<?>>
    {
    }

    public static interface RefillRows extends Refill<BaseRowIterator<?>>
    {
    }

    /** STATIC TRANSFORMATION ACCESSORS **/

    /**
     * Apply a function to this UnfilteredRowIterator
     */
    public static UnfilteredPartitionIterator apply(UnfilteredPartitionIterator iterator, Function function)
    {
        if (EmptyIterators.isEmpty(iterator))
            return iterator;

        if (iterator instanceof UnfilteredPartitions) // already being transformed, so just add our function
            return new UnfilteredPartitions(function, (UnfilteredPartitions) iterator);
        return new UnfilteredPartitions(iterator, function);
    }

    /**
     * Apply a function to this RowIterator
     */
    public static PartitionIterator apply(PartitionIterator iterator, Function function)
    {
        if (EmptyIterators.isEmpty(iterator))
            return iterator;

        assert !(function instanceof BasePartitionFunction) || function instanceof PartitionFunction;

        if (iterator instanceof Partitions) // already being transformed, so just add our function
            return new Partitions(function, (Partitions) iterator);
        return new Partitions(iterator, function);
    }

    /**
     * Apply a function to this UnfilteredRowIterator
     */
    public static UnfilteredRowIterator apply(UnfilteredRowIterator iterator, Function function)
    {
        // we don't call .isEmpty() so that we minimize number of iteration steps performed outside of outer loop
        if (EmptyIterators.isEmpty(iterator))
            return iterator;

        if (iterator instanceof UnfilteredRows) // already being transformed, so just add our function
            return new UnfilteredRows(function, (UnfilteredRows) iterator);
        return new UnfilteredRows(iterator, function);
    }

    /**
     * Apply a function to this RowIterator
     */
    public static RowIterator apply(RowIterator iterator, Function function)
    {
        // we don't call .isEmpty() so that we minimize number of iteration steps performed outside of outer loop
        if (EmptyIterators.isEmpty(iterator))
            return iterator;

        if (iterator instanceof FilteredRows) // already being transformed, so just add our function
            return new FilteredRows(function, (FilteredRows) iterator);
        return new FilteredRows(iterator, function);
    }

    /** FILTRATION **/

    /**
     * Filter any RangeTombstoneMarker from the iterator's iterators, transforming it into a PartitionIterator.
     */
    public static PartitionIterator filter(UnfilteredPartitionIterator iterator, int nowInSecs)
    {
        if (EmptyIterators.isEmpty(iterator))
            return EmptyIterators.partition();

        Filter filter = new Filter(nowInSecs);
        if (iterator instanceof UnfilteredPartitions)
            return new Partitions(filter, (UnfilteredPartitions) iterator);
        return new Partitions(iterator, filter);
    }

    /**
     * Filter any RangeTombstoneMarker from the iterator, transforming it into a RowIterator.
     */
    public static RowIterator filter(UnfilteredRowIterator iterator, int nowInSecs)
    {
        // we don't call .isEmpty() so that we minimize number of iteration steps performed outside of outer loop
        if (EmptyIterators.isEmpty(iterator))
            return EmptyIterators.row(iterator.metadata(), iterator.partitionKey(), iterator.isReverseOrder());

        Filter filter = new Filter(nowInSecs);
        if (iterator instanceof UnfilteredRows)
            return new FilteredRows(filter, (UnfilteredRows) iterator);
        return new FilteredRows(iterator, filter);
    }

    /**
     * Simple function that filters out range tombstone markers, and purges tombstones
     */
    private static final class Filter implements BasePartitionFunction<UnfilteredRowIterator, RowIterator>,
                                                 StaticRowFunction, RowFunction, MarkerFunction
    {
        private final int nowInSec;
        public Filter(int nowInSec)
        {
            this.nowInSec = nowInSec;
        }

        public RowIterator applyToPartition(UnfilteredRowIterator partition)
        {
            return Transformer.filter(partition, nowInSec);
        }

        public Row applyToStatic(Row row)
        {
            if (row.isEmpty())
                return Rows.EMPTY_STATIC_ROW;

            row = row.purge(DeletionPurger.PURGE_ALL, nowInSec);
            return row == null ? Rows.EMPTY_STATIC_ROW : row;
        }

        public Row applyToRow(Row row)
        {
            return row.purge(DeletionPurger.PURGE_ALL, nowInSec);
        }

        public RangeTombstoneMarker applyToMarker(RangeTombstoneMarker marker)
        {
            return null;
        }
    }

     /* ROWS IMPLEMENTATION */

    private static final class UnfilteredRows extends BaseRows<Unfiltered, UnfilteredRowIterator> implements UnfilteredRowIterator
    {
        private final DeletionTime partitionLevelDeletion;

        public UnfilteredRows(UnfilteredRowIterator input, Function apply)
        {
            super(input, apply);
            partitionLevelDeletion = applyToPartitionDeletion(input.partitionLevelDeletion(), apply);
        }

        public UnfilteredRows(Function apply, UnfilteredRows copyFrom)
        {
            super(apply, copyFrom);
            partitionLevelDeletion = applyToPartitionDeletion(copyFrom.partitionLevelDeletion, apply);
        }

        private static DeletionTime applyToPartitionDeletion(DeletionTime dt, Function apply)
        {
            if (apply instanceof DeletionFunction)
                dt = ((DeletionFunction) apply).applyToDeletion(dt);
            return dt;
        }

        public DeletionTime partitionLevelDeletion()
        {
            return partitionLevelDeletion;
        }

        public EncodingStats stats()
        {
            return input.stats();
        }

        public boolean isEmpty()
        {
            return staticRow().isEmpty() && partitionLevelDeletion().isLive() && !hasNext();
        }
    }

    private static final class FilteredRows extends BaseRows<Row, BaseRowIterator<?>> implements RowIterator
    {
        FilteredRows(RowIterator input, Function apply)
        {
            super(input, apply);
            assert !(apply instanceof MarkerFunction);
        }

        FilteredRows(BaseRowIterator<?> input, Filter apply)
        {
            super(input, apply);
        }

        FilteredRows(Function apply, BaseRows<?, ? extends BaseRowIterator<?>> copyFrom)
        {
            super(apply, copyFrom);
            assert !(apply instanceof MarkerFunction) || apply instanceof Filter;
        }

        public boolean isEmpty()
        {
            return staticRow().isEmpty() && !hasNext();
        }
    }

    private static abstract class BaseRows<R extends Unfiltered, I extends BaseRowIterator<? extends Unfiltered>> extends Abstract<Unfiltered, I, R> implements BaseRowIterator<R>
    {
        private Row staticRow;

        public BaseRows(I input, Function apply)
        {
            super(input, apply);
            staticRow = applyToStatic(input.staticRow(), apply);
        }

        // swap parameter order to avoid casting errors
        BaseRows(Function apply, BaseRows<?, ? extends I> copyFrom)
        {
            super(apply, copyFrom);
            staticRow = applyToStatic(copyFrom.staticRow, apply);
            if (copyFrom.next != null)
                next = applyToExisting(copyFrom.next, apply);
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



        // *********** /Begin Core Debug Point



        private static Row applyToStatic(Row row, Function apply)
        {
            if (apply instanceof StaticRowFunction)
                row = ((StaticRowFunction) apply).applyToStatic(row);
            return row;
        }

        private static Unfiltered applyToExisting(Unfiltered unfiltered, Function apply)
        {
            return unfiltered.isRow() ? (apply instanceof RowFunction ? ((RowFunction) apply).applyToRow((Row) unfiltered) : unfiltered)
                                      : (apply instanceof MarkerFunction ? ((MarkerFunction) apply).applyToMarker((RangeTombstoneMarker) unfiltered) : unfiltered);
        }

        protected Unfiltered applyTo(Unfiltered next)
        {
            if (next.isRow())
            {
                Row row = (Row) next;
                RowFunction[] fs = applyToRow;
                for (int i = 0 ; row != null && i < fs.length ; i++)
                    row = fs[i].applyToRow(row);
                return row;
            }
            else
            {
                RangeTombstoneMarker rtm = (RangeTombstoneMarker) next;
                MarkerFunction[] fs = applyToMarker;
                for (int i = 0 ; rtm != null && i < fs.length ; i++)
                    rtm = fs[i].applyToMarker(rtm);
                return rtm;
            }
        }



        // *********** /End Core Debug Point

    }

    /* PARTITIONS IMPLEMENTATION */

    private static final class UnfilteredPartitions extends BasePartitions<UnfilteredRowIterator, UnfilteredPartitionIterator> implements UnfilteredPartitionIterator
    {
        public UnfilteredPartitions(UnfilteredPartitionIterator input, Function apply)
        {
            super(input, apply, input.isForThrift());
        }

        public UnfilteredPartitions(Function apply, UnfilteredPartitions copyFrom)
        {
            super(apply, copyFrom);
        }

        public boolean isForThrift()
        {
            return input.isForThrift();
        }

        public CFMetaData metadata()
        {
            return input.metadata();
        }
    }

    private static final class Partitions extends BasePartitions<RowIterator, BasePartitionIterator<?>> implements PartitionIterator
    {
        Partitions(PartitionIterator input, Function apply)
        {
            super(input, apply, false);
        }

        Partitions(UnfilteredPartitionIterator input, Filter apply)
        {
            super(input, apply, input.isForThrift());
        }

        Partitions(Function apply, BasePartitions<?, ? extends BasePartitionIterator<?>> copyFrom)
        {
            super(apply, copyFrom);
        }
    }

    private static abstract class BasePartitions<R extends BaseRowIterator<?>,
                                                I extends BasePartitionIterator<? extends BaseRowIterator<?>>>
    extends Abstract<BaseRowIterator<?>, I, R>
    implements BasePartitionIterator<R>
    {
        final boolean isForThrift;
        public BasePartitions(I input, Function apply, boolean isForThrift)
        {
            super(input, apply);
            this.isForThrift = isForThrift;
        }



        // *********** /Begin Core Debug Point



        BasePartitions(Function apply, BasePartitions<?, ? extends I> copyFrom)
        {
            super(apply, copyFrom);
            isForThrift = copyFrom.isForThrift;
            if (copyFrom.next != null)
            {
                next = (R) ((apply instanceof BasePartitionFunction)
                            ? ((BasePartitionFunction) apply).applyToPartition(copyFrom.next)
                            : copyFrom.next);
            }
        }

        protected BaseRowIterator<?> applyTo(BaseRowIterator<?> next)
        {
            try
            {
                BasePartitionFunction[] fs = applyToPartition;
                for (int i = 0 ; next != null & i < fs.length ; i++)
                    next = fs[i].applyToPartition(next);

                if (next != null && (isForThrift || !next.isEmpty()))
                    return next;
            }
            catch (Throwable t)
            {
                Throwables.close(t, Collections.singleton(next));
                throw t;
            }

            if (next != null)
                next.close();
            return null;
        }



        // *********** /End Core Debug Point
    }

    /** SHARED IMPLEMENTATION **/

    private abstract static class Abstract<V, I extends CloseableIterator<? extends V>, O extends V> extends Stack implements AutoCloseable
    {
        I input;
        V next;

        // responsibility for initialising next lies with the subclass
        private Abstract(Function apply, Abstract<?, ? extends I, ?> copyFrom)
        {
            super(copyFrom, apply);
            this.input = copyFrom.input;
        }

        private Abstract(I input, Function apply)
        {
            super(EMPTY, apply);
            this.input = input;
        }

        public void close()
        {
            Throwables.perform(Stream.of(runOnClose).map((f) -> f::runOnClose),
                               () -> { if (next instanceof CloseableIterator) ((CloseableIterator) next).close(); },
                               input::close);
        }

        protected abstract V applyTo(V next);



        // *********** /Begin Core Debug Point



        protected boolean isDone()
        {
            for (EarlyTermination et : earlyTermination)
                if (et.terminate())
                    return true;
            return false;
        }

        public final boolean hasNext()
        {
            while (   next == null
                   && !isDone()
                   && (input.hasNext()
                       || (refill.length > 0 && refill())))
            {
                V next = input.next();
                this.next = applyTo(next);
            }
            return next != null;
        }

        public final O next()
        {
            if (next == null && !hasNext())
                throw new NoSuchElementException();

            O next = (O) this.next;
            this.next = null;
            return next;
        }



        // *********** /End Core Debug Point



        @DontInline
        protected boolean refill()
        {
            for (RefillSnapshot snap : refill)
            {
                while (true)
                {
                    I next = (I) snap.function.newContents();
                    if (next == null)
                        break;

                    input.close();
                    input = next;
                    Stack prefix = EMPTY;
                    if (next instanceof Abstract)
                    {
                        Abstract abstr = (Abstract) next;
                        input = (I) abstr.input;
                        prefix = abstr;
                    }

                    // since we're truncating our function stack to only those occurring after the extend function
                    // we have to run any prior runOnClose methods
                    for (int j = 0; j < snap.runOnClose; j++)
                        runOnClose[j].runOnClose();

                    resetRefills(prefix, snap);

                    if (input.hasNext())
                        return true;
                }
            }
            return false;
        }

        // reinitialise the refill states after a refill
        private void resetRefills(Stack prefix, RefillSnapshot snap)
        {
            // save the current snapshot position (same as snap)
            RefillSnapshot delta = new RefillSnapshot(null, this);

            // drop the functions that were present when the Refill method was attached,
            // and prefix any functions in the new contents (if it's a transformer)
            applyToPartition = splice(prefix.applyToPartition, applyToPartition, snap.applyToPartition);
            applyToRow = splice(prefix.applyToRow, applyToRow, snap.applyToRow);
            applyToMarker = splice(prefix.applyToMarker, applyToMarker, snap.applyToMarker);
            earlyTermination = splice(prefix.earlyTermination, earlyTermination, snap.earlyTermination);
            runOnClose = splice(prefix.runOnClose, runOnClose, snap.runOnClose);
            refill = splice(prefix.refill, refill, snap.refill);

            // reset the position of the refill method we're using
            snap.init(prefix);
            // then calculate the delta from our original positions, and apply this delta to each of the remaining refills
            delta.subtract(snap);
            for (int i = 1 ; i < refill.length ; i++)
                refill[i].subtract(delta);
        }
    }

    /**
     * a collection of function stacks
     *
     * for simplicity/legibility, we share this between partition and row transformers; this doesn't lead to an appreciable
     * amount of waste, but prevents a lot of unnecessary and duplicated boilerplate
     */
    private static class Stack
    {
        static final Stack EMPTY = new Stack();

        BasePartitionFunction[] applyToPartition;
        RowFunction[] applyToRow;
        MarkerFunction[] applyToMarker;
        EarlyTermination[] earlyTermination;
        RunOnClose[] runOnClose;
        RefillSnapshot[] refill;

        private Stack()
        {
            applyToPartition = new BasePartitionFunction[0];
            applyToRow = new RowFunction[0];
            applyToMarker = new MarkerFunction[0];
            earlyTermination = new EarlyTermination[0];
            runOnClose = new RunOnClose[0];
            refill = new RefillSnapshot[0];
        }

        // push the provided function onto any matching stack types
        private Stack(Stack copyFrom, Function add)
        {
            this.applyToPartition = maybeAppend(BasePartitionFunction.class, copyFrom.applyToPartition, add);
            this.applyToRow = maybeAppend(RowFunction.class, copyFrom.applyToRow, add);
            this.applyToMarker = maybeAppend(MarkerFunction.class, copyFrom.applyToMarker, add);
            this.earlyTermination = maybeAppend(EarlyTermination.class, copyFrom.earlyTermination, add);
            this.runOnClose = maybeAppend(RunOnClose.class, copyFrom.runOnClose, add);
            this.refill = add instanceof Refill
            ? maybeAppend(RefillSnapshot.class, copyFrom.refill, new RefillSnapshot((Refill) add, copyFrom))
            : copyFrom.refill;
        }

        // saves the position of the function stack at the time the Refill method was attached.
        static class RefillSnapshot
        {
            final Refill function;
            int applyToPartition, applyToRow, applyToMarker, earlyTermination, runOnClose, refill;

            RefillSnapshot(Refill function, Stack truncate)
            {
                this.function = function;
                init(truncate);
            }

            void init(Stack truncate)
            {
                applyToPartition = truncate.applyToPartition.length;
                applyToRow = truncate.applyToRow.length;
                applyToMarker = truncate.applyToMarker.length;
                earlyTermination = truncate.earlyTermination.length;
                runOnClose = truncate.runOnClose.length;
                refill = truncate.refill.length;
            }

            void subtract(RefillSnapshot from)
            {
                applyToPartition -= from.applyToPartition;
                applyToRow -= from.applyToRow;
                applyToMarker -= from.applyToMarker;
                earlyTermination -= from.earlyTermination;
                runOnClose -= from.runOnClose;
                refill -= from.refill;
            }
        }
   }

    private static <E> E[] splice(E[] prefix, E[] suffix, int suffixFrom)
    {
        int newLen = prefix.length + suffix.length - suffixFrom;
        E[] result = (E[]) Array.newInstance(prefix.getClass().getComponentType(), newLen);
        System.arraycopy(prefix, 0, result, 0, prefix.length);
        System.arraycopy(suffix, suffixFrom, result, prefix.length, newLen - prefix.length);
        return result;
    }

    private static <E> E[] maybeAppend(Class<E> clazz, E[] array, Object value)
    {
        if (clazz.isInstance(value))
        {
            int oldLen = array.length;
            E[] newArray = (E[]) Array.newInstance(clazz, oldLen + 1);
            System.arraycopy(array, 0, newArray, 0, oldLen);
            newArray[oldLen] = (E) value;
            array = newArray;
        }
        return array;
    }
}