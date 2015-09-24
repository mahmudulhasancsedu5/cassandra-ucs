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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.partitions.BasePartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.CloseableIterator.Closer;
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

    /** STATIC TRANSFORMATION ACCESSORS **/

    /**
     * Apply a function to this UnfilteredRowIterator
     */
    public static UnfilteredPartitionIterator apply(UnfilteredPartitionIterator iterator, Function function)
    {
        if (EmptyIterators.isEmpty(iterator))
            return iterator;

        return new UnfilteredPartitions(iterator, function);
    }

    /**
     * Apply a function to this RowIterator
     */
    public static PartitionIterator apply(PartitionIterator iterator, Function function)
    {
        assert !(function instanceof BasePartitionFunction) || function instanceof PartitionFunction;

        return new Partitions(iterator, function);
    }

    /**
     * Apply a function to this UnfilteredRowIterator
     */
    public static UnfilteredRowIterator apply(UnfilteredRowIterator iterator, Function function)
    {
        return new UnfilteredRows(iterator, function);
    }

    /**
     * Apply a function to this UnfilteredRowIterator
     */
    public static UnfilteredRowIterator apply(UnfilteredRowIterator iterator, Supplier<Unfiltered> supplier)
    {
        return new UnfilteredRows(iterator, supplier);
    }

    /**
     * Apply a function to this RowIterator
     */
    public static RowIterator apply(RowIterator iterator, Function function)
    {
        return new FilteredRows(iterator, function);
    }

    /** FILTRATION **/

    /**
     * Filter any RangeTombstoneMarker from the iterator's iterators, transforming it into a PartitionIterator.
     */
    public static PartitionIterator filter(UnfilteredPartitionIterator iterator, int nowInSecs)
    {
        Supplier<UnfilteredRowIterator> supplier = iterator.supplier();
        return new Partitions(iterator, () ->
        {
            while (true)
            {
                UnfilteredRowIterator u = supplier.get();
                if (u == null)
                    return null;
                RowIterator p = filter(u, nowInSecs);
                if (iterator.isForThrift() || !p.isEmpty())
                    return p;
                p.close();
            }
        }, iterator.closer());
    }

    /**
     * Filter any RangeTombstoneMarker from the iterator, transforming it into a RowIterator.
     */
    public static RowIterator filter(UnfilteredRowIterator iterator, int nowInSecs)
    {
        Supplier<Unfiltered> supplier = iterator.supplier();
        return new FilteredRows(iterator, () ->
            {
                while (true)
                {
                    Unfiltered u = supplier.get();
                    if (u == null)
                        return null;
                    if (u instanceof Row)
                    {
                        Row r = ((Row) u).purge(DeletionPurger.PURGE_ALL, nowInSecs);
                        if (r != null)
                            return r;
                    }
                }
            });
    }
    
    public static PartitionIterator concat(PartitionIterator head, PartitionIterator tail)
    {
        return new Partitions(head, concat(head.supplier(), tail.supplier()), concat(head.closer(), tail.closer()));
    }

    public static PartitionIterator concat(Iterable<PartitionIterator> iterators)
    {
        Iterator<PartitionIterator> it = iterators.iterator();
        if (!it.hasNext())
            return null;

        return concat(it.next(), it, concatClosers(iterators));
    }

    private static Closer concatClosers(Iterable<? extends CloseableIterator<?>> iterators)
    {
        return () -> Throwables.maybeFail(Throwables.close(null, iterators));
    }

    public static PartitionIterator concat(PartitionIterator head, Iterator<PartitionIterator> tail, Closer closer)
    {
        return new Partitions(head, concat(head.supplier(), Iterators.transform(tail, x -> x.supplier())), closer);
    }

    public static UnfilteredRowIterator concat(UnfilteredRowIterator head, UnfilteredRowIterator tail)
    {
        return new UnfilteredRows(head, concat(head.supplier(), tail.supplier()));
    }

    static <T> Supplier<T> concat(Supplier<T> headSupplier, Supplier<T> tailSupplier)
    {
        return new Supplier<T>() {
            Supplier<T> supplier = headSupplier;
            Supplier<T> continuation = tailSupplier;

            public T get()
            {
                T next = supplier.get();
                if (next != null)
                    return next;

                supplier = continuation;
                continuation = null;
                if (supplier != null)
                    return supplier.get();
                return null;
            }
        };
    }

    static Closer concat(Closer inner, Closer outer)
    {
        return () ->
        {
            try
            {
                outer.close();
            }
            finally
            {
                inner.close();
            }
        };
    }

    static <T> Supplier<T> concat(Supplier<T> head, Iterator<Supplier<T>> tail)
    {
        return new Supplier<T>() {
            Iterator<Supplier<T>> it = tail;
            Supplier<T> supplier = head;

            public T get()
            {
                while (true)
                {
                    T next = supplier.get();
                    if (next != null)
                        return next;
                    if (!it.hasNext())
                        return null;

                    supplier = it.next();
                }
            }
        };
    }

    /* ROWS IMPLEMENTATION */

    private static final class UnfilteredRows extends BaseRows<Unfiltered, UnfilteredRowIterator> implements UnfilteredRowIterator
    {
        private final DeletionTime partitionLevelDeletion;

        public UnfilteredRows(UnfilteredRowIterator input, Function apply)
        {
            super(input, apply, combine(input.supplier(), apply));
            partitionLevelDeletion = applyToPartitionDeletion(input.partitionLevelDeletion(), apply);
        }

        UnfilteredRows(UnfilteredRowIterator input, Supplier<Unfiltered> supplier)
        {
            super(input, supplier, input.closer());
            partitionLevelDeletion = input.partitionLevelDeletion();
        }

        private static Supplier<Unfiltered> combine(Supplier<Unfiltered> supplier, Function apply)
        {
            if (apply instanceof RowFunction)
            {
                RowFunction rf = (RowFunction) apply;
                if (apply instanceof MarkerFunction)
                {
                    MarkerFunction mf = (MarkerFunction) apply;
                    return () -> {
                        for (;;)
                        {
                            Unfiltered next = supplier.get();
                            if (next == null)
                                return next;
                            if (next.kind() == Unfiltered.Kind.ROW)
                                next = rf.applyToRow((Row) next);
                            else
                                next = mf.applyToMarker((RangeTombstoneMarker) next);
                            if (next != null)
                                return next;
                        }
                    };
                }
                else
                {
                    return () -> {
                        for (;;)
                        {
                            Unfiltered next = supplier.get();
                            if (next == null)
                                return next;
                            if (next.kind() == Unfiltered.Kind.ROW)
                                next = rf.applyToRow((Row) next);
                            if (next != null)
                                return next;
                        }
                    };
                }
            }
            else if (apply instanceof MarkerFunction)
            {
                MarkerFunction mf = (MarkerFunction) apply;
                return () -> {
                    for (;;)
                    {
                        Unfiltered next = supplier.get();
                        if (next == null)
                            return next;
                        if (next.kind() != Unfiltered.Kind.ROW)
                            next = mf.applyToMarker((RangeTombstoneMarker) next);
                        if (next != null)
                            return next;
                    }
                };
            }
            
            return supplier;
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
            super(input, apply, combine(input.supplier(), apply));
            assert !(apply instanceof MarkerFunction);
        }

        FilteredRows(BaseRowIterator<?> input, Supplier<Row> supplier)
        {
            super(input, supplier, input.closer());
        }

        public boolean isEmpty()
        {
            return staticRow().isEmpty() && !hasNext();
        }
        
        private static
        Supplier<Row> combine(Supplier<Row> supplier, Function apply)
        {
            if (apply instanceof RowFunction)
                return combine(supplier, (RowFunction) apply);
            else
                return supplier;
        }
        
//        private static filteredSupplier(Supplier<Unfiltered> src, Filter filter)
//        {
//            return () -> {
//                for (;;)
//                {
//                    Row next = supplier.get();
//                    if (next == null)
//                        return next; // includes null
//                    Row transformed = rf.applyToRow(next);
//                    if (transformed != null)
//                        return transformed;
//                }
//            };
//        }

        private static
        Supplier<Row> combine(Supplier<Row> supplier, RowFunction rf)
        {
            return () -> {
                for (;;)
                {
                    Row next = supplier.get();
                    if (next == null)
                        return next; // includes null
                    Row transformed = rf.applyToRow(next);
                    if (transformed != null)
                        return transformed;
                }
            };
        }

    }

    private static abstract class BaseRows<R extends Unfiltered, I extends BaseRowIterator<? extends Unfiltered>> extends Abstract<Unfiltered, I, R> implements BaseRowIterator<R>
    {
        private Row staticRow;

        public BaseRows(I input, Supplier<R> supplier, Closer closer)
        {
            super(input, supplier, closer);
            staticRow = input.staticRow();
        }
        
        public BaseRows(I input, Function apply, Supplier<R> supplier)
        {
            super(input, apply,
                  apply instanceof EarlyTermination ?
                          combineEarlyTermination(supplier, (EarlyTermination) apply) :
                          supplier);
            staticRow = applyToStatic(input.staticRow(), apply);
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

        private static Row applyToStatic(Row row, Function apply)
        {
            if (apply instanceof StaticRowFunction)
                row = ((StaticRowFunction) apply).applyToStatic(row);
            return row;
        }
    }

    /* PARTITIONS IMPLEMENTATION */

    private static final class UnfilteredPartitions extends BasePartitions<UnfilteredRowIterator, UnfilteredPartitionIterator> implements UnfilteredPartitionIterator
    {
        public UnfilteredPartitions(UnfilteredPartitionIterator input, Function apply)
        {
            super(input, apply, input.isForThrift());
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

        Partitions(BasePartitionIterator<?> input, Supplier<RowIterator> supplier, Closer closer)
        {
            super(input, supplier, closer);
        }
    }

    private static abstract class BasePartitions<R extends BaseRowIterator<?>,
                                                I extends BasePartitionIterator<? extends BaseRowIterator<?>>>
    extends Abstract<BaseRowIterator<?>, I, R>
    implements BasePartitionIterator<R>
    {
        public BasePartitions(I input, Function apply, boolean isForThrift)
        {
            super(input, apply, combine(input.supplier(), apply, isForThrift));
        }
        
        public BasePartitions(I input, Supplier<R> supplier)
        {
            super(input, supplier, input.closer());
        }

        public BasePartitions(I input, Supplier<R> supplier, Closer closer)
        {
            super(input, supplier, closer);
        }


        // *********** /Begin Core Debug Point


        private static <O extends BaseRowIterator<?>, I extends BaseRowIterator<? extends Unfiltered>>
        Supplier<O> combinePartitionFunction(Supplier<I> supplier, BasePartitionFunction<I, O> apply, boolean isForThrift)
        {
            return () -> {
                for (;;)
                {
                    I next = supplier.get();
                    if (next == null)
                        return null;
                    O transformed = apply.applyToPartition(next);
                    if (transformed != null && (isForThrift || !transformed.isEmpty()))
                        return transformed;
                    next.close();
                }
            };
        }
        

        private static <O extends BaseRowIterator<?>, I extends BaseRowIterator<? extends Unfiltered>>
        Supplier<O> combine(Supplier<I> src, Function apply, boolean isForThrift)
        {
            Supplier<O> supplier;
            if (apply instanceof BasePartitionFunction)
                supplier = combinePartitionFunction(src, (BasePartitionFunction<I, O>) apply, isForThrift);
            else
                supplier = (Supplier<O>) src;
            if (apply instanceof EarlyTermination)
                supplier = combineEarlyTermination(supplier, (EarlyTermination) apply);
            // TODO: single-step if both
            return supplier;
        }

//        protected BaseRowIterator<?> applyTo(BaseRowIterator<?> next)
//        {
//            try
//            {
//                BasePartitionFunction[] fs = applyToPartition;
//                for (int i = 0 ; next != null & i < fs.length ; i++)
//                    next = fs[i].applyToPartition(next);
//
//                if (next != null && (isForThrift || !next.isEmpty()))
//                    return next;
//            }
//            catch (Throwable t)
//            {
//                Throwables.close(t, Collections.singleton(next));
//                throw t;
//            }
//
//            if (next != null)
//                next.close();
//            return null;
//        }



        // *********** /End Core Debug Point
    }

    /** SHARED IMPLEMENTATION **/

    private abstract static class Abstract<V, I extends CloseableIterator<? extends V>, O extends V> implements CloseableIterator<O>
    {
        I input;
        V next = null;
        Supplier<O> supplier;
        Supplier<O> iterationSource;
        Closer closer;

        // responsibility for initialising next lies with the subclass
        private Abstract(I input, Supplier<O> supplier, Closer closer)
        {
            this.input = input instanceof Abstract ? ((Abstract<V, I, O>) input).input : input;
            this.supplier = supplier;
            this.closer = closer;
        }
        
        static Closer combine(Closer inner, RunOnClose outer)
        {
            return () ->
            {
                try
                {
                    outer.runOnClose();
                }
                finally
                {
                    inner.close();
                }
            };
        }
        
        static <T> Supplier<T> combineEarlyTermination(Supplier<T> src, EarlyTermination apply)
        {
            return () -> apply.terminate() ? null : src.get();
        }

        static Closer combine(Closer first, Function apply)
        {
            return apply instanceof RunOnClose ?
                combine(first, (RunOnClose) apply) :
                first;
        }

        private Abstract(I input, Function apply, Supplier<O> supplier)
        {
            this(input, supplier, combine(input.closer(), apply));
        }
        
        public Closer closer()
        {
            return closer;
        }

        public void close()
        {
            closer().close();
        }

        public Supplier<O> supplier()
        {
            Supplier<O> toReturn = supplier;
            supplier = null;
            if (toReturn == null && iterationSource != null)
                return () -> hasNext() ? Preconditions.checkNotNull(next()) : null;
            return toReturn;
        }

        public final boolean hasNext()
        {
            if (iterationSource == null)
            {
                iterationSource = supplier;
                supplier = null;
            }
            if (next == null)
                next = iterationSource.get();
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
    }
}