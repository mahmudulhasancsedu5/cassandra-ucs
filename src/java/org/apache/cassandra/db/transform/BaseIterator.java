package org.apache.cassandra.db.transform;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.cassandra.utils.CloseableIterator;

import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;

abstract class BaseIterator<IN, ITER extends CloseableIterator<? extends IN>, OUT extends IN> implements AutoCloseable, Iterator<OUT>, Consumer.Provider<IN, OUT>
{
    /** Input non-transformation iterator. */
    ITER input;

    /** Previous transformation in the chain. */
    BaseIterator<IN, ITER, IN> prevTransformation;

    /** Transformation to apply. */
    final Transformation transformation;

    /**
     * Current next value, produced after calling hasNext().
     * Cleared by next() or by calling hasNext() on a transformation applied on top of this (which then takes responsibility of the value).
     */
    private OUT nextValue;

    /** Consumer applied to each input value. Transformation takes place in the consumer, which ultimately sets the value of next. */
    private Consumer<IN> consumer;

    /** Set to true when a signal to terminate iteration has been received. */
    private boolean terminated = false;

    BaseIterator(ITER input, Transformation transformation)
    {
        if (input instanceof BaseIterator)
        {
            @SuppressWarnings("unchecked")
            BaseIterator<IN, ITER, IN> prev = ((BaseIterator<IN, ITER, IN>) input);
            this.input = prev.input;
            this.prevTransformation = prev;
            // Disable previous iterator as this one takes ownership of the input.
            prev.input = null;
            // Release its consumer chain, it won't be used any more.
            prev.consumer = null;
            // prev may hold a value in nextValue. Leave it alone for now, it will be consumed when our hasNext() is called.
        }
        else
        {
            this.input = input;
            this.prevTransformation = null;
        }
        this.transformation = transformation;
    }

    /**
     * run the corresponding runOnClose method for the first length transformations.
     *
     * used in hasMoreContents to close the methods preceding the MoreContents
     */
    protected abstract Throwable runOnClose();

    /**
     * Construct a consumer corresponding to this step of the transformation.
     */
    protected abstract Consumer<IN> apply(Consumer<OUT> nextConsumer);

    public final void close()
    {
        Throwable fail = runOnClose();
        if (nextValue instanceof AutoCloseable)
        {
            try { ((AutoCloseable) nextValue).close(); }
            catch (Throwable t) { fail = merge(fail, t); }
        }
        if (input != null)
            try { input.close(); }
            catch (Throwable t) { fail = merge(fail, t); }
        maybeFail(fail);
    }

    /**
     * Construct a consumer for the chain of transformations.
     *
     * If a value is held in nextValue in one of the transformations in the chain, apply the consumer for the rest of
     * the transformations to it which pulls it to our nextValue. This can happen only once as previous transformed
     * iterators cannot obtain new values after they have been attached to the chain.
     *
     * Returns null if iteration has already been terminated.
     */
    public Consumer<IN> consumer(Consumer<OUT> nextConsumer)
    {
        if (nextValue != null)
            if (!nextConsumer.accept(clearNext()))
                return null;
        if (terminated)
            return null;

        Consumer<IN> ourConsumer = apply(nextConsumer);

        return prevTransformation != null ? prevTransformation.consumer(ourConsumer) : ourConsumer;
    }

    private void initConsumer()
    {
        if (consumer == null)
        {
            consumer = consumer(this::acceptNext);
            if (consumer == null)
                terminated = true;
        }
    }

    private boolean acceptNext(OUT next)
    {
        assert this.nextValue == null;
        this.nextValue = next;
        return true;
    }

    private OUT clearNext()
    {
        OUT toReturn = nextValue;
        nextValue = null;
        return toReturn;
    }

    public final boolean hasNext()
    {
        initConsumer();
        while (nextValue == null && !terminated)
        {
            if (input.hasNext())
            {
                if (!consumer.accept(input.next()))
                {
                    terminated = true;
                    break;
                }
            }
            else
            {
                if (!tryGetMoreContents())
                    break;
            }
        }

        return nextValue != null;
    }

    public final OUT next()
    {
        if (nextValue == null && !hasNext())
            throw new NoSuchElementException();

        return clearNext();
    }

    public final ITER getMoreContents()
    {
        // Child transformations don't switch input as they are not going to use it.
        ITER iter = prevTransformation != null ? prevTransformation.getMoreContents() : null;
        if (iter != null)
            return iter; // Switch already done.

        if (transformation instanceof MoreContents)
            iter = ((MoreContents<ITER, ?, ?>) transformation).moreContents();
        if (iter == null)
            return null;

        if (prevTransformation != null)
            maybeFail(prevTransformation.runOnClose());

        // Switch consumer chains.
        if (iter instanceof BaseIterator)
        {
            @SuppressWarnings("unchecked")
            BaseIterator<IN, ITER, IN> base = (BaseIterator<IN, ITER, IN>) iter;
            prevTransformation = base;
            iter = base.input;
            base.input = null;
            base.consumer = null;
        }
        else
            prevTransformation = null;
        return iter;
    }

    public boolean tryGetMoreContents()
    {
        ITER newInput = getMoreContents();
        if (newInput == null)
            return false;

        // Switch input as well as consumer chain which may have a new set of transformations to apply.
        input.close();
        input = newInput;
        consumer = null;
        initConsumer();
        return true;
    }
}

