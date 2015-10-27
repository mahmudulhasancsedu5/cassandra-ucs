package org.apache.cassandra.db.transform;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.cassandra.utils.CloseableIterator;

import static org.apache.cassandra.utils.Throwables.maybeFail;
import static org.apache.cassandra.utils.Throwables.merge;

abstract class BaseIterator<IN, ITER extends CloseableIterator<? extends IN>, OUT extends IN> implements AutoCloseable, Iterator<OUT>, Consumer.Provider<IN, OUT>
{
    ITER input;
    BaseIterator<IN, ITER, IN> prev;
    final Stop stop; // applies at the end of the current next()
    final Transformation transformation;
    OUT next;
    Consumer<IN> consumer;

    static class Stop
    {
        // TODO: consider moving "next" into here, so that a stop() when signalled outside of a function call (e.g. in attach)
        // can take effect immediately; this doesn't seem to be necessary at the moment, but it might cause least surprise in future
        boolean isSignalled;
    }

    BaseIterator(ITER input, Transformation transformation)
    {
        if (input instanceof BaseIterator)
        {
            @SuppressWarnings("unchecked")
            BaseIterator<IN, ITER, IN> prev = ((BaseIterator<IN, ITER, IN>) input);
            this.input = prev.input;
            this.stop = prev.stop;
            this.prev = prev;
        }
        else
        {
            this.input = input;
            this.stop = new Stop();
            this.prev = null;
        }
        this.transformation = transformation;
    }

    /**
     * run the corresponding runOnClose method for the first length transformations.
     *
     * used in hasMoreContents to close the methods preceding the MoreContents
     */
    protected abstract Throwable runOnClose();

    protected abstract Consumer<IN> apply(Consumer<OUT> nextConsumer);

    public final void close()
    {
        Throwable fail = runOnClose();
        if (next instanceof AutoCloseable)
        {
            try { ((AutoCloseable) next).close(); }
            catch (Throwable t) { fail = merge(fail, t); }
        }
        try { input.close(); }
        catch (Throwable t) { fail = merge(fail, t); }
        maybeFail(fail);
    }

    // this could call the consumer. only once!
    public Consumer<IN> consumer(Consumer<OUT> nextConsumer)
    {
        if (next != null)
            if (!nextConsumer.accept(clearNext()))
                stop.isSignalled = true;

        Consumer<IN> ourConsumer = apply(nextConsumer);

        return prev != null ? prev.consumer(ourConsumer) : ourConsumer;
    }

    private Consumer<IN> initConsumer()
    {
        if (consumer == null)
            consumer = consumer(this::acceptNext);
        return consumer;
    }

    private boolean acceptNext(OUT next)
    {
        assert this.next == null;
        this.next = next;
        return true;
    }

    private OUT clearNext()
    {
        OUT toReturn = next;
        next = null;
        return toReturn;
    }

    public final boolean hasNext()
    {
        initConsumer();
        while (next == null && !stop.isSignalled)
        {
            if (input.hasNext())
            {
                if (!consumer.accept(input.next()))
                    break;
            }
            else
            {
                if (!tryGetMoreContents())
                    break;
            }
        }

        return next != null;
    }

    public final OUT next()
    {
        if (next == null && !hasNext())
            throw new NoSuchElementException();

        return clearNext();
    }

    public final ITER getMoreContents()
    {
        // We don't switch input as we are not going to use it.
        ITER iter = prev != null ? prev.getMoreContents() : null;
        if (iter != null)
            return iter; // Switch already done.

        if (transformation instanceof MoreContents)
            iter = ((MoreContents<ITER, ?>) transformation).moreContents();
        if (iter == null)
            return null;

        prev.close();

        // Switch consumer chains.
        if (iter instanceof BaseIterator)
        {
            @SuppressWarnings("unchecked")
            BaseIterator<IN, ITER, IN> base = (BaseIterator<IN, ITER, IN>) iter;
            prev = base;
            return base.input;
        }
        else
        {
            prev = null;
            return iter;
        }
    }

    public boolean tryGetMoreContents()
    {
        ITER newInput = getMoreContents();
        if (newInput == null)
            return false;
        input = newInput;
        consumer = null;
        initConsumer();
        return true;
    }
}

