package org.apache.cassandra.db.transform;

public interface Consumer<T>
{
    boolean accept(T element);

    interface Provider<IN, OUT>
    {
        Consumer<IN> consumer(Consumer<OUT> next);
    }
}
