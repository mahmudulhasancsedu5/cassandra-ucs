package org.apache.cassandra.utils;

public class Pool<Type>
{

    final private Allocator<Type> allocator;

    final private Object[] pool;

    private int position;

    public interface Allocator<Type>
    {
        Type allocate();
    }

    public Pool(Allocator<Type> allocator, int maxSize)
    {
        this.allocator = allocator;
        pool = new Object[maxSize];
        position = 0;
    }

    // TODO: Test and possibly replace this with a lock-free stack implementation.
    @SuppressWarnings("unchecked")
    public synchronized Type get()
    {
        if (position == 0)
        {
            return allocator.allocate();
        }
        else
        {
            Type v = (Type) pool[--position];
            // Clear value to avoid holding on to a potentially lost reference.
            pool[position] = null;
            return v;
        }
    }

    public synchronized void put(Type v)
    {
        if (position < pool.length)
        {
            pool[position++] = v;
        }
    }
}
