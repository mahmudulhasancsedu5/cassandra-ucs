package org.apache.cassandra.utils;

import java.nio.ByteBuffer;

public enum ByteBuffers
{

    OFF_HEAP
    {
        ByteBuffer allocateBuffer(int size)
        {
            return ByteBuffer.allocateDirect(size);
        }

        boolean shouldPool(int size)
        {
            return size > 0 && size <= MAX_POOLED_SIZE;
        }
    },

    ON_HEAP
    {
        ByteBuffer allocateBuffer(int size)
        {
            return ByteBuffer.allocate(size);
        }

        boolean shouldPool(int size)
        {
            // It is more efficient to not pool objects that are allocated in young generation memory.
            // Note: The code below assumes shouldPool(n) == shouldPool(2^log2ceil(n)) for all n.
            return size > 65536 && size <= MAX_POOLED_SIZE;
        }
    };

    abstract ByteBuffer allocateBuffer(int size);
    abstract boolean shouldPool(int size);

    static final int MAX_POOLED_SIZE_LOG = 26;
    static final int MAX_POOLED_SIZE = 1 << MAX_POOLED_SIZE_LOG;
    static final int MAX_POOL_SIZE = 128;
    static final int MAX_POOLED_MEM_LOG = 30;

    Pool<ByteBuffer> pools[];

    @SuppressWarnings("unchecked")
    private ByteBuffers()
    {
        pools = new Pool[MAX_POOLED_SIZE_LOG];
        for (int i = 0; i < MAX_POOLED_SIZE_LOG; ++i)
        {
            int size = 1 << i;
            if (shouldPool(size))
            {
                pools[i] = new Pool<>(makeAllocator(size), Math.min(MAX_POOL_SIZE, 1 << (MAX_POOLED_MEM_LOG - i)));
            }
        }
    }

    private Pool.Allocator<ByteBuffer> makeAllocator(final int size)
    {
        return new Pool.Allocator<ByteBuffer>()
        {
            public ByteBuffer allocate()
            {
                return allocateBuffer(size);
            }
        };
    }

    private static int log2ceil(int size)
    {
        return 32 - Integer.numberOfLeadingZeros(size - 1);
    }

    public ByteBuffer allocate(int size)
    {
        if (!shouldPool(size))
        {
            return allocateBuffer(size);
        }
        int sizeLog = log2ceil(size);
        ByteBuffer buf = pools[sizeLog].get();
        assert buf.capacity() >= size;
        assert shouldPool(buf.capacity());
        return buf;
    }

    public void release(ByteBuffer buf)
    {
        int size = buf.capacity();
        if (!shouldPool(size))
        {
            return;
        }
        // Buffer must be allocated through us.
        assert Integer.bitCount(size) == 1;

        int sizeLog = log2ceil(size);
        buf.clear();
        pools[sizeLog].put(buf);
    }

}
