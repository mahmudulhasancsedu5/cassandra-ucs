package org.apache.cassandra.io.compress;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ByteBuffers;

/**
 * Base class for compressors that cannot work on byte buffers directly. Implements machinery to handle extracting
 * arrays from on-heap buffers and copying data for off-heap ones.
 */
public abstract class ByteArrayCompressor
{

    public abstract int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset)
            throws IOException;

    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        byte[] inputArray;
        int inputPosition;
        int inputLength = input.remaining();
        if (input.hasArray())
        {
            inputArray = input.array();
            inputPosition = input.arrayOffset() + input.position();
        }
        else
        {
            inputArray = ByteBufferUtil.getArray(input);
            inputPosition = 0;
        }

        output.clear();
        byte[] outputArray;
        int outputPosition;
        if (output.hasArray())
        {
            outputArray = output.array();
            outputPosition = output.arrayOffset() + output.position();
        }
        else
        {
            outputArray = new byte[output.remaining()];
            outputPosition = 0;
        }

        int outputLength = uncompress(inputArray, inputPosition, inputLength, outputArray, outputPosition);

        if (output.hasArray())
        {
            output.limit(outputLength);
        }
        else
        {
            output.put(outputArray, 0, outputLength);
            output.flip();
        }
    }

    public ByteBuffers preferredByteBufferPool()
    {
        return ByteBuffers.ON_HEAP;
    }

}