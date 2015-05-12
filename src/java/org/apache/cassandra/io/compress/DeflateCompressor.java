/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.io.compress;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class DeflateCompressor implements ICompressor
{
    public static final DeflateCompressor instance = new DeflateCompressor();

    private final ThreadLocal<Deflater> deflater;
    private final ThreadLocal<Inflater> inflater;

    public static DeflateCompressor create(Map<String, String> compressionOptions)
    {
        // no specific options supported so far
        return instance;
    }

    private DeflateCompressor()
    {
        deflater = new ThreadLocal<Deflater>()
        {
            @Override
            protected Deflater initialValue()
            {
                return new Deflater();
            }
        };
        inflater = new ThreadLocal<Inflater>()
        {
            @Override
            protected Inflater initialValue()
            {
                return new Inflater();
            }
        };
    }

    public Set<String> supportedOptions()
    {
        return Collections.emptySet();
    }

    public int initialCompressedBufferLength(int sourceLen)
    {
        // Taken from zlib deflateBound(). See http://www.zlib.net/zlib_tech.html.
        return sourceLen + (sourceLen >> 12) + (sourceLen >> 14) + (sourceLen >> 25) + 13;
    }

    public int compress(ByteBuffer input, int inputOffset, int inputLength, ByteBuffer output, int outputOffset)
    {
        int maxOutputLength = output.capacity() - outputOffset;
        assert maxOutputLength >= initialCompressedBufferLength(inputLength);
        if (input.hasArray() && output.hasArray())
            return compressArray(input.array(), input.arrayOffset() + inputOffset, inputLength,
                                 output.array(), output.arrayOffset() + outputOffset, maxOutputLength);
        else
            return compressBuffer(input, inputOffset, inputLength, output, outputOffset, maxOutputLength);
    }

    public int compressArray(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset, int maxOutputLength)
    {
        Deflater def = deflater.get();
        def.reset();
        def.setInput(input, inputOffset, inputLength);
        def.finish();
        if (def.needsInput())
            return 0;

        int len = def.deflate(output, outputOffset, maxOutputLength);
        assert def.finished();
        return len;
    }

    public int compressBuffer(ByteBuffer input, int inputOffset, int inputLength, ByteBuffer output, int outputOffset, int maxOutputLength)
    {
        output = output.duplicate();
        output.limit(outputOffset + maxOutputLength).position(outputOffset);

        Deflater def = deflater.get();
        def.reset();

        byte[] buffer = FBUtilities.getThreadLocalScratchBuffer();
        // Use half the buffer for input, half for output.
        int chunkLen = buffer.length / 2;
        while (inputLength > chunkLen)
        {
            ByteBufferUtil.arrayCopy(input, inputOffset, buffer, 0, chunkLen);
            inputOffset += chunkLen;
            inputLength -= chunkLen;
            def.setInput(buffer, 0, chunkLen);
            while (!def.needsInput())
            {
                int len = def.deflate(buffer, chunkLen, chunkLen);
                output.put(buffer, chunkLen, len);
            }
        }
        ByteBufferUtil.arrayCopy(input, inputOffset, buffer, 0, inputLength);
        def.setInput(buffer, 0, inputLength);
        def.finish();
        while (!def.finished())
        {
            int len = def.deflate(buffer, chunkLen, chunkLen);
            output.put(buffer, chunkLen, len);
        }
        return output.position() - outputOffset;
    }


    public int uncompress(ByteBuffer input, int inputOffset, int inputLength, ByteBuffer output, int outputOffset) throws IOException
    {
        int maxOutputLength = output.capacity() - outputOffset;
        if (input.hasArray() && output.hasArray())
            return uncompress(input.array(), input.arrayOffset() + inputOffset, inputLength,
                              output.array(), output.arrayOffset() + outputOffset);
        else
            return uncompressBuffer(input, inputOffset, inputLength, output, outputOffset, maxOutputLength);
    }

    public int uncompressBuffer(ByteBuffer input, int inputOffset, int inputLength, ByteBuffer output, int outputOffset, int maxOutputLength) throws IOException
    {
        try
        {
            output = output.duplicate();
            output.limit(outputOffset + maxOutputLength).position(outputOffset);

            Inflater inf = inflater.get();
            inf.reset();

            byte[] buffer = FBUtilities.getThreadLocalScratchBuffer();
            // Use half the buffer for input, half for output.
            int chunkLen = buffer.length / 2;
            while (inputLength > chunkLen)
            {
                ByteBufferUtil.arrayCopy(input, inputOffset, buffer, 0, chunkLen);
                inputOffset += chunkLen;
                inputLength -= chunkLen;
                inf.setInput(buffer, 0, chunkLen);
                while (!inf.needsInput())
                {
                    int len = inf.inflate(buffer, chunkLen, chunkLen);
                    output.put(buffer, chunkLen, len);
                }
            }
            ByteBufferUtil.arrayCopy(input, inputOffset, buffer, 0, inputLength);
            inf.setInput(buffer, 0, inputLength);
            while (!inf.needsInput())
            {
                int len = inf.inflate(buffer, chunkLen, chunkLen);
                output.put(buffer, chunkLen, len);
            }
            return output.position() - outputOffset;
        }
        catch (DataFormatException e)
        {
            throw new IOException(e);
        }
    }

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException
    {
        Inflater inf = inflater.get();
        inf.reset();
        inf.setInput(input, inputOffset, inputLength);
        if (inf.needsInput())
            return 0;

        // We assume output is big enough
        try
        {
            return inf.inflate(output, outputOffset, output.length - outputOffset);
        }
        catch (DataFormatException e)
        {
            throw new IOException(e);
        }
    }

    public int uncompress(ByteBuffer input, ByteBuffer output) throws IOException
    {
        if (!output.hasArray())
            throw new IllegalArgumentException("DeflateCompressor doesn't work with direct byte buffers");

        if (input.hasArray())
            return uncompress(input.array(), input.arrayOffset() + input.position(), input.remaining(), output.array(), output.arrayOffset() + output.position());
        return uncompress(ByteBufferUtil.getArray(input), 0, input.remaining(), output.array(), output.arrayOffset() + output.position());
    }

    public boolean supports(BufferType bufferType)
    {
        return true;
    }

    public BufferType preferredBufferType()
    {
        // Prefer array-backed buffers.
        return BufferType.ON_HEAP;
    }
}
