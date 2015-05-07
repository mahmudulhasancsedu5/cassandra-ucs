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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Set;

public interface ICompressor
{
    public int initialCompressedBufferLength(int chunkLength);

    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException;

    /**
     * Compression for ByteBuffers.
     * The compressor must not modify any parameter (position/limit/etc) of the buffers as they may be in other use concurrently.
     */
    public int compress(ByteBuffer input, int inputOffset, int inputLength, ByteBuffer output, int outputOffset) throws IOException;

    /**
     * Decompression for DirectByteBuffers.
     * The compressor must not modify any parameter (position/limit/etc) of the buffers as they may be in other use concurrently.
     */
    public int uncompress(ByteBuffer input, int inputOffset, int inputLength, ByteBuffer output, int outputOffset) throws IOException;

    /**
     * Returns the preferred (most efficient) buffer type for this compressor.
     */
    public BufferType preferredBufferType();

    /**
     * Checks if the given buffer would be supported by the compressor. If a type is supported the compressor must be
     * able to use it in combination with all other supported types.
     */
    public boolean supports(ByteBuffer buffer);

    /**
     * Returns whether the compressor would be able to work with mapped buffers.
     */
    public boolean supportsMemoryMappedBuffers();

    public Set<String> supportedOptions();
}
