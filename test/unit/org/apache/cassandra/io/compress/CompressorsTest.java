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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;

import org.apache.cassandra.io.compress.ICompressor.WrappedArray;
import org.apache.cassandra.utils.ByteBuffers;
import org.junit.Test;

public class CompressorsTest
{
    
    @Test
    public void testLZ4() throws UnsupportedEncodingException, IOException {
        new CompressorTest(LZ4Compressor.create(Collections.<String, String>emptyMap())).run();
    }
    
    @Test
    public void testSnappy() throws UnsupportedEncodingException, IOException {
        new CompressorTest(SnappyCompressor.create(Collections.<String, String>emptyMap())).run();
    }
    
    @Test
    public void testDeflate() throws UnsupportedEncodingException, IOException {
        new CompressorTest(DeflateCompressor.create(Collections.<String, String>emptyMap())).run();
    }
    
    static class CompressorTest {
            
        ICompressor compressor;
        
        public CompressorTest(ICompressor compressor) {
            this.compressor = compressor;
        }
    
        public void test(byte[] data, int off, int len) throws IOException
        {
            final int outOffset = 3;
            final WrappedArray out = new WrappedArray(new byte[outOffset + compressor.initialCompressedBufferLength(len)]);
            new Random().nextBytes(out.buffer);
            final int compressedLength = compressor.compress(data, off, len, out, outOffset);
            final int restoredOffset = 5;
            
            // Testing array decompress.
            final byte[] restored = new byte[restoredOffset + len];
            new Random().nextBytes(restored);
            final int decompressedLength = compressor.uncompress(out.buffer, outOffset, compressedLength, restored, restoredOffset);
            assertEquals(decompressedLength, len);
            assertArrayEquals(Arrays.copyOfRange(data, off, off + len),
                              Arrays.copyOfRange(restored, restoredOffset, restoredOffset + decompressedLength));
            
            // Test byte buffer uncompress for both buffer types.
            for (ByteBuffers pool: ByteBuffers.values()) {
                ByteBuffer input = pool.allocate(out.buffer.length);
                input.put(out.buffer);
                input.flip().position(outOffset).limit(outOffset + compressedLength);
                ByteBuffer output = pool.allocate(len);
                byte[] restoredBytes = new byte[len];
                new Random().nextBytes(restoredBytes);
                output.put(restoredBytes);
                compressor.uncompress(input, output);
                assertEquals(len, output.remaining());
                output.get(restoredBytes);
                assertArrayEquals(Arrays.copyOfRange(data, off, off + len), restoredBytes);
                pool.release(input);
                pool.release(output);
            }
        }
    
        public void test(byte[] data) throws IOException
        {
            test(data, 0, data.length);
        }
    
        public void testEmptyArray() throws IOException
        {
            test(new byte[0]);
        }
    
        public void testShortArray() throws UnsupportedEncodingException, IOException
        {
            test("Cassandra".getBytes("UTF-8"), 1, 7);
        }
    
        public void testLongArray() throws UnsupportedEncodingException, IOException
        {
            byte[] data = new byte[1 << 20];
            test(data, 13, 1 << 19);
            new Random(0).nextBytes(data);
            test(data, 13, 1 << 19);
        }
        
        public void run() throws UnsupportedEncodingException, IOException {
            testEmptyArray();
            testShortArray();
            testLongArray();
        }
    }
}
