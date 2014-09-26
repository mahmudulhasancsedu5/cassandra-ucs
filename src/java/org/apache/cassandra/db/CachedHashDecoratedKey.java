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
package org.apache.cassandra.db;

import java.nio.ByteBuffer;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.MurmurHash;

public class CachedHashDecoratedKey extends BufferDecoratedKey
{
    long hash0;
    long hash1;
    volatile boolean hashed;

    public CachedHashDecoratedKey(Token<?> token, ByteBuffer key)
    {
        super(token, key);
        hashed = false;
    }

    @Override
    public boolean retrieveCachedFilterHash(long[] dest)
    {
        if (hashed)
        {
            dest[0] = hash0;
            dest[1] = hash1;
        }
        else
        {
            ByteBuffer key = getKey();
            MurmurHash.hash3_x64_128(key, key.position(), key.remaining(), 0, dest);
            hash0 = dest[0];
            hash1 = dest[1];
            hashed = true;
        }
        return true;
    }
}
