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

package org.apache.cassandra.db.tries;

import org.apache.cassandra.io.compress.BufferType;

public class LimitedSizeMemtableTrie<T> extends MemtableTrie<T>
{
    final int sizeLimit;
    boolean markedForRemoval;
    int index = -1;

    public LimitedSizeMemtableTrie(BufferType bufferType, int sizeLimit)
    {
        super(bufferType);
        this.sizeLimit = sizeLimit;
        this.markedForRemoval = false;
    }

    public LimitedSizeMemtableTrie(BufferType bufferType, int sizeLimit, int initialSize, int initialValuesCount)
    {
        super(bufferType, initialSize, initialValuesCount);
        this.sizeLimit = sizeLimit;
        this.markedForRemoval = false;
    }

    @Override
    protected int calcNewSize(int oldSize) throws SpaceExhaustedException
    {
        if (oldSize >= sizeLimit)
            throw new SpaceExhaustedException();
        return Math.min(sizeLimit, oldSize * 2);
    }

    public synchronized boolean markForRemoval()
    {
        if (markedForRemoval)
            return false;
        markedForRemoval = true;
        return true;
    }
}
