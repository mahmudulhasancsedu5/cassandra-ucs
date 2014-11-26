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
package org.apache.cassandra.db.marshal;

import java.nio.ByteBuffer;

import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.github.jamm.Unmetered;

/**
 * Specifies a Comparator for a specific type of ByteBuffer.
 *
 * Note that empty ByteBuffer are used to represent "start at the beginning"
 * or "stop at the end" arguments to get_slice, so the Comparator
 * should always handle those values even if they normally do not
 * represent a valid ByteBuffer for the type being compared.
 */
@Unmetered
public abstract class ConcreteType<T> extends AbstractType
{
    public abstract TypeSerializer<T> getSerializer();
    public abstract T cast(Object value);

    public T compose(ByteBuffer bytes)
    {
        return getSerializer().deserialize(bytes);
    }

    public ByteBuffer decomposeTyped(T value)
    {
        return getSerializer().serialize(value);
    }

    public ByteBuffer decompose(Object value)
    {
        return getSerializer().serialize(cast(value));
    }

    /** get a string representation of the bytes suitable for log messages */
    public String getString(ByteBuffer bytes)
    {
        TypeSerializer<T> serializer = getSerializer();
        serializer.validate(bytes);

        return serializer.toString(serializer.deserialize(bytes));
    }

    /** get a byte representation of the given string. */
    public abstract ByteBuffer fromString(String source) throws MarshalException;

    /* validate that the byte array is a valid sequence for the type we are supposed to be comparing */
    public void validate(ByteBuffer bytes) throws MarshalException
    {
        getSerializer().validate(bytes);
    }
}
