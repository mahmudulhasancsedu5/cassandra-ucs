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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;
import org.github.jamm.Unmetered;

/**
 * Base implementation for all AbstractType instances.
 */
@Unmetered
public abstract class ConcreteType<T> implements AbstractType
{
    public final Comparator<ByteBuffer> reverseComparator;

    protected ConcreteType()
    {
        reverseComparator = new Comparator<ByteBuffer>()
        {
            public int compare(ByteBuffer o1, ByteBuffer o2)
            {
                if (o1.remaining() == 0)
                {
                    return o2.remaining() == 0 ? 0 : -1;
                }
                if (o2.remaining() == 0)
                {
                    return 1;
                }

                return ConcreteType.this.compare(o2, o1);
            }
        };
    }

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

    public String getString(ByteBuffer bytes)
    {
        TypeSerializer<T> serializer = getSerializer();
        serializer.validate(bytes);

        return serializer.toString(serializer.deserialize(bytes));
    }

    public void validate(ByteBuffer bytes) throws MarshalException
    {
        getSerializer().validate(bytes);
    }


    public void validateCellValue(ByteBuffer cellValue) throws MarshalException
    {
        validate(cellValue);
    }

    public CQL3Type asCQL3Type()
    {
        return new CQL3Type.Custom(this);
    }

    /* convenience method */
    public String getString(Collection<ByteBuffer> names)
    {
        StringBuilder builder = new StringBuilder();
        for (ByteBuffer name : names)
        {
            builder.append(getString(name)).append(",");
        }
        return builder.toString();
    }

    public boolean isCounter()
    {
        return false;
    }

    public boolean isCompatibleWith(AbstractType previous)
    {
        return this.equals(previous);
    }

    public boolean isValueCompatibleWith(AbstractType otherType)
    {
        return isValueCompatibleWithInternal((otherType instanceof ReversedType) ? ((ReversedType<?>) otherType).baseType : otherType);
    }

    /**
     * Needed to handle ReversedType in value-compatibility checks.  Subclasses should implement this instead of
     * isValueCompatibleWith().
     */
    protected boolean isValueCompatibleWithInternal(AbstractType otherType)
    {
        return isCompatibleWith(otherType);
    }

    public boolean isByteOrderComparable()
    {
        return false;
    }

    public int compareCollectionMembers(ByteBuffer v1, ByteBuffer v2, ByteBuffer collectionName)
    {
        return compare(v1, v2);
    }

    public void validateCollectionMember(ByteBuffer bytes, ByteBuffer collectionName) throws MarshalException
    {
        validate(bytes);
    }

    public boolean isCollection()
    {
        return false;
    }

    public boolean isMultiCell()
    {
        return false;
    }

    public AbstractType freeze()
    {
        return this;
    }

    public String toString(boolean ignoreFreezing)
    {
        return this.toString();
    }

    public int componentsCount()
    {
        return 1;
    }

    public List<AbstractType> getComponents()
    {
        return Collections.<AbstractType>singletonList(this);
    }

    public Comparator<ByteBuffer> reverseComparator()
    {
        return reverseComparator;
    }

    public AbstractType getNonReversedType()
    {
        return this;
    }

    public String toString()
    {
        return getClass().getName();
    }
}
