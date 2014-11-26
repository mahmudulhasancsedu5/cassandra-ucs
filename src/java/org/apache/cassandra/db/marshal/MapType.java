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
import java.util.*;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import org.apache.cassandra.db.Cell;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.serializers.MapSerializer;
import org.apache.cassandra.transport.Server;

public class MapType<K, V> extends CollectionType<Map<K, V>>
{
    // interning instances
    private static final Table<ConcreteType<?>, ConcreteType<?>, MapType<?, ?>> instances = HashBasedTable.create();
    private static final Table<ConcreteType<?>, ConcreteType<?>, MapType<?, ?>> frozenInstances = HashBasedTable.create();

    private final ConcreteType<K> keys;
    private final ConcreteType<V> values;
    private final MapSerializer<K, V> serializer;
    private final boolean isMultiCell;

    public static MapType<?, ?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType> l = parser.getTypeParameters();
        if (l.size() != 2)
            throw new ConfigurationException("MapType<?, ?> takes exactly 2 type parameters");

        return getInstance(l.get(0), l.get(1), true);
    }

    public static MapType<?, ?> getInstance(AbstractType keys, AbstractType values, boolean isMultiCell)
    {
        return getInstance((ConcreteType<?>) keys, (ConcreteType<?>) values, isMultiCell);
    }

    public static synchronized <K, V> MapType<K, V> getInstance(ConcreteType<K> keys, ConcreteType<V> values, boolean isMultiCell)
    {
        Table<ConcreteType<?>, ConcreteType<?>, MapType<?, ?>> internMap = isMultiCell ? instances : frozenInstances;
        @SuppressWarnings("unchecked")
        MapType<K, V> t = (MapType<K, V>) internMap.get(keys, values);
        if (t == null)
        {
            t = new MapType<>(keys, values, isMultiCell);
            internMap.put(keys, values, t);
        }
        return t;
    }

    private MapType(ConcreteType<K> keys, ConcreteType<V> values, boolean isMultiCell)
    {
        super(Kind.MAP);
        this.keys = keys;
        this.values = values;
        this.serializer = MapSerializer.getInstance(keys.getSerializer(), values.getSerializer());
        this.isMultiCell = isMultiCell;
    }

    public AbstractType getKeysType()
    {
        return keys;
    }

    public AbstractType getValuesType()
    {
        return values;
    }

    public AbstractType nameComparator()
    {
        return keys;
    }

    public AbstractType valueComparator()
    {
        return values;
    }

    @Override
    public boolean isMultiCell()
    {
        return isMultiCell;
    }

    @Override
    public AbstractType freeze()
    {
        if (isMultiCell)
            return getInstance(this.keys, this.values, false);
        else
            return this;
    }

    @Override
    public boolean isCompatibleWithFrozen(CollectionType<?> previous)
    {
        assert !isMultiCell;
        MapType<?, ?> tprev = (MapType<?, ?>) previous;
        return keys.isCompatibleWith(tprev.keys) && values.isCompatibleWith(tprev.values);
    }

    @Override
    public boolean isValueCompatibleWithFrozen(CollectionType<?> previous)
    {
        assert !isMultiCell;
        MapType<?, ?> tprev = (MapType<?, ?>) previous;
        return keys.isCompatibleWith(tprev.keys) && values.isValueCompatibleWith(tprev.values);
    }

    @Override
    public int compare(ByteBuffer o1, ByteBuffer o2)
    {
        return compareMaps(keys, values, o1, o2);
    }

    public static int compareMaps(AbstractType keysComparator, AbstractType valuesComparator, ByteBuffer o1, ByteBuffer o2)
    {
         if (!o1.hasRemaining() || !o2.hasRemaining())
            return o1.hasRemaining() ? 1 : o2.hasRemaining() ? -1 : 0;

        ByteBuffer bb1 = o1.duplicate();
        ByteBuffer bb2 = o2.duplicate();

        int protocolVersion = Server.VERSION_3;
        int size1 = CollectionSerializer.readCollectionSize(bb1, protocolVersion);
        int size2 = CollectionSerializer.readCollectionSize(bb2, protocolVersion);

        for (int i = 0; i < Math.min(size1, size2); i++)
        {
            ByteBuffer k1 = CollectionSerializer.readValue(bb1, protocolVersion);
            ByteBuffer k2 = CollectionSerializer.readValue(bb2, protocolVersion);
            int cmp = keysComparator.compare(k1, k2);
            if (cmp != 0)
                return cmp;

            ByteBuffer v1 = CollectionSerializer.readValue(bb1, protocolVersion);
            ByteBuffer v2 = CollectionSerializer.readValue(bb2, protocolVersion);
            cmp = valuesComparator.compare(v1, v2);
            if (cmp != 0)
                return cmp;
        }

        return size1 == size2 ? 0 : (size1 < size2 ? -1 : 1);
    }

    @Override
    public MapSerializer<K, V> getSerializer()
    {
        return serializer;
    }

    public boolean isByteOrderComparable()
    {
        return keys.isByteOrderComparable();
    }

    @Override
    public String toString(boolean ignoreFreezing)
    {
        boolean includeFrozenType = !ignoreFreezing && !isMultiCell();

        StringBuilder sb = new StringBuilder();
        if (includeFrozenType)
            sb.append(FrozenType.class.getName()).append("(");
        sb.append(getClass().getName()).append(TypeParser.stringifyTypeParameters(Arrays.<AbstractType>asList(keys, values), ignoreFreezing || !isMultiCell));
        if (includeFrozenType)
            sb.append(")");
        return sb.toString();
    }

    public List<ByteBuffer> serializedValues(List<Cell> cells)
    {
        assert isMultiCell;
        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>(cells.size() * 2);
        for (Cell c : cells)
        {
            bbs.add(c.name().collectionElement());
            bbs.add(c.value());
        }
        return bbs;
    }

    @Override
    public Map<K, V> cast(Object value)
    {
        return (Map<K, V>) (Map<?, ?>) value;
    }
}
