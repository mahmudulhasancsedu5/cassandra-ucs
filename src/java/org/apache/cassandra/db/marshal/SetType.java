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

import org.apache.cassandra.cql3.Json;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.SetSerializer;

public class SetType<T> extends ConcreteCollectionType<Set<T>>
{
    // interning instances
    private static final Map<ConcreteType<?>, SetType<?>> instances = new HashMap<>();
    private static final Map<ConcreteType<?>, SetType<?>> frozenInstances = new HashMap<>();

    private final ConcreteType<T> elements;
    private final SetSerializer<T> serializer;
    private final boolean isMultiCell;

    public static SetType<?> getInstance(TypeParser parser) throws ConfigurationException, SyntaxException
    {
        List<AbstractType> l = parser.getTypeParameters();
        if (l.size() != 1)
            throw new ConfigurationException("SetType takes exactly 1 type parameter");

        return getInstance(l.get(0), true);
    }

    @SuppressWarnings("unchecked")
    public static synchronized <T> SetType<T> getInstance(AbstractType elements, boolean isMultiCell)
    {
        return getInstance((ConcreteType<T>) elements, isMultiCell);
    }

    public static synchronized <T> SetType<T> getInstance(ConcreteType<T> elements, boolean isMultiCell)
    {
        Map<ConcreteType<?>, SetType<?>> internMap = isMultiCell ? instances : frozenInstances;
        @SuppressWarnings("unchecked")
        SetType<T> t = (SetType<T>) internMap.get(elements);

        if (t == null)
        {
            t = new SetType<T>(elements, isMultiCell);
            internMap.put(elements, t);
        }
        return t;
    }

    public SetType(ConcreteType<T> elements, boolean isMultiCell)
    {
        super(ComparisonType.CUSTOM, Kind.SET);
        this.elements = elements;
        this.serializer = SetSerializer.getInstance(elements.getSerializer(), elements);
        this.isMultiCell = isMultiCell;
    }

    @Override
    public boolean referencesUserType(String userTypeName)
    {
        return getElementsType().referencesUserType(userTypeName);
    }

    public AbstractType getElementsType()
    {
        return elements;
    }

    public AbstractType nameComparator()
    {
        return elements;
    }

    public AbstractType valueComparator()
    {
        return EmptyType.instance;
    }

    @Override
    public boolean isMultiCell()
    {
        return isMultiCell;
    }

    @Override
    public SetType<T> freeze()
    {
        if (isMultiCell)
            return getInstance(this.elements, false);
        else
            return this;
    }

    @Override
    public boolean isCompatibleWithFrozen(ConcreteCollectionType<?> previous)
    {
        assert !isMultiCell;
        return this.elements.isCompatibleWith(((SetType<?>) previous).elements);
    }

    @Override
    public boolean isValueCompatibleWithFrozen(ConcreteCollectionType<?> previous)
    {
        // because sets are ordered, any changes to the type must maintain the ordering
        return isCompatibleWithFrozen(previous);
    }

    @Override
    public int compareCustom(ByteBuffer o1, ByteBuffer o2)
    {
        return ListType.compareListOrSet(elements, o1, o2);
    }

    public SetSerializer<T> getSerializer()
    {
        return serializer;
    }

    @Override
    public String toString(boolean ignoreFreezing)
    {
        boolean includeFrozenType = !ignoreFreezing && !isMultiCell();

        StringBuilder sb = new StringBuilder();
        if (includeFrozenType)
            sb.append(FrozenType.class.getName()).append("(");
        sb.append(getClass().getName());
        sb.append(TypeParser.stringifyTypeParameters(Collections.<AbstractType>singletonList(elements), ignoreFreezing || !isMultiCell));
        if (includeFrozenType)
            sb.append(")");
        return sb.toString();
    }

    public List<ByteBuffer> serializedValues(Iterator<Cell> cells)
    {
        List<ByteBuffer> bbs = new ArrayList<ByteBuffer>();
        while (cells.hasNext())
            bbs.add(cells.next().path().get(0));
        return bbs;
    }

    @Override
    public Term fromJSONObject(Object parsed) throws MarshalException
    {
        if (parsed instanceof String)
            parsed = Json.decodeJson((String) parsed);

        if (!(parsed instanceof List))
            throw new MarshalException(String.format(
                    "Expected a list (representing a set), but got a %s: %s", parsed.getClass().getSimpleName(), parsed));

        List<?> list = (List<?>) parsed;
        Set<Term> terms = new HashSet<>(list.size());
        for (Object element : list)
        {
            if (element == null)
                throw new MarshalException("Invalid null element in set");
            terms.add(elements.fromJSONObject(element));
        }

        return new Sets.DelayedValue(elements, terms);
    }

    @Override
    public String toJSONString(ByteBuffer buffer, int protocolVersion)
    {
        return ListType.setOrListToJsonString(buffer, elements, protocolVersion);
    }
}
