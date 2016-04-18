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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.Lists;
import org.apache.cassandra.cql3.Maps;
import org.apache.cassandra.cql3.Sets;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.CollectionSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * The abstract validator that is the base for maps, sets and lists (both frozen and non-frozen).
 *
 * Please note that this comparator shouldn't be used "manually" (through thrift for instance).
 */
public interface CollectionType extends AbstractType
{
    public enum Kind
    {
        MAP(Map.class)
        {
            public ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey)
            {
                return isKey ? Maps.keySpecOf(collection) : Maps.valueSpecOf(collection);
            }
        },
        SET(Set.class)
        {
            public ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey)
            {
                return Sets.valueSpecOf(collection);
            }
        },
        LIST(List.class)
        {
            public ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey)
            {
                return Lists.valueSpecOf(collection);
            }
        };

        Kind(Class<?> type)
        {
            this.type = type;
        }

        public final Class<?> type;
        public abstract ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey);
    }

    public Kind kind();

    public abstract AbstractType nameComparator();
    public abstract AbstractType valueComparator();

    @Override
    public abstract CollectionSerializer<?> getSerializer();

    public ColumnSpecification makeCollectionReceiver(ColumnSpecification collection, boolean isKey);

    public ByteBuffer serializeForNativeProtocol(Iterator<Cell> cells, int version);

    public static CellPath.Serializer cellPathSerializer = new CellPath.Serializer()
    {
        public void serialize(CellPath path, DataOutputPlus out) throws IOException
        {
            ByteBufferUtil.writeWithVIntLength(path.get(0), out);
        }

        public CellPath deserialize(DataInputPlus in) throws IOException
        {
            return CellPath.create(ByteBufferUtil.readWithVIntLength(in));
        }

        public long serializedSize(CellPath path)
        {
            return ByteBufferUtil.serializedSizeWithVIntLength(path.get(0));
        }

        public void skip(DataInputPlus in) throws IOException
        {
            ByteBufferUtil.skipWithVIntLength(in);
        }
    };
}
