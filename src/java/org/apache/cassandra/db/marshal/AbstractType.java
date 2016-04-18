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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.Term;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.serializers.TypeSerializer;

/**
 * Specifies a Comparator for a specific type of ByteBuffer.
 *
 * Note that empty ByteBuffer are used to represent "start at the beginning"
 * or "stop at the end" arguments to get_slice, so the Comparator
 * should always handle those values even if they normally do not
 * represent a valid ByteBuffer for the type being compared.
 */
public interface AbstractType extends Comparator<ByteBuffer>
{
    public Object compose(ByteBuffer bytes);

    public ByteBuffer decompose(Object value);

    /**
     * Same as compare except that this ignore ReversedType. This is to be use when
     * comparing 2 values to decide for a CQL condition (see Operator.isSatisfiedBy) as
     * for CQL, ReversedType is simply an "hint" to the storage engine but it does not
     * change the meaning of queries per-se.
     */
    public int compareForCQL(ByteBuffer v1, ByteBuffer v2);

    /** get a string representation of the bytes suitable for log messages */
    public String getString(ByteBuffer bytes);

    /** get a byte representation of the given string. */
    public ByteBuffer fromString(String source) throws MarshalException;

    /* validate that the byte array is a valid sequence for the type we are supposed to be comparing */
    public void validate(ByteBuffer bytes) throws MarshalException;

    public TypeSerializer<?> getSerializer();

    /**
     * Validate cell value. Unlike {@linkplain #validate(java.nio.ByteBuffer)},
     * cell value is passed to validate its content.
     * Usually, this is the same as validate except collection.
     *
     * @param cellValue ByteBuffer representing cell value
     * @throws MarshalException
     */
    public void validateCellValue(ByteBuffer cellValue) throws MarshalException;

    /* Most of our internal type should override that. */
    public CQL3Type asCQL3Type();

    public boolean isCounter();

    /**
     * Checks to see if two types are equal when ignoring or not ignoring differences in being frozen, depending on
     * the value of the ignoreFreezing parameter.
     * @param other type to compare
     * @param ignoreFreezing if true, differences in the types being frozen will be ignored
     */
    public boolean equals(Object other, boolean ignoreFreezing);

    /**
     * Returns true if this comparator is compatible with the provided
     * previous comparator, that is if previous can safely be replaced by this.
     * A comparator cn should be compatible with a previous one cp if forall columns c1 and c2,
     * if   cn.validate(c1) and cn.validate(c2) and cn.compare(c1, c2) == v,
     * then cp.validate(c1) and cp.validate(c2) and cp.compare(c1, c2) == v.
     *
     * Note that a type should be compatible with at least itself and when in
     * doubt, keep the default behavior of not being compatible with any other comparator!
     */
    public boolean isCompatibleWith(AbstractType previous);

    /**
     * Returns true if values of the other AbstractType can be read and "reasonably" interpreted by the this
     * AbstractType. Note that this is a weaker version of isCompatibleWith, as it does not require that both type
     * compare values the same way.
     *
     * The restriction on the other type being "reasonably" interpreted is to prevent, for example, IntegerType from
     * being compatible with all other types.  Even though any byte string is a valid IntegerType value, it doesn't
     * necessarily make sense to interpret a UUID or a UTF8 string as an integer.
     *
     * Note that a type should be compatible with at least itself.
     */
    public boolean isValueCompatibleWith(AbstractType otherType);

    /**
     * An alternative comparison function used by CollectionsType in conjunction with CompositeType.
     *
     * This comparator is only called to compare components of a CompositeType. It gets the value of the
     * previous component as argument (or null if it's the first component of the composite).
     *
     * Unless you're doing something very similar to CollectionsType, you shouldn't override this.
     */
    public int compareCollectionMembers(ByteBuffer v1, ByteBuffer v2, ByteBuffer collectionName);

    /**
     * An alternative validation function used by CollectionsType in conjunction with CompositeType.
     *
     * This is similar to the compare function above.
     */
    public void validateCollectionMember(ByteBuffer bytes, ByteBuffer collectionName) throws MarshalException;

    public boolean isCollection();

    public boolean isUDT();

    public boolean isMultiCell();

    public boolean isFreezable();

    public boolean isReversed();

    public AbstractType freeze();

    public void checkComparable();

    /**
     * Returns {@code true} for types where empty should be handled like {@code null} like {@link Int32Type}.
     */
    public boolean isEmptyValueMeaningless();

    /**
     * @param ignoreFreezing if true, the type string will not be wrapped with FrozenType(...), even if this type is frozen.
     */
    public String toString(boolean ignoreFreezing);

    /** Given a parsed JSON string, return a byte representation of the object.
     * @param parsed the result of parsing a json string
     **/
    public Term fromJSONObject(Object parsed) throws MarshalException;

    /** Converts a value to a JSON string. */
    public String toJSONString(ByteBuffer buffer, int protocolVersion);

    public ByteBuffer readValue(DataInputPlus in) throws IOException;

    public void skipValue(DataInputPlus in) throws IOException;

    public void writeValue(ByteBuffer value, DataOutputPlus out) throws IOException;

    public long writtenLength(ByteBuffer value);

    /**
     * The number of subcomponents this type has.
     * This is always 1, i.e. the type has only itself as "subcomponents", except for CompositeType.
     */
    public int componentsCount();

    /**
     * Return a list of the "subcomponents" this type has.
     * This always return a singleton list with the type itself except for CompositeType.
     */
    public List<? extends AbstractType> getComponents();

    public boolean referencesUserType(String userTypeName);

    /**
     * This must be overriden by subclasses if necessary so that for any
     * AbstractType, this == TypeParser.parse(toString()).
     *
     * Note that for backwards compatibility this includes the full classname.
     * For CQL purposes the short name is fine.
     */
    public String toString();

    public static List<String> asCQLTypeStringList(List<AbstractType> abstractTypes)
    {
        List<String> r = new ArrayList<>(abstractTypes.size());
        for (AbstractType abstractType : abstractTypes)
            r.add(abstractType.asCQL3Type().toString());
        return r;
    }

    public static AbstractType parseDefaultParameters(AbstractType baseType, TypeParser parser) throws SyntaxException
    {
        Map<String, String> parameters = parser.getKeyValueParameters();
        String reversed = parameters.get("reversed");
        if (reversed != null && (reversed.isEmpty() || reversed.equals("true")))
        {
            return ReversedType.getInstance(baseType);
        }
        else
        {
            return baseType;
        }
    }
}