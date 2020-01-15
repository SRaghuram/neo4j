/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.freki;

import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.neo4j.values.storable.Value;

import static org.neo4j.internal.freki.StreamVByte.writeDeltas;
import static org.neo4j.values.storable.Values.EMPTY_PRIMITIVE_INT_ARRAY;

/**
 * [IN_USE|OFFSETS|LABELS|PROPERTY_KEYS,PROPERTY_VALUES|RELATIONSHIPS]
 */
class MutableNodeRecordData
{
    static final int SIZE_SLOT_HEADER = 3;

    int[] labels = EMPTY_PRIMITIVE_INT_ARRAY;
    MutableIntObjectMap<Property> properties = IntObjectMaps.mutable.empty();

    static class Property implements Comparable<Property>
    {
        int key;
        Value value;

        Property( int key, Value value )
        {
            this.key = key;
            this.value = value;
        }

        @Override
        public int compareTo( Property o )
        {
            return Integer.compare( key, o.key );
        }
    }

    static class Relationship
    {
        long otherNode;
        int type;
        boolean outgoing;
        MutableIntObjectMap<Property> properties = IntObjectMaps.mutable.empty();
    }

    void serialize( ByteBuffer buffer )
    {
        int offsetHeaderPosition = buffer.position();
        int position = offsetHeaderPosition + SIZE_SLOT_HEADER;

        // labels
        position = writeDeltas( labels, buffer.array(), position );
        int propertiesOffset = position;

        // properties
        // format being something like:
        // - StreamVByte array of sorted keys
        // - for each key: type/value   [externalType,internalType]
        Property[] propertiesArray = properties.toArray( new Property[0] );
        Arrays.sort( propertiesArray );
        position = writeDeltas( keysOf( propertiesArray ), buffer.array(), position );
        buffer.position( position );
        PropertyValueFormat writer = new PropertyValueFormat( buffer );
        for ( Property property : propertiesArray )
        {
            property.value.writeTo( writer );
        }
        int relationshipsOffset = buffer.position();

        // write the 3B offset header   msb [rrrr,pppp][rrrr,rrrr][pppp,pppp] lsb
        byte propertyOffsetBits = (byte) propertiesOffset;
        byte relationshipOffsetBits = (byte) relationshipsOffset;
        byte highOffsetBits = (byte) (((relationshipOffsetBits & 0xF00) >>> 4) | ((propertyOffsetBits & 0xF00) >>> 12));
        buffer.put( offsetHeaderPosition,     propertyOffsetBits );
        buffer.put( offsetHeaderPosition + 1, relationshipOffsetBits );
        buffer.put( offsetHeaderPosition + 2, highOffsetBits );
    }

    void deserialize( ByteBuffer buffer )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    private int[] keysOf( Property[] propertiesArray )
    {
        int[] keys = new int[propertiesArray.length];
        for ( int i = 0; i < propertiesArray.length; i++ )
        {
            keys[i] = propertiesArray[i].key;
        }
        return keys;
    }

    static int readOffsetsHeader( ByteBuffer buffer )
    {
        int propertyOffsetBits = buffer.get() & 0xFF;
        int relationshipOffsetBits = buffer.get() & 0xFF;
        int highOffsetBits = buffer.get() & 0xFF;
        int propertyOffset = ((highOffsetBits & 0xF) << 8) | propertyOffsetBits;
        int relationshipOffset = ((highOffsetBits & 0xF0) << 4) | relationshipOffsetBits;
        return (relationshipOffset << 16) | propertyOffset;
    }

    static int propertyOffset( int offsetHeader )
    {
        return offsetHeader & 0xFFFF;
    }

    static int relationshipOffset( int offsetHeader )
    {
        return (offsetHeader & 0xFFFF0000) >>> 16;
    }

//    static Value propertyValue( PropertyValue value )
//    {
//        switch ( value.type() )
//        {
//        case Type.INT:
//            return Values.intValue( value.intValue() );
//        case Type.LONG:
//            return Values.longValue( value.longValue() );
//        case Type.STRING:
//            return Values.stringValue( value.stringValue() );
//        default:
//            throw new UnsupportedOperationException();
//        }
//    }
}
