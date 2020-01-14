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

import com.google.flatbuffers.FlatBufferBuilder;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;

import java.nio.ByteBuffer;

import org.neo4j.internal.freki.generated.PropertyValue;
import org.neo4j.internal.freki.generated.StoreRecord;
import org.neo4j.internal.freki.generated.Type;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class MutableNodeRecordData
{
    boolean inUse;
    int[] labels;
    MutableIntObjectMap<Property> properties = IntObjectMaps.mutable.empty();

    static class Property
    {
        int key;
        Value value;

        Property( int key, Value value )
        {
            this.key = key;
            this.value = value;
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
        FlatBufferBuilder builder = new FlatBufferBuilder( buffer );
        int labelsOffset = labels != null ? StoreRecord.createLabelsVector( builder, labels ) : -1;
        int[] propertyOffsets = new int[properties.size()];
        int propertyI = 0;
        for ( Property property : properties )
        {
            Value value = property.value;
            if ( value instanceof IntValue )
            {
                PropertyValue.startPropertyValue( builder );
                PropertyValue.addIntValue( builder, ((IntValue) value).value() );
                PropertyValue.addType( builder, Type.INT );
            }
            else if ( value instanceof LongValue )
            {
                PropertyValue.startPropertyValue( builder );
                PropertyValue.addLongValue( builder, ((LongValue) value).longValue() );
                PropertyValue.addType( builder, Type.LONG );
            }
            else
            {
                String string = ((TextValue) value).stringValue();
                int stringOffset = builder.createString( string );
                PropertyValue.startPropertyValue( builder );
                PropertyValue.addStringValue( builder, stringOffset );
                PropertyValue.addType( builder, Type.STRING );
            }
            int valueOffset = PropertyValue.endPropertyValue( builder );
            propertyOffsets[propertyI++] = org.neo4j.internal.freki.generated.Property.createProperty( builder, property.key, valueOffset );
        }
        StoreRecord.startStoreRecord( builder );
        StoreRecord.addInUse( builder, inUse );
        if ( labelsOffset != -1 )
        {
            StoreRecord.addLabels( builder, labelsOffset );
        }
        for ( int propertyOffset : propertyOffsets )
        {
            StoreRecord.addProperties( builder, propertyOffset );
        }
        int finish = StoreRecord.endStoreRecord( builder );
        builder.finish( finish );
    }

    void deserialize( ByteBuffer buffer )
    {
        StoreRecord storeRecord = StoreRecord.getRootAsStoreRecord( buffer );
        inUse = storeRecord.inUse();
        labels = new int[storeRecord.labelsLength()];
        for ( int i = 0; i < labels.length; i++ )
        {
            labels[i] = storeRecord.labels( i );
        }
        int propertiesLength = storeRecord.propertiesLength();
        for ( int i = 0; i < propertiesLength; i++ )
        {
            org.neo4j.internal.freki.generated.Property property = storeRecord.properties( i );
            properties.put( property.key(), new Property( property.key(), propertyValue( property.value() ) ) );
        }
    }

    static Value propertyValue( PropertyValue value )
    {
        switch ( value.type() )
        {
        case Type.INT:
            return Values.intValue( value.intValue() );
        case Type.LONG:
            return Values.longValue( value.longValue() );
        case Type.STRING:
            return Values.stringValue( value.stringValue() );
        default:
            throw new UnsupportedOperationException();
        }
    }
}
