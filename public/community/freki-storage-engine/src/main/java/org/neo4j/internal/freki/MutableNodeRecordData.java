/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;

import org.neo4j.values.storable.Value;

import static org.neo4j.internal.freki.StreamVByte.writeIntDeltas;
import static org.neo4j.values.storable.Values.EMPTY_PRIMITIVE_INT_ARRAY;

/**
 * [IN_USE|OFFSETS|LABELS|PROPERTY_KEYS,PROPERTY_VALUES|RELATIONSHIPS]
 */
class MutableNodeRecordData
{
    static final int SIZE_SLOT_HEADER = 3;

    int[] labels = EMPTY_PRIMITIVE_INT_ARRAY;
    MutableIntObjectMap<Property> properties = IntObjectMaps.mutable.empty();
    MutableIntObjectMap<Relationships> relationships = IntObjectMaps.mutable.empty();

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
        // TODO also store the ID
        long id;
        long otherNode;
        int type;
        boolean outgoing;
        MutableIntObjectMap<Property> properties = IntObjectMaps.mutable.empty();

        Relationship( long id, long otherNode, int type, boolean outgoing )
        {
            this.otherNode = otherNode;
            this.type = type;
            this.outgoing = outgoing;
        }
    }

    static class Relationships implements Iterable<Relationship>, Comparable<Relationships>
    {
        private final int type;
        private final List<Relationship> relationships = new ArrayList<>();

        Relationships( int type )
        {
            this.type = type;
        }

        void add( long id, long otherNode, int type, boolean outgoing )
        {
            relationships.add( new Relationship( id, otherNode, type, outgoing ) );
        }

        @Nonnull
        @Override
        public Iterator<Relationship> iterator()
        {
            return relationships.iterator();
        }

        @Override
        public int compareTo( Relationships o )
        {
            return Integer.compare( type, o.type );
        }

        long[] packIntoLongArray()
        {
            long[] array = new long[relationships.size()];
            for ( int i = 0; i < relationships.size(); i++ )
            {
                Relationship relationship = relationships.get( i );
                assert (relationship.otherNode & 0xC0000000_00000000L) == 0; // using 2 bits as header
                array[i] = relationship.otherNode << 2
                        | (relationship.outgoing ? 1 : 0) << 1
                        | (relationship.properties.notEmpty() ? 1 : 0);
            }
            return array;
        }
    }

    void serialize( ByteBuffer buffer )
    {
        int offsetHeaderPosition = buffer.position();
        // labels
        buffer.position( offsetHeaderPosition + SIZE_SLOT_HEADER );
        writeIntDeltas( labels, buffer );

        // properties
        // format being something like:
        // - StreamVByte array of sorted keys
        // - for each key: type/value   [externalType,internalType]

        int propertiesOffset = 0;
        if ( properties.notEmpty() )
        {
            propertiesOffset = buffer.position();
            writeProperties( properties, buffer );
        }
        int relationshipsOffset = relationships.notEmpty() ? buffer.position() : 0;

        // write the 3B offset header   msb [rrrr,pppp][rrrr,rrrr][pppp,pppp] lsb
        byte propertyOffsetBits = (byte) propertiesOffset;
        byte relationshipOffsetBits = (byte) relationshipsOffset;
        byte highOffsetBits = (byte) (((relationshipOffsetBits & 0xF00) >>> 4) | ((propertyOffsetBits & 0xF00) >>> 12));
        buffer.put( offsetHeaderPosition,     propertyOffsetBits );
        buffer.put( offsetHeaderPosition + 1, relationshipOffsetBits );
        buffer.put( offsetHeaderPosition + 2, highOffsetBits );

        // relationships
        // - StreamVByte array of types
        // - StreamVByte array (of length types-1) w/ offsets to starts of type data blocks
        // - For each type:
        // -   Number of relationships
        // -   Rel1 (dir+otherNode)
        // -   Rel2 (dir+otherNode)
        // -   ...

        if ( relationships.notEmpty() )
        {
            Relationships[] allRelationships = this.relationships.toArray( new Relationships[relationships.size()] );
            Arrays.sort( allRelationships );
            writeIntDeltas( typesOf( allRelationships ), buffer );
            long[][] allPackedRelationships = new long[allRelationships.length][];
            int[] typeOffsets = new int[allRelationships.length - 1];
            for ( int i = 0; i < allRelationships.length; i++ )
            {
                allPackedRelationships[i] = allRelationships[i].packIntoLongArray();
                // We don't quite need the offset to _after_ the last type data block
                if ( i < allRelationships.length - 1 )
                {
                    int prev = i == 0 ? 0 : typeOffsets[i - 1];
                    typeOffsets[i] = prev + StreamVByte.calculateLongsSize( allPackedRelationships[i] );
                }
            }
            StreamVByte.writeIntDeltas( typeOffsets, buffer );
            // end of types and offsets header, below is the relationship data
            for ( int i = 0; i < allPackedRelationships.length; i++ )
            {
                //First write all the relationships of the type
                StreamVByte.writeLongs( allPackedRelationships[i], buffer );
                //Then write the properties
                for ( Relationship relationship : allRelationships[i].relationships )
                {
                    if ( relationship.properties.notEmpty() )
                    {
                        writeProperties( relationship.properties, buffer );
                    }
                }
            }
        }
    }

    private static void writeProperties(  MutableIntObjectMap<Property> properties, ByteBuffer buffer )
    {
        Property[] propertiesArray = properties.toArray( new Property[0] );
        Arrays.sort( propertiesArray );
        writeIntDeltas( keysOf( propertiesArray ), buffer );
        PropertyValueFormat writer = new PropertyValueFormat( buffer );
        for ( Property property : propertiesArray )
        {
            property.value.writeTo( writer );
        }
    }

    void deserialize( ByteBuffer buffer )
    {
        throw new UnsupportedOperationException( "Not implemented yet" );
    }

    private static int[] keysOf( Property[] propertiesArray )
    {
        int[] keys = new int[propertiesArray.length];
        for ( int i = 0; i < propertiesArray.length; i++ )
        {
            keys[i] = propertiesArray[i].key;
        }
        return keys;
    }

    // TODO DRY
    private int[] typesOf( Relationships[] relationshipsArray )
    {
        int[] keys = new int[relationshipsArray.length];
        for ( int i = 0; i < relationshipsArray.length; i++ )
        {
            keys[i] = relationshipsArray[i].type;
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
}
