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
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.eclipse.collections.impl.factory.primitive.IntSets;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;

import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.StreamVByte.intArrayTarget;
import static org.neo4j.internal.freki.StreamVByte.readIntDeltas;
import static org.neo4j.internal.freki.StreamVByte.readLongs;
import static org.neo4j.internal.freki.StreamVByte.writeIntDeltas;
import static org.neo4j.internal.freki.StreamVByte.writeLongs;
import static org.neo4j.internal.helpers.Numbers.safeCastIntToUnsignedByte;
import static org.neo4j.util.Preconditions.checkState;

/**
 * [IN_USE|OFFSETS|LABELS|PROPERTY_KEYS,PROPERTY_VALUES|REL_TYPES|RELS[]|REL_TYPE_OFFSETS|FW]
 * <--------------------------------------- Record ----------------------------------------->
 *         <---------------------- MutableNodeRecordData ----------------------------------->
 */
class MutableNodeRecordData
{
    static final int ARTIFICIAL_MAX_RELATIONSHIP_COUNTER = 0xFFFFFF;
    static final int SIZE_SLOT_HEADER = 4;
    static final int ARRAY_ENTRIES_PER_RELATIONSHIP = 2;

    private final long id;
    MutableIntSet labels = IntSets.mutable.empty();
    private MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();
    private MutableIntObjectMap<Relationships> relationships = IntObjectMaps.mutable.empty();
    private long internalRelationshipIdCounter = 1;
    private long forwardPointer = NULL;

    MutableNodeRecordData( long id )
    {
        this.id = id;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        MutableNodeRecordData that = (MutableNodeRecordData) o;
        return id == that.id && internalRelationshipIdCounter == that.internalRelationshipIdCounter && forwardPointer == that.forwardPointer &&
                labels.equals( that.labels ) && Objects.equals( properties, that.properties ) && Objects.equals( relationships, that.relationships );
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash( id, properties, relationships, internalRelationshipIdCounter, forwardPointer );
        result = 31 * result + labels.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return String.format( "ID:%s, labels:%s, properties:%s, relationships:%s, fw:%d", id, labels, properties, relationships, forwardPointer );
    }

    void moveDataTo( MutableNodeRecordData otherData )
    {
        otherData.labels.addAll( labels );
        otherData.properties.putAll( properties );
        otherData.relationships.putAll( relationships );
        otherData.internalRelationshipIdCounter = internalRelationshipIdCounter;
        clearData();
    }

    void clearData()
    {
        // Doesn't clear entity/identity things like id and forwardPointer
        labels.clear();
        properties.clear();
        relationships.clear();
        internalRelationshipIdCounter = 1;
    }

    static class Relationship
    {
        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            Relationship that = (Relationship) o;
            return internalId == that.internalId && sourceNodeId == that.sourceNodeId && otherNode == that.otherNode && type == that.type &&
                    outgoing == that.outgoing && properties.equals( that.properties );
        }

        @Override
        public String toString()
        {
            long id = externalRelationshipId( sourceNodeId, internalId, otherNode, outgoing );
            return String.format( "ID:%s (%s), %s%s, properties: %s", id, internalId, outgoing ? "->" : " <-", otherNode, properties );
        }

        // These two combined makes up the actual external relationship ID
        long internalId;
        long sourceNodeId;

        long otherNode;
        int type;
        boolean outgoing;
        private MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();

        Relationship( long internalId, long sourceNodeId, long otherNode, int type, boolean outgoing )
        {
            this.internalId = internalId;
            this.sourceNodeId = sourceNodeId;
            this.otherNode = otherNode;
            this.type = type;
            this.outgoing = outgoing;
        }

        void addProperty( int propertyKeyId, Value value )
        {
            properties.put( propertyKeyId, value );
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

        Relationship add( long id, long sourceNode, long otherNode, int type, boolean outgoing )
        {
            Relationship relationship = new Relationship( id, sourceNode, otherNode, type, outgoing );
            relationships.add( relationship );
            return relationship;
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
            int numRelationships = relationships.size();
            long[] array = new long[numRelationships * ARRAY_ENTRIES_PER_RELATIONSHIP];
            for ( int i = 0; i < numRelationships; i++ )
            {
                Relationship relationship = relationships.get( i );
                int arrayIndex = i * ARRAY_ENTRIES_PER_RELATIONSHIP;
                assert (relationship.otherNode & 0xC0000000_00000000L) == 0; // using 2 bits as header
                // msb [62b:otherNode,1b:outgoing,1b:hasProperties] lsb
                array[arrayIndex] = relationship.otherNode << 2 | (relationship.properties.notEmpty() ? 1 : 0) << 1 | (relationship.outgoing ? 1 : 0 );
                // TODO the schema we use to write this packed long[] is the one initially intended for mostly "big" longs and
                //      therefore it doesn't scale all the way down to 1B, just 3B. Therefore for most of the relationships in a store
                //      the waste is going to be 2B
                array[arrayIndex + 1] = relationship.internalId;
            }
            return array;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            Relationships that = (Relationships) o;
            return type == that.type && relationships.equals( that.relationships );
        }

        @Override
        public String toString()
        {
            return String.format( "Type:%s, %s", type, relationships );
        }
    }

    void setNodeProperty( int propertyKeyId, Value value )
    {
        properties.put( propertyKeyId, value );
    }

    Relationship createRelationship( Relationship sourceNodeRelationship, long otherNode, int type )
    {
        checkState( internalRelationshipIdCounter < ARTIFICIAL_MAX_RELATIONSHIP_COUNTER, "Relationship counter exhausted for node %d", id );
        long internalId = sourceNodeRelationship != null ? sourceNodeRelationship.internalId : internalRelationshipIdCounter++;
        boolean outgoing = sourceNodeRelationship == null;
        return relationships.getIfAbsentPut( type, () -> new MutableNodeRecordData.Relationships( type ) ).add( internalId, id, otherNode, type, outgoing );
    }

    // [ssff,ffff][ffff,ffff][ffff,ffff][ffff,ffff] [ffff,ffff][ffff,ffff][ffff,ffff][ffff,ffff]
    // s: sizeExp
    // f: forward pointer id
    void setForwardPointer( long forwardPointer )
    {
        this.forwardPointer = forwardPointer;
    }

    long getForwardPointer()
    {
        return this.forwardPointer;
    }

    void serialize( ByteBuffer buffer )
    {
        buffer.clear();
        int offsetHeaderPosition = buffer.position();
        // labels
        buffer.position( offsetHeaderPosition + SIZE_SLOT_HEADER );
        writeIntDeltas( labels.toSortedArray(), buffer );

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

        // relationships
        // - StreamVByte array of types
        // - StreamVByte array (of length types-1) w/ offsets to starts of type data blocks
        // - For each type:
        // -   Number of relationships
        // -   Rel1 (dir+hasProperties+otherNode+internalId)
        // -   Rel2 (dir+hasProperties+otherNode+internalId)
        // -   ...

        int endOffset = buffer.position();
        if ( relationships.notEmpty() )
        {
            Relationships[] allRelationships = this.relationships.toArray( new Relationships[relationships.size()] );
            Arrays.sort( allRelationships );
            writeIntDeltas( typesOf( allRelationships ), buffer );
            int firstTypeOffset = buffer.position();
            long[][] allPackedRelationships = new long[allRelationships.length][];
            for ( int i = 0; i < allRelationships.length; i++ )
            {
                allPackedRelationships[i] = allRelationships[i].packIntoLongArray();
            }
            // end of types header, below is the relationship data
            int[] typeOffsets = new int[allRelationships.length - 1];
            for ( int i = 0; i < allPackedRelationships.length; i++ )
            {
                //First write all the relationships of the type
                writeLongs( allPackedRelationships[i], buffer );
                //Then write the properties
                for ( Relationship relationship : allRelationships[i].relationships )
                {
                    if ( relationship.properties.notEmpty() )
                    {
                        // Relationship properties will have a 1-byte header which is the size of the keys+values block,
                        // this to efficiently be able to skip through to a specific properties set
                        int blockSizeHeaderOffset = buffer.position();
                        buffer.position( blockSizeHeaderOffset + 1 );
                        writeProperties( relationship.properties, buffer );
                        int blockSize = buffer.position() - blockSizeHeaderOffset;
                        buffer.put( blockSizeHeaderOffset, safeCastIntToUnsignedByte( blockSize ) );
                    }

                    // We don't quite need the offset to _after_ the last type data block
                    if ( i < allRelationships.length - 1 )
                    {
                        typeOffsets[i] = buffer.position() - firstTypeOffset;
                    }
                }
            }
            endOffset = buffer.position();
            writeIntDeltas( typeOffsets, buffer );
        }

        if ( forwardPointer != NULL )
        {
            buffer.putLong( forwardPointer );
        }

        // Write the offsets (properties,relationships,end) at the reserved offsets header position at the beginning
        // [ fee,eeee][eeee,eeee][rrrr,rrrr][rrpp,pppp][pppp,pppp]
        // f: this record contains forward pointer
        // e: end offset
        // r: relationships offset
        // p: properties offset
        int fw = forwardPointer == NULL ? 0 : 0x40000000;
        buffer.putInt( offsetHeaderPosition, fw | ((endOffset & 0x3FF) << 20) | ((relationshipsOffset & 0x3FF) << 10) | propertiesOffset & 0x3FF );
    }

    private static void writeProperties( MutableIntObjectMap<Value> properties, ByteBuffer buffer )
    {
        int[] propertyKeys = properties.keySet().toSortedArray();
        writeIntDeltas( propertyKeys, buffer );
        PropertyValueFormat writer = new PropertyValueFormat( buffer );
        for ( int propertyKey : propertyKeys )
        {
            properties.get( propertyKey ).writeTo( writer );
        }
    }

    private void readProperties( MutableIntObjectMap<Value> into, ByteBuffer buffer )
    {
        for ( int propertyKey : readIntDeltas( intArrayTarget(), buffer ).array() )
        {
            into.put( propertyKey, PropertyValueFormat.read( buffer ) );
        }
    }

    static boolean relationshipIsOutgoing( long relationshipOtherNodeRawLong )
    {
        return (relationshipOtherNodeRawLong & 0b01) != 0;
    }

    static boolean relationshipHasProperties( long relationshipOtherNodeRawLong )
    {
        return (relationshipOtherNodeRawLong & 0b10) != 0;
    }

    static long otherNodeOf( long relationshipOtherNodeRawLong )
    {
        return relationshipOtherNodeRawLong >>> 2;
    }

    void deserialize( ByteBuffer buffer )
    {
        int offsetHeader = buffer.getInt(); // [_fee][eeee][eeee][rrrr][rrrr][rrpp][pppp][pppp]
        int propertiesOffset = propertyOffset( offsetHeader );
        int relationshipOffset = relationshipOffset( offsetHeader );
        int endOffset = endOffset( offsetHeader );
        boolean containsForwardPointer = containsForwardPointer( offsetHeader );

        labels = IntSets.mutable.of( readIntDeltas( intArrayTarget(), buffer ).array() );
        properties.clear();
        if ( propertiesOffset != 0 )
        {
            readProperties( properties, buffer );
        }

        relationships.clear();
        internalRelationshipIdCounter = 1;
        if ( relationshipOffset != 0 )
        {
            for ( int relationshipType : readIntDeltas( intArrayTarget(), buffer ).array() )
            {
                Relationships relationships = new Relationships( relationshipType );
                long[] packedRelationships = readLongs( buffer );
                int numRelationships = packedRelationships.length / ARRAY_ENTRIES_PER_RELATIONSHIP;
                for ( int i = 0; i < numRelationships; i++ )
                {
                    int relationshipArrayIndex = i * ARRAY_ENTRIES_PER_RELATIONSHIP;
                    long otherNodeRaw = packedRelationships[ relationshipArrayIndex ];
                    boolean outgoing =  relationshipIsOutgoing( otherNodeRaw );
                    long internalId = packedRelationships[ relationshipArrayIndex + 1 ];

                    if ( outgoing && internalId >= internalRelationshipIdCounter )
                    {
                        internalRelationshipIdCounter = internalId + 1;
                    }
                    Relationship relationship = relationships.add( internalId, id, otherNodeOf( otherNodeRaw ), relationshipType, outgoing );
                    if ( relationshipHasProperties( otherNodeRaw ) )
                    {
                        buffer.get(); //blocksize
                        readProperties( relationship.properties, buffer );
                    }
                }
                this.relationships.put( relationships.type, relationships );
            }
        }

        if ( endOffset != 0 )
        {
            StreamVByte.readIntDeltas( StreamVByte.SKIP, buffer );
            if ( containsForwardPointer )
            {
                forwardPointer = buffer.getLong();
            }
        }
    }

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
        return buffer.getInt();
    }

    static int propertyOffset( int offsetHeader )
    {
        return offsetHeader & 0x3FF;
    }

    static int relationshipOffset( int offsetHeader )
    {
        return (offsetHeader >>> 10) & 0x3FF;
    }

    static int endOffset( int offsetHeader )
    {
        return (offsetHeader >>> 20) & 0x3FF;
    }

    static boolean containsForwardPointer( int offsetHeader )
    {
        return (offsetHeader & 0x40000000) != 0;
    }

    static long externalRelationshipId( long sourceNode, long internalRelationshipId, long otherNode, boolean outgoing )
    {
        long nodeId = outgoing ? sourceNode : otherNode;
        return nodeId | (internalRelationshipId << 40);
    }

    static long nodeIdFromRelationshipId( long relationshipId )
    {
        return relationshipId & 0xFF_FFFFFFFFL;
    }

    static long internalRelationshipIdFromRelationshipId( long relationshipId )
    {
        return relationshipId >>> 40;
    }

    static int sizeExponentialFromForwardPointer( long forwardPointer )
    {
        Preconditions.checkArgument( forwardPointer != NULL, "NULL FW pointer" );
        return (int) (forwardPointer >>> 62);
    }

    static long idFromForwardPointer( long forwardPointer )
    {
        Preconditions.checkArgument( forwardPointer != NULL, "NULL FW pointer" );
        return forwardPointer & 0x3FFFFFFF_FFFFFFFFL;
    }

    static long forwardPointer( int sizeExp, long id )
    {
        return id | (((long) sizeExp) << 62);
    }
}
