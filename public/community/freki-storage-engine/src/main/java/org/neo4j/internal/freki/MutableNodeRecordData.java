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
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.util.EagerDegrees;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;

import static java.lang.Long.max;
import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.Record.recordXFactor;
import static org.neo4j.internal.freki.StreamVByte.intArrayTarget;
import static org.neo4j.internal.freki.StreamVByte.readIntDeltas;
import static org.neo4j.internal.freki.StreamVByte.readLongs;
import static org.neo4j.internal.freki.StreamVByte.writeIntDeltas;
import static org.neo4j.internal.freki.StreamVByte.writeLongs;
import static org.neo4j.internal.helpers.Numbers.safeCastIntToUnsignedByte;
import static org.neo4j.util.Preconditions.checkState;

/**
 *                 propertyOffset             relationshipOffset    endOffset
 *                       |                             |                |
 * [IN_USE|OFFSETS|LABELS|PROPERTY_KEYS,PROPERTY_VALUES|REL_TYPES|RELS[]|REL_TYPE_OFFSETS|FW/BW]
 * <--------------------------------------- Record -------------------------------------------->
 *         <---------------------- MutableNodeRecordData -------------------------------------->
 */
class MutableNodeRecordData
{
    static final int ARTIFICIAL_MAX_RELATIONSHIP_COUNTER = 0xFFFFFF;
    static final int SIZE_SLOT_HEADER = 4;
    static final int ARRAY_ENTRIES_PER_RELATIONSHIP = 2;
    static final int FIRST_RELATIONSHIP_ID = 1;
    static final int HAS_DEGREES = 1;

    static final int FLAG_LABELS = 0x1;
    static final int FLAG_PROPERTIES = 0x2;
    static final int FLAG_RELATIONSHIPS = 0x4;
    static final int FLAG_DEGREES = 0x8;

    long nodeId;
    final MutableIntSet labels = IntSets.mutable.empty();
    final MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();
    final MutableIntObjectMap<Relationships> relationships = IntObjectMaps.mutable.empty();
    final EagerDegrees degrees = new EagerDegrees(); // only really for dense nodes
    long nextInternalRelationshipId = FIRST_RELATIONSHIP_ID;
    private long forwardPointer = NULL;
    private long backPointer = NULL;

    MutableNodeRecordData( long nodeId )
    {
        this.nodeId = nodeId;
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
        return nodeId == that.nodeId && nextInternalRelationshipId == that.nextInternalRelationshipId &&
                forwardPointer == that.forwardPointer && backPointer == that.backPointer &&
                labels.equals( that.labels ) && Objects.equals( properties, that.properties ) && Objects.equals( relationships, that.relationships ) &&
                degrees.equals( that.degrees );
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash( nodeId, properties, relationships, degrees, nextInternalRelationshipId, forwardPointer, backPointer );
        result = 31 * result + labels.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return String.format( "ID:%s, labels:%s, properties:%s, relationships:%s, degrees:%s, fw:%s, bw:%d, nextRelId:%d", nodeId, labels, properties,
                relationships, degrees, forwardPointerToString( forwardPointer ), backPointer, nextInternalRelationshipId );
    }

    void copyDataFrom( int flags, MutableNodeRecordData from )
    {
        if ( (flags & FLAG_LABELS) != 0 )
        {
            labels.addAll( labels );
        }
        if ( (flags & FLAG_PROPERTIES) != 0 )
        {
            properties.putAll( from.properties );
        }
        if ( (flags & FLAG_DEGREES) != 0 )
        {
            degrees.addAll( from.degrees );
        }
        if ( (flags & FLAG_RELATIONSHIPS) != 0 )
        {
            relationships.putAll( from.relationships );
            nextInternalRelationshipId = max( from.nextInternalRelationshipId, nextInternalRelationshipId );
        }
        // pointers and nodeId are left untouched
    }

    void clearData( int flags )
    {
        if ( (flags & FLAG_LABELS) != 0 )
        {
            labels.clear();
        }
        if ( (flags & FLAG_PROPERTIES) != 0 )
        {
            properties.clear();
        }
        if ( (flags & FLAG_DEGREES) != 0 )
        {
            degrees.clear();
        }
        if ( (flags & FLAG_RELATIONSHIPS) != 0 )
        {
            relationships.clear();
        }
    }

    long nextInternalRelationshipId()
    {
        return nextInternalRelationshipId++;
    }

    void registerInternalRelationshipId( long internalId )
    {
        nextInternalRelationshipId = max( this.nextInternalRelationshipId, internalId + 1 );
    }

    void deleteRelationship( long internalId, int type, long otherNode, boolean outgoing )
    {
        Relationships relationships = this.relationships.get( type );
        checkState( relationships != null, "No such relationship (%d)-[:%d]->(%d)", nodeId, type, otherNode );
        if ( relationships.remove( internalId, nodeId, otherNode, outgoing ) )
        {
            this.relationships.remove( type );
        }
    }

    boolean hasRelationships()
    {
        return !relationships.isEmpty();
    }

    static class Relationship
    {
        // These two combined makes up the actual external relationship ID
        long internalId;
        long sourceNodeId;
        long otherNode;

        int type;
        boolean outgoing;
        MutableIntObjectMap<Value> properties = IntObjectMaps.mutable.empty();

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
        public int hashCode()
        {
            return Objects.hash( internalId, sourceNodeId, otherNode, type, outgoing, properties );
        }

        @Override
        public String toString()
        {
            long id = externalRelationshipId( sourceNodeId, internalId, otherNode, outgoing );
            return String.format( "ID:%s (%s), %s%s, properties: %s", id, internalId, outgoing ? "->" : " <-", otherNode, properties );
        }
    }

    static class Relationships implements Iterable<Relationship>, Comparable<Relationships>
    {
        private final int type;
        final List<Relationship> relationships = new ArrayList<>();

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

        /**
         * @return {@code true} if the last relationship of this type was removed, otherwise {@code false}.
         */
        boolean remove( long internalId, long sourceNode, long otherNode, boolean outgoing )
        {
            int foundIndex = -1;
            for ( int i = 0, size = relationships.size(); i < size; i++ )
            {
                Relationship relationship = relationships.get( i );
                if ( relationship.internalId == internalId && relationship.sourceNodeId == sourceNode && relationship.otherNode == otherNode &&
                        relationship.outgoing == outgoing )
                {
                    foundIndex = i;
                    break;
                }
            }
            checkState( foundIndex != -1, "No such relationship of type:%d internalId:%d", type, internalId );
            relationships.remove( foundIndex );
            return relationships.isEmpty();
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
        public int hashCode()
        {
            return Objects.hash( type, relationships );
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

    void removeNodeProperty( int propertyKeyId )
    {
        properties.remove( propertyKeyId );
    }

    Relationship createRelationship( long internalId, long otherNode, int type, boolean outgoing )
    {
        // Internal IDs are allocated in the CommandCreationContext and merely passed in here, so it's nice to keep this up to data for sanity's sake
        // Also when moving over from sparse --> dense we can simply read this field to get the correct value
        if ( outgoing )
        {
            registerInternalRelationshipId( internalId );
        }
        return relationships.getIfAbsentPut( type, () -> new MutableNodeRecordData.Relationships( type ) ).add( internalId, nodeId, otherNode, type, outgoing );
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

    void setBackPointer( long backPointer )
    {
        // For "large" records this is the node id, i.e. back-ref to the small record
        this.backPointer = backPointer;
    }

    void serialize( ByteBuffer buffer, SimpleBigValueStore bigPropertyValueStore, Consumer<StorageCommand> commandConsumer )
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
            writeProperties( properties, buffer, bigPropertyValueStore, commandConsumer );
        }
        boolean hasDegrees = !degrees.isEmpty();
        boolean hasRelationships = relationships.notEmpty();
        assert (!hasRelationships && !hasDegrees) || (hasRelationships != hasDegrees) : "Cannot have both degrees and relationships at the same time";
        int relationshipsOffset = hasRelationships || hasDegrees ? buffer.position() : 0;

        // relationships
        // - StreamVByte array of types
        // - StreamVByte array (of length types-1) w/ offsets to starts of type data blocks
        // - For each type:
        // -   Number of relationships
        // -   Rel1 (dir+hasProperties+otherNode+internalId)
        // -   Rel2 (dir+hasProperties+otherNode+internalId)
        // -   ...

        int endOffset = buffer.position();
        if ( hasRelationships || hasDegrees )
        {
            buffer.put( (byte) (hasDegrees ? HAS_DEGREES : 0) );
            int[] typeOffsets;
            if ( hasDegrees )
            {
                int[] types = degrees.types();
                Arrays.sort( types );
                writeIntDeltas( types, buffer );
                int firstTypeOffset = buffer.position();
                typeOffsets = new int[types.length - 1];
                for ( int i = 0; i < types.length; i++ )
                {
                    EagerDegrees.Degree typeDegrees = degrees.findDegree( types[i] );
                    writeIntDeltas( new int[]{typeDegrees.outgoing(), typeDegrees.incoming(), typeDegrees.loop()}, buffer );
                    if ( i < types.length - 1 )
                    {
                        typeOffsets[i] = buffer.position() - firstTypeOffset;
                    }
                }
            }
            else
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
                typeOffsets = new int[allRelationships.length - 1];
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
                            buffer.put( (byte) 0 );
                            writeProperties( relationship.properties, buffer, bigPropertyValueStore, commandConsumer );
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
            }
            endOffset = buffer.position();
            writeIntDeltas( typeOffsets, buffer );
        }

        if ( forwardPointer != NULL )
        {
            writeLongs( new long[]{forwardPointer}, buffer );
            if ( forwardPointerPointsToDense( forwardPointer ) )
            {
                writeLongs( new long[]{nextInternalRelationshipId}, buffer );
            }
        }
        else if ( backPointer != NULL )
        {
            writeLongs( new long[]{backPointer}, buffer );
        }

        // Write the offsets (properties,relationships,end) at the reserved offsets header position at the beginning
        // [bfee,eeee][eeee,eeee][rrrr,rrrr][rrpp,pppp][pppp,pppp]
        // b: this record contains back pointer, i.e. node id
        // f: this record contains forward pointer
        // e: end offset
        // r: relationships offset
        // p: properties offset
        int fw = forwardPointer == NULL ? 0 : 0x40000000;
        int bw = backPointer == NULL ? 0 : 0x80000000;
        buffer.putInt( offsetHeaderPosition, bw | fw | ((endOffset & 0x3FF) << 20) | ((relationshipsOffset & 0x3FF) << 10) | propertiesOffset & 0x3FF );

        buffer.flip();
    }

    private static void writeProperties( MutableIntObjectMap<Value> properties, ByteBuffer buffer, SimpleBigValueStore bigPropertyValueStore,
            Consumer<StorageCommand> commandConsumer )
    {
        int[] propertyKeys = properties.keySet().toSortedArray();
        writeIntDeltas( propertyKeys, buffer );
        PropertyValueFormat writer = new PropertyValueFormat( bigPropertyValueStore, commandConsumer, buffer );
        for ( int propertyKey : propertyKeys )
        {
            properties.get( propertyKey ).writeTo( writer );
        }
    }

    private void readProperties( MutableIntObjectMap<Value> into, ByteBuffer buffer, SimpleBigValueStore bigPropertyValueStore )
    {
        for ( int propertyKey : readIntDeltas( intArrayTarget(), buffer ).array() )
        {
            into.put( propertyKey, PropertyValueFormat.read( buffer, bigPropertyValueStore ) );
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

    void deserialize( ByteBuffer buffer, SimpleBigValueStore bigPropertyValueStore /*temporary, should not be necessary in this scenario*/ )
    {
        assert buffer.position() == 0 : buffer.position();
        int offsetHeader = buffer.getInt(); // [_fee][eeee][eeee][rrrr][rrrr][rrpp][pppp][pppp]
        int labelsOffset = buffer.position();
        int propertiesOffset = propertyOffset( offsetHeader );
        int relationshipOffset = relationshipOffset( offsetHeader );
        int endOffset = endOffset( offsetHeader );
        boolean containsForwardPointer = containsForwardPointer( offsetHeader );
        boolean containsBackPointer = containsBackPointer( offsetHeader );
        if ( containsForwardPointer || containsBackPointer )
        {
            buffer.position( endOffset );
            if ( relationshipOffset != 0 )
            {
                readIntDeltas( StreamVByte.SKIP, buffer );
            }
            if ( containsForwardPointer )
            {
                forwardPointer = readLongs( buffer )[0];
                if ( forwardPointerPointsToDense( forwardPointer ) )
                {
                    nextInternalRelationshipId = readLongs( buffer )[0];
                }
            }
            else
            {
                nextInternalRelationshipId = FIRST_RELATIONSHIP_ID;
            }
            if ( containsBackPointer )
            {
                backPointer = readLongs( buffer )[0];
            }
        }

        buffer.position( labelsOffset );
        labels.clear();
        labels.addAll( readIntDeltas( intArrayTarget(), buffer ).array() );
        properties.clear();
        if ( propertiesOffset != 0 )
        {
            checkState( buffer.position() == propertiesOffset, "Mismatching properties offset:%d and expected offset:%d", buffer.position(), propertiesOffset );
            readProperties( properties, buffer, bigPropertyValueStore );
        }

        relationships.clear();
        degrees.clear();
        if ( relationshipOffset != 0 )
        {
            boolean hasDegrees = buffer.get() == HAS_DEGREES;
            int[] relationshipTypes = readIntDeltas( intArrayTarget(), buffer ).array();
            if ( hasDegrees )
            {
                for ( int relationshipType : relationshipTypes )
                {
                    int[] typeDegrees = readIntDeltas( new StreamVByte.IntArrayTarget(), buffer ).array();
                    degrees.add( relationshipType, typeDegrees[0], typeDegrees[1], typeDegrees[2] );
                }
            }
            else
            {
                for ( int relationshipType : relationshipTypes )
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

                        if ( outgoing && internalId >= nextInternalRelationshipId )
                        {
                            nextInternalRelationshipId = internalId + 1;
                        }
                        Relationship relationship = relationships.add( internalId, nodeId, otherNodeOf( otherNodeRaw ), relationshipType, outgoing );
                        if ( relationshipHasProperties( otherNodeRaw ) )
                        {
                            buffer.get(); //blocksize
                            readProperties( relationship.properties, buffer, bigPropertyValueStore );
                        }
                    }
                    this.relationships.put( relationships.type, relationships );
                }
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

    static boolean containsBackPointer( int offsetsHeader )
    {
        return (offsetsHeader & 0x80000000) != 0;
    }

    static long externalRelationshipId( long sourceNode, long internalRelationshipId )
    {
        return sourceNode | (internalRelationshipId << 40);
    }

    static long externalRelationshipId( long sourceNode, long internalRelationshipId, long otherNode, boolean outgoing )
    {
        long nodeId = outgoing ? sourceNode : otherNode;
        return externalRelationshipId( nodeId, internalRelationshipId );
    }

    static long nodeIdFromRelationshipId( long relationshipId )
    {
        return relationshipId & 0xFF_FFFFFFFFL;
    }

    static long internalRelationshipIdFromRelationshipId( long relationshipId )
    {
        return relationshipId >>> 40;
    }

    // Forward pointer: [    ,    ][iiii,iiii][iiii,iiii][iiii,iiii]  [iiii,iiii][iiii,iiii][iiii,iiii][iiii,issd]
    // i: id of the record to point to
    // s: size exponential of the record id
    // d: 1 if this node has data in dense store, otherwise 0
    // examples:
    // NULL (-1): no forward pointer at all
    // i=5, s=1, d=0: fw pointer to x2 w/ id 5
    // i=3, s=2, d=1: fw pointer to x4 w/ id 3 and more data is in dense store too
    // i=0, s=0, d=1: more data is in dense store
    static boolean forwardPointerPointsToDense( long forwardPointer )
    {
        return forwardPointer != NULL && (forwardPointer & 0x1) != 0;
    }

    static boolean forwardPointerPointsToXRecord( long forwardPointer )
    {
        return forwardPointer != NULL && (forwardPointer & 0x6L) != 0;
    }

    static int sizeExponentialFromForwardPointer( long forwardPointer )
    {
        Preconditions.checkArgument( forwardPointer != NULL, "NULL FW pointer" );
        return (int) ((forwardPointer >>> 1) & 0x3);
    }

    static long idFromForwardPointer( long forwardPointer )
    {
        Preconditions.checkArgument( forwardPointer != NULL, "NULL FW pointer" );
        return forwardPointer >>> 3;
    }

    static long buildForwardPointer( int sizeExp, long id, boolean hasDense )
    {
        return (id << 3) | (sizeExp << 1) | (hasDense ? 0x1 : 0);
    }

    static String forwardPointerToString( long forwardPointer )
    {
        if ( forwardPointer == NULL )
        {
            return "NULL";
        }
        StringBuilder builder = new StringBuilder();
        if ( forwardPointerPointsToXRecord( forwardPointer ) )
        {
            builder.append( String.format( "->[x%d]%d",
                    recordXFactor( sizeExponentialFromForwardPointer( forwardPointer ) ), idFromForwardPointer( forwardPointer ) ) );
        }
        if ( forwardPointerPointsToDense( forwardPointer ) )
        {
            builder.append( "->DENSE" );
        }
        return builder.toString();
    }

    static int not( int flags )
    {
        return ~flags;
    }
}
