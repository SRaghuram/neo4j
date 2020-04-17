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

import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.Degrees;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.util.EagerDegrees;
import org.neo4j.util.Preconditions;
import org.neo4j.values.storable.Value;

import static java.lang.Long.max;
import static org.neo4j.internal.freki.FrekiMainStoreCursor.NULL;
import static org.neo4j.internal.freki.Record.recordXFactor;
import static org.neo4j.internal.freki.StreamVByte.readIntDeltas;
import static org.neo4j.internal.freki.StreamVByte.readInts;
import static org.neo4j.internal.freki.StreamVByte.readLongs;
import static org.neo4j.internal.freki.StreamVByte.writeIntDeltas;
import static org.neo4j.internal.freki.StreamVByte.writeInts;
import static org.neo4j.internal.freki.StreamVByte.writeLongs;
import static org.neo4j.internal.helpers.Numbers.safeCastIntToUnsignedByte;
import static org.neo4j.util.Preconditions.checkState;

/**
 * [IN_USE|OFFSETS|LABELS|PROPERTY_KEYS,PROPERTY_VALUES|REL_TYPES|RELS[]|REL_TYPE_OFFSETS|RECORD_POINTER]
 * <--------------------------------------- Record ----------------------------------------------------->
 *         <---------------------- MutableNodeRecordData ----------------------------------------------->
 */
class MutableNodeRecordData
{
    static final int ARTIFICIAL_MAX_RELATIONSHIP_COUNTER = 0xFFFFFF;
    static final int ARRAY_ENTRIES_PER_RELATIONSHIP = 2;
    static final int FIRST_RELATIONSHIP_ID = 1;

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
    private long recordPointer = NULL;
    private boolean isDense;

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
                recordPointer == that.recordPointer && isDense == that.isDense &&
                labels.equals( that.labels ) && Objects.equals( properties, that.properties ) && Objects.equals( relationships, that.relationships ) &&
                degrees.equals( that.degrees );
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash( nodeId, properties, relationships, degrees, nextInternalRelationshipId, recordPointer, isDense );
        result = 31 * result + labels.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return String.format( "ID:%s, labels:%s, properties:%s, relationships:%s, degrees:%s, pointer:%s, dense:%b, nextRelId:%d", nodeId, labels, properties,
                relationships, degrees, recordPointerToString( recordPointer ), isDense, nextInternalRelationshipId );
    }

    void copyDataFrom( MutableNodeRecordData from )
    {
        labels.addAll( from.labels );
        properties.putAll( from.properties );
        degrees.addAll( from.degrees );
        relationships.putAll( from.relationships );
        nextInternalRelationshipId = max( from.nextInternalRelationshipId, nextInternalRelationshipId );
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
    // f: record pointer id
    void setRecordPointer( long recordPointer )
    {
        this.recordPointer = recordPointer;
    }

    long getRecordPointer()
    {
        return this.recordPointer;
    }

    void setDense( boolean isDense )
    {
        this.isDense = isDense;
    }

    boolean isDense()
    {
        return this.isDense;
    }

    boolean serializeMainData( ByteBuffer[] intermediateBuffers, SimpleBigValueStore bigPropertyValueStore, Consumer<StorageCommand> bigValueCommandConsumer )
    {
        assert Arrays.stream( intermediateBuffers ).allMatch( buff -> buff.position() == 0 );

        if ( labels.notEmpty() )
        {
            writeLabels( intermediateBuffers[GraphUpdates.LABELS] );
        }

        if ( properties.notEmpty() )
        {
            writeProperties( properties, intermediateBuffers[GraphUpdates.PROPERTIES], bigPropertyValueStore, bigValueCommandConsumer );
        }

        if ( relationships.notEmpty() )
        {
            // TODO cheaper pre-check if best case cannot fit
            try
            {
                int[] typeOffsets = writeRelationships( intermediateBuffers[GraphUpdates.RELATIONSHIPS], bigPropertyValueStore, bigValueCommandConsumer );
                writeTypeOffsets( intermediateBuffers[GraphUpdates.RELATIONSHIPS_OFFSETS], typeOffsets );
                return false;
            }
            catch ( BufferOverflowException | ArrayIndexOutOfBoundsException e )
            {
                //TODO this is slow, better check after each serialized rel for overflow.
                return true;
            }
        }

        if ( !degrees.isEmpty() )
        {
            writeDegrees( intermediateBuffers[GraphUpdates.DEGREES] );
        }
        return isDense;
    }

    void serializeDegrees( ByteBuffer degreesBuffer )
    {
        assert isDense;
        writeDegrees( degreesBuffer );
    }

    void serializeNextInternalRelationshipId( ByteBuffer buffer )
    {
        assert buffer.position() == 0;
        writeNextInternalRelationshipId( buffer );
    }

    void serializeRecordPointer( ByteBuffer buffer )
    {
        assert buffer.position() == 0;
        writeRecordPointer( buffer );
    }

    private void writeNextInternalRelationshipId( ByteBuffer buffer )
    {
        writeLongs( new long[]{nextInternalRelationshipId}, buffer );
    }

    private void writeRecordPointer( ByteBuffer buffer )
    {
        writeLongs( new long[]{recordPointer}, buffer );
    }

    private void writeLabels( ByteBuffer buffer )
    {
        writeIntDeltas( labels.toSortedArray(), buffer );
    }

    private int[] writeRelationships( ByteBuffer buffer, SimpleBigValueStore bigPropertyValueStore, Consumer<StorageCommand> commandConsumer )
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
        return typeOffsets;
    }

    private void writeTypeOffsets( ByteBuffer buffer, int[] typeOffsets )
    {
        writeIntDeltas( typeOffsets, buffer );
    }

    private void writeDegrees( ByteBuffer buffer )
    {
        int[] types = degrees.types();
        int typesLength = types.length;
        for ( int i = 0; i < typesLength; i++ )
        {
            int type = types[i];
            if ( degrees.findDegree( type ).isEmpty() )
            {
                if ( i + 1 < typesLength )
                {
                    types[i] = types[typesLength - 1];
                    i--;
                }
                typesLength--;
            }
        }
        if ( typesLength < types.length )
        {
            types = Arrays.copyOf( types, typesLength );
        }
        Arrays.sort( types );
        int[] degreesArray = new int[typesLength * 3];
        int degreesArrayIndex = 0;
        for ( int t = 0; t < typesLength; t++ )
        {
            EagerDegrees.Degree typeDegrees = degrees.findDegree( types[t] );
            int loop = typeDegrees.loop();
            degreesArray[degreesArrayIndex++] = typeDegrees.outgoing() << 1 | (loop > 0 ? 1 : 0);
            degreesArray[degreesArrayIndex++] = typeDegrees.incoming();
            if ( loop > 0 )
            {
                degreesArray[degreesArrayIndex++] = loop;
            }
        }
        writeIntDeltas( types, buffer );
        writeInts( Arrays.copyOf( degreesArray, degreesArrayIndex ), buffer );
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

    private void readProperties( MutableIntObjectMap<Value> into, ByteBuffer buffer, SimpleBigValueStore bigPropertyValueStore, PageCursorTracer tracer )
    {
        for ( int propertyKey : readIntDeltas( buffer.array(), buffer.position(), buffer ) )
        {
            into.put( propertyKey, PropertyValueFormat.read( buffer, bigPropertyValueStore, tracer ) );
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

    void deserialize( ByteBuffer buffer,
            SimpleBigValueStore bigPropertyValueStore, PageCursorTracer tracer ) /*temporary, should not be necessary in this scenario*/
    {
        assert buffer.position() == 0 : buffer.position();
        Header headerBuilder = new Header();
        headerBuilder.deserialize( buffer );

        labels.clear();
        properties.clear();
        relationships.clear();
        degrees.clear();

        if ( headerBuilder.hasFlag( Header.FLAG_LABELS ) )
        {
            readLabels( buffer );
        }
        isDense = headerBuilder.hasFlag( Header.FLAG_IS_DENSE );
        if ( headerBuilder.hasOffset( Header.OFFSET_PROPERTIES ) )
        {
            buffer.position( headerBuilder.getOffset( Header.OFFSET_PROPERTIES ) );
            readProperties( properties, buffer, bigPropertyValueStore, tracer );
        }
        if ( headerBuilder.hasOffset( Header.OFFSET_DEGREES ) )
        {
            buffer.position( headerBuilder.getOffset( Header.OFFSET_DEGREES ) );
            readDegrees( buffer );
        }
        if ( headerBuilder.hasOffset( Header.OFFSET_RELATIONSHIPS ) )
        {
            buffer.position( headerBuilder.getOffset( Header.OFFSET_RELATIONSHIPS ) );
            readRelationships( buffer, bigPropertyValueStore, tracer );
        }
        if ( headerBuilder.hasOffset( Header.OFFSET_RECORD_POINTER ) )
        {
            buffer.position( headerBuilder.getOffset( Header.OFFSET_RECORD_POINTER ) );
            readRecordPointer( buffer );
        }
        if ( headerBuilder.hasOffset( Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID ) )
        {
            buffer.position( headerBuilder.getOffset( Header.OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID ) );
            readNextInternalRelationshipId( buffer );
        }
    }

    private void readNextInternalRelationshipId( ByteBuffer buffer )
    {
        nextInternalRelationshipId = readLongs( buffer )[0];
    }

    private void readRecordPointer( ByteBuffer buffer )
    {
        recordPointer = readLongs( buffer )[0];
    }

    private void readRelationships( ByteBuffer buffer, SimpleBigValueStore bigPropertyValueStore, PageCursorTracer tracer )
    {
        int[] relationshipTypes = readIntDeltas( buffer.array(), buffer.position(), buffer );
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
                    readProperties( relationship.properties, buffer, bigPropertyValueStore, tracer );
                }
            }
            this.relationships.put( relationships.type, relationships );
        }
    }

    private void readDegrees( ByteBuffer buffer )
    {
        int[] relationshipTypes = readIntDeltas( buffer );
        int[] degreesArray = readInts( buffer );
        for ( int t = 0, i = 0; t < relationshipTypes.length; t++ )
        {
            i = readDegreesForNextType( degrees, relationshipTypes[t], degreesArray, i );
        }
    }

    static int readDegreesForNextType( Degrees.Mutator degrees, int relationshipType, int[] degreesArray, int degreesArrayIndex )
    {
        int outgoingRaw = degreesArray[degreesArrayIndex++];
        int outgoing = outgoingRaw >>> 1;
        int incoming = degreesArray[degreesArrayIndex++];
        boolean hasLoop = (outgoingRaw & 0x1) == 1;
        int loop = hasLoop ? degreesArray[degreesArrayIndex++] : 0;
        degrees.add( relationshipType, outgoing, incoming, loop );
        return degreesArrayIndex;
    }

    private void readLabels( ByteBuffer buffer )
    {
        labels.addAll( readIntDeltas( buffer ) );
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

    // Record pointer: [    ,    ][iiii,iiii][iiii,iiii][iiii,iiii]  [iiii,iiii][iiii,iiii][iiii,iiii][iiii,iiss]
    // i: id of the record to point to
    // s: size exponential of the record id
    // examples:
    // NULL (-1): no record pointer at all
    // i=5, s=1: record pointer to x2 w/ id 5
    // i=3, s=2: record pointer to x4 w/ id 3
    static int sizeExponentialFromRecordPointer( long recordPointer )
    {
        Preconditions.checkArgument( recordPointer != NULL, "NULL FW pointer" );
        return (int) (recordPointer & 0x3);
    }

    static long idFromRecordPointer( long recordPointer )
    {
        Preconditions.checkArgument( recordPointer != NULL, "NULL FW pointer" );
        return recordPointer >>> 2;
    }

    static long buildRecordPointer( int sizeExp, long id )
    {
        return (id << 2) | sizeExp;
    }

    static String recordPointerToString( long recordPointer )
    {
        if ( recordPointer == NULL )
        {
            return "NULL";
        }
        return String.format( "->[x%d]%d", recordXFactor( sizeExponentialFromRecordPointer( recordPointer ) ), idFromRecordPointer( recordPointer ) );
    }
}
