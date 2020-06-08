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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Values;

@ExtendWith( RandomExtension.class )
class MutableNodeDataTest
{
    private static final long ID = 1;
    private MutableNodeData mutableNodeData;

    @Inject
    private RandomRule random;

    @BeforeEach
    void setup()
    {
        mutableNodeData = new MutableNodeData( ID, null, PageCursorTracer.NULL );
    }

    @Test
    void canWriteAndReadEmptyRecord()
    {
        checkIfDeserializedRecordIsEqual();
    }

    @Test
    void canWriteAndReadLabelRecord()
    {
        mutableNodeData.addLabel( 1 );
        mutableNodeData.addLabel( 2 );
        mutableNodeData.addLabel( 3 );
        checkIfDeserializedRecordIsEqual();
    }

    @Test
    void canWriteAndReadPropertyRecord()
    {
        mutableNodeData.setNodeProperty( 1, Values.intValue( 5 ) );
        mutableNodeData.setNodeProperty( 2, Values.booleanValue( false ) );
        mutableNodeData.setNodeProperty( 3, Values.stringOrNoValue( "foo" ) );
        checkIfDeserializedRecordIsEqual();
    }

    @Test
    void canWriteAndReadRelationshipRecord()
    {
        mutableNodeData.createRelationship( mutableNodeData.nextInternalRelationshipId(), 10, 2, true );
        mutableNodeData.createRelationship( mutableNodeData.nextInternalRelationshipId(), 11, 2, true );
        mutableNodeData.createRelationship( mutableNodeData.nextInternalRelationshipId(), 12, 3, true );
        checkIfDeserializedRecordIsEqual();
    }

    @Test
    void canWriteAndReadRelationshipRecordMultipleTimes()
    {
        mutableNodeData.createRelationship( mutableNodeData.nextInternalRelationshipId(), 10, 2, true );
        checkIfDeserializedRecordIsEqual();
        MutableNodeData.Relationship rel = new MutableNodeData.Relationship( 7, 11, ID, 2, true );
        mutableNodeData.createRelationship( rel.internalId, 11, 2, false );
        checkIfDeserializedRecordIsEqual();
        mutableNodeData.createRelationship( mutableNodeData.nextInternalRelationshipId(), 12, 3, true );
        checkIfDeserializedRecordIsEqual();
    }

    @Test
    void canWriteAndReadRelationshipWithPropertyRecord()
    {
        MutableNodeData.Relationship r1 = mutableNodeData.createRelationship( mutableNodeData.nextInternalRelationshipId(), 10, 2, true );
        r1.addProperty( 1, Values.intValue( 5 ) );
        r1.addProperty( 2, Values.booleanValue( false ) );
        r1.addProperty( 3, Values.stringOrNoValue( "foo" ) );
        checkIfDeserializedRecordIsEqual();

        MutableNodeData.Relationship r2 = mutableNodeData.createRelationship( mutableNodeData.nextInternalRelationshipId(), 11, 2, true );
        r2.addProperty( 4, Values.intArray( new int[]{123, 4, -56} ) );
        r2.addProperty( 5, Values.pointValue( CoordinateReferenceSystem.Cartesian, 3.0,4.0 ) );
        checkIfDeserializedRecordIsEqual();

        MutableNodeData.Relationship r3 = mutableNodeData.createRelationship( mutableNodeData.nextInternalRelationshipId(), 12, 3, true );
        r3.addProperty( 6, Values.charValue( 'f' ) );
        r3.addProperty( 7, Values.charValue( 'o' ) );
        r3.addProperty( 8, Values.charValue( 'o' ) );
        checkIfDeserializedRecordIsEqual();
    }

    @Test
    void canWriteAndReadRecordWithLabelsPropertiesRelationships()
    {
        mutableNodeData.addLabel( 1 );
        mutableNodeData.addLabel( 2 );
        mutableNodeData.addLabel( 3 );
        checkIfDeserializedRecordIsEqual();

        mutableNodeData.setNodeProperty( 1, Values.intValue( 5 ) );
        checkIfDeserializedRecordIsEqual();

        mutableNodeData.setNodeProperty( 2, Values.booleanValue( false ) );
        checkIfDeserializedRecordIsEqual();

        mutableNodeData.setNodeProperty( 3, Values.stringOrNoValue( "foo" ) );
        checkIfDeserializedRecordIsEqual();

        MutableNodeData.Relationship r1 = mutableNodeData.createRelationship( mutableNodeData.nextInternalRelationshipId(), 10, 2, true );
        r1.addProperty( 1, Values.intValue( 5 ) );
        r1.addProperty( 2, Values.booleanValue( false ) );
        r1.addProperty( 3, Values.stringOrNoValue( "foo" ) );
        checkIfDeserializedRecordIsEqual();

        MutableNodeData.Relationship r2 = mutableNodeData.createRelationship( mutableNodeData.nextInternalRelationshipId(), 12, 3, true );
        r2.addProperty( 6, Values.charValue( 'f' ) );
        r2.addProperty( 7, Values.charValue( 'o' ) );
        r2.addProperty( 8, Values.charValue( 'o' ) );
        checkIfDeserializedRecordIsEqual();

        mutableNodeData.removeLabel( 1 );
        mutableNodeData.removeLabel( 2 );
        mutableNodeData.removeLabel( 3 );
        mutableNodeData.addLabel( 4 );
        checkIfDeserializedRecordIsEqual();
    }

    @Test
    void canDeleteRelationship()
    {
        // given
        List<MutableNodeData.Relationship> expectedRelationships = new ArrayList<>();
        for ( int i = 0; i < 20; i++ )
        {
            boolean outgoing = random.nextBoolean();
            long internalId = outgoing ? mutableNodeData.nextInternalRelationshipId() : MutableNodeData.FIRST_RELATIONSHIP_ID;
            expectedRelationships.add( mutableNodeData.createRelationship( internalId, random.nextInt( 10 ), random.nextInt( 4 ), outgoing ) );
        }
        checkIfDeserializedRecordIsEqual();

        // when
        MutableNodeData.Relationship deletedRelationship = expectedRelationships.remove( random.nextInt( expectedRelationships.size() ) );
        mutableNodeData.deleteRelationship( deletedRelationship.internalId, deletedRelationship.type, deletedRelationship.otherNode,
                deletedRelationship.outgoing, value -> {} );
        if ( deletedRelationship.outgoing && deletedRelationship.internalId == mutableNodeData.getNextInternalRelationshipId() - 1 )
        {
            // If we delete the last relationship then the 'nextRelationshipId' on next load will be one less, therefore decrement the expected here
            mutableNodeData.setNextInternalRelationshipId( mutableNodeData.getNextInternalRelationshipId() - 1 );
        }

        // then
        checkIfDeserializedRecordIsEqual();
    }

    @Test
    void canHaveDegreesForDenseNode()
    {
        // when
        mutableNodeData.addDegrees( 3, 33, 333, 3333 );
        mutableNodeData.addDegrees( 5, 55, 555, 5555 );

        // then
        checkIfDeserializedRecordIsEqual();
    }

    @Test
    void canHaveZeroLoopDegreesForDenseNode()
    {
        // when
        mutableNodeData.addDegrees( 3, 1234, 5678, 0 );
        mutableNodeData.addDegrees( 5, 4321, 21, 0 );

        // then
        checkIfDeserializedRecordIsEqual();
    }

    private void checkIfDeserializedRecordIsEqual()
    {
//        //Given
//        ByteBuffer[] buffers = new ByteBuffer[10];
//        for ( int i = 0; i < buffers.length; i++ )
//        {
//            buffers[i] = ByteBuffer.wrap( new byte[256] );
//        }
//        mutableNodeRecordData.serializeMainData( buffers, null, null );
//
//        //When
//        Stream.of( buffers ).forEach( ByteBuffer::flip );
//        MutableNodeRecordData after = new MutableNodeRecordData( ID, null );
//        after.deserialize( buffer, null, null );
//
//        //Then
//        assertThat( mutableNodeRecordData ).isEqualTo( after );
    }
}
