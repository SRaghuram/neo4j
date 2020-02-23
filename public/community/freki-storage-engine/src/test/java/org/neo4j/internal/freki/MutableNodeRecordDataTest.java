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

import java.nio.ByteBuffer;

import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MutableNodeRecordDataTest
{
    private static final long ID = 1;
    private MutableNodeRecordData record;

    @BeforeEach
    void setup()
    {
        record = new MutableNodeRecordData( ID );
    }

    @Test
    void canWriteAndReadEmptyRecord()
    {
        checkIfDeserializedRecordIsEqual();
    }

    @Test
    void canWriteAndReadLabelRecord()
    {
        record.labels.addAll( 1, 2, 3 );
        checkIfDeserializedRecordIsEqual();
    }

    @Test
    void canWriteAndReadPropertyRecord()
    {
        record.setNodeProperty( 1, Values.intValue( 5 ) );
        record.setNodeProperty( 2, Values.booleanValue( false ) );
        record.setNodeProperty( 3, Values.stringOrNoValue( "foo" ) );
        checkIfDeserializedRecordIsEqual();
    }

    @Test
    void canWriteAndReadRelationshipRecord()
    {
        record.createRelationship( null, 10, 2 );
        record.createRelationship( null, 11, 2 );
        record.createRelationship( null, 12, 3 );
        checkIfDeserializedRecordIsEqual();
    }

    @Test
    void canWriteAndReadRelationshipRecordMultipleTimes()
    {
        record.createRelationship( null, 10, 2 );
        checkIfDeserializedRecordIsEqual();
        MutableNodeRecordData.Relationship rel = new MutableNodeRecordData.Relationship( 7, 11, ID, 2, true );
        record.createRelationship( rel, 11, 2 );
        checkIfDeserializedRecordIsEqual();
        record.createRelationship( null, 12, 3 );
        checkIfDeserializedRecordIsEqual();
    }

    @Test
    void canWriteAndReadRelationshipWithPropertyRecord()
    {
        MutableNodeRecordData.Relationship r1 = record.createRelationship( null, 10, 2 );
        r1.addProperty( 1, Values.intValue( 5 ) );
        r1.addProperty( 2, Values.booleanValue( false ) );
        r1.addProperty( 3, Values.stringOrNoValue( "foo" ) );
        checkIfDeserializedRecordIsEqual();

        MutableNodeRecordData.Relationship r2 = record.createRelationship( null, 11, 2 );
        r2.addProperty( 4, Values.intArray( new int[]{123, 4, -56} ) );
        r2.addProperty( 5, Values.pointValue( CoordinateReferenceSystem.Cartesian, 3.0,4.0 ) );
        checkIfDeserializedRecordIsEqual();

        MutableNodeRecordData.Relationship r3 = record.createRelationship( null, 12, 3 );
        r3.addProperty( 6, Values.charValue( 'f' ) );
        r3.addProperty( 7, Values.charValue( 'o' ) );
        r3.addProperty( 8, Values.charValue( 'o' ) );
        checkIfDeserializedRecordIsEqual();
    }

    @Test
    void canWriteAndReadRecordWithLabelsPropertiesRelationships()
    {
        record.labels.addAll( 1, 2, 3 );
        checkIfDeserializedRecordIsEqual();

        record.setNodeProperty( 1, Values.intValue( 5 ) );
        checkIfDeserializedRecordIsEqual();

        record.setNodeProperty( 2, Values.booleanValue( false ) );
        checkIfDeserializedRecordIsEqual();

        record.setNodeProperty( 3, Values.stringOrNoValue( "foo" ) );
        checkIfDeserializedRecordIsEqual();

        MutableNodeRecordData.Relationship r1 = record.createRelationship( null, 10, 2 );
        r1.addProperty( 1, Values.intValue( 5 ) );
        r1.addProperty( 2, Values.booleanValue( false ) );
        r1.addProperty( 3, Values.stringOrNoValue( "foo" ) );
        checkIfDeserializedRecordIsEqual();

        MutableNodeRecordData.Relationship r2 = record.createRelationship( null, 12, 3 );
        r2.addProperty( 6, Values.charValue( 'f' ) );
        r2.addProperty( 7, Values.charValue( 'o' ) );
        r2.addProperty( 8, Values.charValue( 'o' ) );
        checkIfDeserializedRecordIsEqual();

        record.labels.clear();
        record.labels.add( 4 );
        checkIfDeserializedRecordIsEqual();
    }

    private void checkIfDeserializedRecordIsEqual()
    {
        //Given
        ByteBuffer buffer = ByteBuffer.wrap( new byte[256] );
        record.serialize( buffer, null, null );

        //When
        buffer.position( 0 );
        MutableNodeRecordData after = new MutableNodeRecordData( ID );
        after.deserialize( buffer, null );

        //Then
        assertEquals( record, after );
    }
}
