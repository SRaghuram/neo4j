/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.cypher.internal.runtime.InputCursor;
import org.neo4j.cypher.internal.runtime.InputDataStream;
import org.neo4j.fabric.stream.InputDataStreamImpl;
import org.neo4j.fabric.stream.Record;
import org.neo4j.fabric.stream.Rx2SyncStream;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class InputDataStreamTest
{

    @Test
    void testInputDataStream()
    {
        Rx2SyncStream recordStream = mock( Rx2SyncStream.class );
        Record r1 = record( "v1", "v2", "v3" );
        Record r2 = record( "v4", "v5", "v6" );
        Record r3 = record( "v7", "v8", "v9" );
        when( recordStream.readRecord() ).thenReturn( r1, r2, r3, null );

        InputDataStream inputDataStream = new InputDataStreamImpl( recordStream );

        InputCursor inputCursor = inputDataStream.nextInputBatch();
        assertNotNull( inputCursor );

        assertTrue( inputCursor.next() );
        assertValue( "v1", inputCursor.value( 0 ) );
        assertValue( "v2", inputCursor.value( 1 ) );
        assertValue( "v3", inputCursor.value( 2 ) );

        assertTrue( inputCursor.next() );
        assertValue( "v4", inputCursor.value( 0 ) );
        assertValue( "v5", inputCursor.value( 1 ) );
        assertValue( "v6", inputCursor.value( 2 ) );

        assertTrue( inputCursor.next() );
        assertValue( "v7", inputCursor.value( 0 ) );
        assertValue( "v8", inputCursor.value( 1 ) );
        assertValue( "v9", inputCursor.value( 2 ) );

        assertFalse( inputCursor.next() );
        assertNull( inputDataStream.nextInputBatch() );
    }

    private Record record( String... values )
    {
        List<AnyValue> neoValues = Arrays.stream( values ).map( Values::stringValue ).collect( Collectors.toList() );

        return new Record()
        {
            @Override
            public AnyValue getValue( int offset )
            {
                return neoValues.get( offset );
            }

            @Override
            public int size()
            {
                return neoValues.size();
            }
        };
    }

    private void assertValue( String expected, AnyValue value )
    {
        assertEquals( Values.stringValue( expected ), value );
    }

}
