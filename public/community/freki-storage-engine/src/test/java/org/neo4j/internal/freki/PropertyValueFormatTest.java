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
import java.util.Map;

import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PropertyValueFormatTest
{

    private final byte[] data = new byte[1024]; // should be enough to hold any properties we write in those tests
    private final ByteBuffer readBuffer = ByteBuffer.wrap( data);
    private final ByteBuffer writeBuffer = ByteBuffer.wrap( data );
    private final PropertyValueFormat propertyValueFormat = new PropertyValueFormat( writeBuffer );

    @BeforeEach
    void setUp()
    {
        readBuffer.clear();
        writeBuffer.clear();
    }

    @Test
    void shouldWriteAndReadPositiveIntegerScalars()
    {
        //Given
        int[] values = {0, 1, 56, 212, 541,10022, 321123, 4323425, 312314512, 2073432112 };

        //When
        for ( int value : values )
        {
            propertyValueFormat.writeInteger( value );
        }

        //Then
        for ( int value : values )
        {
            assertEquals( Values.intValue( value ), readValue() );
        }
    }

    @Test
    void shouldWriteAndReadNegativeIntegerScalars()
    {
        //Given
        int[] values = {-0, -5, -150, -6452, -31223, -87667 };

        //When
        for ( int value : values )
        {
            propertyValueFormat.writeInteger( value );
        }

        //Then
        for ( int value : values )
        {
            assertEquals( Values.intValue( value ), readValue() );
        }
    }

    @Test
    void shouldPackScalarsTightly()
    {
        //Given
        Map<Integer,Integer> scalarSizeMap = Map.of(
                6, 2,
                200, 3,
                10_000, 3,
                500_000_000, 5
        );

        //Then
        scalarSizeMap.forEach( ( scalar, size ) ->
        {
            //Type takes 1B, rest is packed in 1-4B
            propertyValueFormat.writeInteger( scalar );
            assertEquals( size, writeBuffer.position() );
            writeBuffer.clear();

            //Negative value should be packed on equal size
            propertyValueFormat.writeInteger( -scalar );
            assertEquals( size, writeBuffer.position() );
            writeBuffer.clear();
        });
    }

    @Test
    void shouldWriteAndReadLongs()
    {
        //Given
        long[] values = {0, 10, -1000, 10000000000L, -100000000000000L, 1000000000000000000L };

        //When
        for ( long value : values )
        {
            propertyValueFormat.writeInteger( value );
        }

        //Then
        for ( long value : values )
        {
            assertEquals( Values.longValue( value ), readValue() );
        }

    }

    @Test
    void shouldWriteAndReadStrings()
    {
        //When
        propertyValueFormat.writeString( "abc" );
        propertyValueFormat.writeString( "" );
        propertyValueFormat.writeString( "A B C D E F G H I J K L M N O P Q R S T U V W X Y Z" );

        //Then
        assertEquals( Values.stringValue( "abc" ), readValue() );
        assertEquals( Values.stringValue( "" ), readValue() );
        assertEquals( Values.stringValue( "A B C D E F G H I J K L M N O P Q R S T U V W X Y Z" ), readValue() );

    }

    private Value readValue()
    {
        return PropertyValueFormat.read( readBuffer );
    }


}