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
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;

import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueWriter;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void shouldWriteAndReadBooleans()
    {
        //When
        propertyValueFormat.writeBoolean( true );
        propertyValueFormat.writeBoolean( false );

        //Then
        assertEquals( Values.booleanValue( true ), readValue() );
        assertEquals( Values.booleanValue( false ), readValue() );
    }

    @Test
    void shouldFitBooleansInSingleByte()
    {
        //When
        propertyValueFormat.writeBoolean( true );

        //Then
        assertEquals( Values.booleanValue( true ), readValue() );
        assertEquals( 1, writeBuffer.position() );

    }

    @Test
    void shouldWriteAndReadShortScalars()
    {
        //Given
        short[] values = {0, 1, 56, 212, 541,10022, 31000 };

        //When
        for ( short value : values )
        {
            propertyValueFormat.writeInteger( value );
            propertyValueFormat.writeInteger( (short) -value );
        }

        //Then
        for ( short value : values )
        {
            assertEquals( Values.shortValue( value ), readValue() );
            assertEquals( Values.shortValue( (short) -value ), readValue() );
        }
    }

    @Test
    void shouldPackShortScalarsTightly()
    {
        //Given
        Map<Short,Integer> scalarSizeMap = Map.of(
                (short) 100, 2,
                (short) 10000, 3
        );

        //Then
        scalarSizeMap.forEach( ( scalar, size ) ->
        {
            //Type takes 1B, rest is packed in 1-2B
            propertyValueFormat.writeInteger( scalar );
            assertEquals( size, writeBuffer.position() );
            writeBuffer.clear();

            //Negative value should be packed on equal size
            propertyValueFormat.writeInteger( (short) -scalar );
            assertEquals( size, writeBuffer.position() );
            writeBuffer.clear();
        });
    }

    @Test
    void shouldWriteAndReadBytes()
    {
        //Given
        byte[] values = {1, -5, 100, -128 };

        //When
        for ( byte value : values )
        {
            propertyValueFormat.writeInteger( value );
        }

        //Then
        for ( byte value : values )
        {
            assertEquals( Values.byteValue( value ), readValue() );
        }
    }

    @Test
    void shouldReadAndWriteFloatValues()
    {
        //Given
        float[] values = {1.2345f, -51235.234f, 12345678.9f, -50000f };

        //When
        for ( float value : values )
        {
            propertyValueFormat.writeFloatingPoint( value );
        }

        //Then
        for ( float value : values )
        {
            assertEquals( Values.floatValue( value ), readValue() );
        }
    }

    @Test
    void shouldReadAndWriteDoubleValues()
    {
        //Given
        double[] values = {1.2345, -51235.234, 12345678.9, -50000.321, 9090909090909090909090909090909090909090909090909090909090909090900.90909090909090d };

        //When
        for ( double value : values )
        {
            propertyValueFormat.writeFloatingPoint( value );
        }

        //Then
        for ( double value : values )
        {
            assertEquals( Values.doubleValue( value ), readValue() );
        }
    }

    @Test
    void shouldWriteAndReadChars()
    {
        //When
        propertyValueFormat.writeString( '1' );
        propertyValueFormat.writeString( 'a' );
        propertyValueFormat.writeString( '&' );
        propertyValueFormat.writeString( ' ' );

        //Then
        assertEquals( Values.charValue( '1' ), readValue() );
        assertEquals( Values.charValue( 'a' ), readValue() );
        assertEquals( Values.charValue( '&' ), readValue() );
        assertEquals( Values.charValue( ' ' ), readValue() );
    }

    @Test
    void shouldWriteAndReadArrays()
    {
        char[] array = {'a', 'b', 'c'};
        propertyValueFormat.beginArray( array.length, ValueWriter.ArrayType.CHAR );
        for ( char c : array )
        {
            propertyValueFormat.writeString( c );
        }
        propertyValueFormat.endArray();

        assertEquals( Values.charArray( array ), readValue() );
    }

    @Test
    void shouldWriteAndReadMultipleArraysOfSameType()
    {
        int[][] arrays = {{1, 2, 3}, {4, 5, 6, 7}};
        for ( int[] array : arrays )
        {
            propertyValueFormat.beginArray( array.length, ValueWriter.ArrayType.INT );
            for ( int i : array )
            {
                propertyValueFormat.writeInteger( i );
            }
            propertyValueFormat.endArray();
        }

        assertEquals( Values.intArray( arrays[0] ), readValue() );
        assertEquals( Values.intArray( arrays[1] ), readValue() );
    }

    @Test
    void shouldReadAndWriteEmptyArray()
    {
        propertyValueFormat.beginArray( 0, ValueWriter.ArrayType.INT );
        propertyValueFormat.endArray();

        assertEquals( Values.intArray( new int[0] ), readValue() );

    }

    @Test
    void shouldFailWhenWritingArraysWithMismatchingTypes()
    {
        propertyValueFormat.beginArray( 2, ValueWriter.ArrayType.FLOAT );
        assertThrows( UnsupportedOperationException.class, () -> propertyValueFormat.writeInteger( 5 ) );
    }

    @Test
    void shouldFailWhenInterleavingArrays()
    {
        propertyValueFormat.beginArray( 2, ValueWriter.ArrayType.FLOAT );
        assertThrows( AssertionError.class, () ->  propertyValueFormat.beginArray( 7, ValueWriter.ArrayType.LONG ) );
    }


    @Test
    void shouldFailWhenNotFillingArray()
    {
        propertyValueFormat.beginArray( 2, ValueWriter.ArrayType.INT );
        propertyValueFormat.writeInteger( 5 );

        assertThrows( IllegalStateException.class, propertyValueFormat::endArray );
    }

    @Test
    void shouldFailWhenOverflowingArray()
    {
        propertyValueFormat.beginArray( 2, ValueWriter.ArrayType.INT );
        propertyValueFormat.writeInteger( 5 );
        propertyValueFormat.writeInteger( 10 );
        assertThrows( IndexOutOfBoundsException.class, () -> propertyValueFormat.writeInteger( 5 ) );
    }

    @Test
    void shouldCorrectlyCalculateSizeOfArray()
    {
        long[] array = {8, 500, 1231234323L, 34123,53, -543234, 1900 };
        propertyValueFormat.beginArray( array.length, ValueWriter.ArrayType.LONG );
        for ( long i : array )
        {
            propertyValueFormat.writeInteger( i );
        }
        propertyValueFormat.endArray();

        assertEquals( writeBuffer.position(), PropertyValueFormat.calculatePropertyValueSizeIncludingTypeHeader( readBuffer ) );
    }

    @Test
    void shouldSupportArraysOfPropertiesUsingNestedProperties()
    {
        propertyValueFormat.beginArray( 2, ValueWriter.ArrayType.ZONED_DATE_TIME ); //ZonedTime currently writes multiple int properties
        propertyValueFormat.writeDateTime( 867000000000000L, 123456789, -3600 );
        propertyValueFormat.writeDateTime( 541235431L, 984345, "Europe/Stockholm" );

        propertyValueFormat.endArray();
        assertEquals( Values.dateTimeArray( new ZonedDateTime[] {
                DateTimeValue.datetimeRaw( 867000000000000L, 123456789, ZoneOffset.ofTotalSeconds( -3600 ) ),
                DateTimeValue.datetimeRaw( 541235431L, 984345, ZoneId.of( "Europe/Stockholm" ) )
        } ), readValue() );
    }

    @Test
    void shouldWriteAndReadDuration()
    {
        propertyValueFormat.writeDuration( 1, 0, 0, 0 );
        propertyValueFormat.writeDuration( 2, 3, 0, 0 );
        propertyValueFormat.writeDuration( 4, 0, 5, 0 );
        propertyValueFormat.writeDuration( 0, 6, 5, 7 );
        propertyValueFormat.writeDuration( 0, 0, 0, 0 );
        propertyValueFormat.writeDuration( 13, 14, 432, 999 );
        propertyValueFormat.writeDuration( 13, 14, 100000, 999 );

        assertEquals( DurationValue.duration( 1, 0 , 0 ,0 ), readValue() );
        assertEquals( DurationValue.duration( 2, 3 , 0 ,0 ), readValue() );
        assertEquals( DurationValue.duration( 4, 0 , 5 ,0 ), readValue() );
        assertEquals( DurationValue.duration( 0, 6 , 5 ,7 ), readValue() );
        assertEquals( DurationValue.duration( 0, 0 , 0 ,0 ), readValue() );
        assertEquals( DurationValue.duration( 13, 14 , 432 ,999 ), readValue() );
        assertEquals( DurationValue.duration( 13, 14 , 100000 ,999 ), readValue() );
    }

    @Test
    void shouldCorrectlyCalculateSizeOfDuration()
    {
        propertyValueFormat.writeDuration( 13, 14, 100000, 999 );
        assertEquals( writeBuffer.position(), PropertyValueFormat.calculatePropertyValueSizeIncludingTypeHeader( readBuffer ) );
    }

    @Test
    void shouldWriteAndReadPoints()
    {
        propertyValueFormat.writePoint( CoordinateReferenceSystem.Cartesian, new double[]{1.0, 32.2d} );
        propertyValueFormat.writePoint( CoordinateReferenceSystem.Cartesian_3D, new double[]{-213.d, 102.2d, 60} );
        propertyValueFormat.writePoint( CoordinateReferenceSystem.WGS84, new double[]{30, 45} );
        propertyValueFormat.writePoint( CoordinateReferenceSystem.WGS84_3D, new double[]{30, 60, 90} );

        assertEquals( Values.pointValue( CoordinateReferenceSystem.Cartesian, 1.0, 32.2d ), readValue() );
        assertEquals( Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, -213.d, 102.2d, 60 ), readValue() );
        assertEquals( Values.pointValue( CoordinateReferenceSystem.WGS84, 30, 45 ), readValue() );
        assertEquals( Values.pointValue( CoordinateReferenceSystem.WGS84_3D, 30, 60, 90 ), readValue() );
    }

    @Test
    void shouldCorrectlyCalculateSizeOfPoint()
    {
        propertyValueFormat.writePoint( CoordinateReferenceSystem.Cartesian, new double[]{1.0, 32.2d} );
        assertEquals( writeBuffer.position(), PropertyValueFormat.calculatePropertyValueSizeIncludingTypeHeader( readBuffer ) );
    }

    @Test
    void shouldWriteAndReadDate()
    {
        propertyValueFormat.writeDate( 100 );
        propertyValueFormat.writeDate( 18250 );

        assertEquals( DateValue.epochDate( 100 ), readValue() );
        assertEquals( DateValue.epochDate( 18250 ), readValue() );
    }

    @Test
    void shouldWriteAndReadLocalTime()
    {
        propertyValueFormat.writeLocalTime( 0 );
        propertyValueFormat.writeLocalTime( 5_000_000_000_000L );

        assertEquals( LocalTimeValue.localTime( 0 ), readValue() );
        assertEquals( LocalTimeValue.localTime( 5_000_000_000_000L ), readValue() );
    }

    @Test
    void shouldWriteAndReadTime()
    {
        propertyValueFormat.writeTime( 0, 7200 );
        propertyValueFormat.writeTime( 2000, -3600 );

        assertEquals( TimeValue.time( 0, ZoneOffset.ofTotalSeconds( 7200 ) ), readValue() );
        assertEquals( TimeValue.time( 2000, ZoneOffset.ofTotalSeconds( -3600 ) ), readValue() );
    }

    @Test
    void shouldCorrectlyCalculateSizeOfTime()
    {
        propertyValueFormat.writeTime( 2000, -3600 );
        assertEquals( writeBuffer.position(), PropertyValueFormat.calculatePropertyValueSizeIncludingTypeHeader( readBuffer ) );
    }

    @Test
    void shouldWriteAndReadLocalDateTime()
    {
        propertyValueFormat.writeLocalDateTime( 867000000000000L, 123456789 );

        assertEquals( LocalDateTimeValue.localDateTime( 867000000000000L, 123456789 ), readValue() );
    }

    @Test
    void shouldCorrectlyCalculateSizeOfLocalDateTime()
    {
        propertyValueFormat.writeLocalDateTime( 867000000000000L, 123456789 );
        assertEquals( writeBuffer.position(), PropertyValueFormat.calculatePropertyValueSizeIncludingTypeHeader( readBuffer ) );
    }

    @Test
    void shouldWriteAndReadDateTime()
    {
        propertyValueFormat.writeDateTime( 867000000000000L, 123456789, -3600 );
        propertyValueFormat.writeDateTime( 541235431L, 984345, "Europe/Stockholm" );

        assertEquals( DateTimeValue.datetime( 867000000000000L, 123456789, ZoneOffset.ofTotalSeconds( -3600 ) ), readValue() );
        assertEquals( DateTimeValue.datetime( 541235431L, 984345, ZoneId.of( "Europe/Stockholm" ) ), readValue() );
    }

    @Test
    void shouldCorrectlyCalculateSizeOfDateTime()
    {
        propertyValueFormat.writeDateTime( 867000000000000L, 123456789, -3600 );
        assertEquals( writeBuffer.position(), PropertyValueFormat.calculatePropertyValueSizeIncludingTypeHeader( readBuffer ) );
    }

    private Value readValue()
    {
        return PropertyValueFormat.read( readBuffer );
    }


}