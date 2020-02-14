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

import org.apache.commons.lang3.NotImplementedException;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAmount;

import org.neo4j.string.UTF8;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.TimeZones;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.utils.TemporalValueWriterAdapter;

import static org.neo4j.internal.helpers.Numbers.safeCastIntToUnsignedByte;

class PropertyValueFormat extends TemporalValueWriterAdapter<RuntimeException>
{
    // Header 1B [x___,____]
    //  x -> 0 = simple inlined value
    //      [0iii,tttt][data]
    //      i - internal data tied to type
    //      t - external type to read
    //  x -> 1 = special type
    //      [1_pa_,tttt]
    //      p - inlined
    //      a - array
    //      t - type
    //      p -> 1 = pointer data
    //          [11__,tttt][pointer]
    //          p - pointer to data
    //      a -> 1
    //          [1_1_,tttt][length][data]
    private static final byte EXTERNAL_TYPE_BOOL = 0;
    private static final byte EXTERNAL_TYPE_BYTE = 1;
    private static final byte EXTERNAL_TYPE_SHORT = 2;
    private static final byte EXTERNAL_TYPE_INT = 3;
    private static final byte EXTERNAL_TYPE_LONG = 4;
    private static final byte EXTERNAL_TYPE_FLOAT = 5;
    private static final byte EXTERNAL_TYPE_DOUBLE = 6;
    private static final byte EXTERNAL_TYPE_STRING = 7;
    private static final byte EXTERNAL_TYPE_CHAR = 8;
    private static final byte EXTERNAL_TYPE_POINT = 9;
    private static final byte EXTERNAL_TYPE_ZONED_DATE_TIME = 10;
    private static final byte EXTERNAL_TYPE_LOCAL_DATE_TIME = 11;
    private static final byte EXTERNAL_TYPE_DATE = 12;
    private static final byte EXTERNAL_TYPE_ZONED_TIME = 13;
    private static final byte EXTERNAL_TYPE_LOCAL_TIME = 14;
    private static final byte EXTERNAL_TYPE_DURATION = 15;

    private static final byte INTERNAL_SCALAR_TYPE_INT_0 = 0;
    private static final byte INTERNAL_SCALAR_TYPE_INT_8 = 1;
    private static final byte INTERNAL_SCALAR_TYPE_INT_16 = 2;
    private static final byte INTERNAL_SCALAR_TYPE_INT_32 = 3;
    private static final byte INTERNAL_SCALAR_TYPE_INT_64 = 4;

    private static final byte INTERNAL_BOOL_FALSE = 0;
    private static final byte INTERNAL_BOOL_TRUE = 1;

    private static final byte INTERNAL_DURATION_HAS_NANOS = 0x10;
    private static final byte INTERNAL_DURATION_HAS_DAYS = 0x20;
    private static final byte INTERNAL_DURATION_HAS_MONTHS = 0x40;

    private static final short SPECIAL_TYPE_MASK = 0x80;
    private static final short SPECIAL_TYPE_POINTER = 0x20;
    private static final short SPECIAL_TYPE_ARRAY = 0x10;

    private final ByteBuffer buffer;

    //Array state
    private boolean writingArray;
    private byte currentArrayType = -1;
    private int currentArrayElementsLeft = -1;
    //Nested properties
    private int nestedPropertyCount;

    PropertyValueFormat( ByteBuffer buffer )
    {
        this.buffer = buffer;
    }

    private static byte externalType( byte typeByte )
    {
        return (byte) (typeByte & 0xF);
    }

    private static byte internalType( byte typeByte )
    {
        return (byte) ((typeByte & 0x70) >> 4);
    }

    private static byte createTypeByte( byte externalType, byte internalType )
    {
        return (byte) (externalType | (internalType << 4));
    }

    @Override
    public void writeNull() throws IllegalArgumentException
    {
        throw new IllegalArgumentException( "Cannot write null values to the property store" );
    }

    private void beginWriteProperty( byte externalType )
    {
        if ( nestedPropertyCount++ == 0 && writingArray )
        {
            if ( externalType != currentArrayType )
            {
                throw new UnsupportedOperationException( "Array type mismatch. Got " + externalType + " want " + currentArrayType );
            }
            if ( --currentArrayElementsLeft < 0 )
            {
                throw new IndexOutOfBoundsException( "To many elements in array" );
            }
        }
    }

    private void endWriteProperty()
    {
        nestedPropertyCount--;
        assert nestedPropertyCount >= 0;
    }

    @Override
    public void beginArray( int size, ArrayType arrayType )
    {
        //TODO optimize array writing size (esp. byte and char)
        assert !writingArray;
        byte externalType = getExternalType( arrayType );
        buffer.put( (byte) ( externalType | SPECIAL_TYPE_MASK | SPECIAL_TYPE_ARRAY) );
        writeInteger( size ); //write length first, before setting array state

        currentArrayType = externalType;
        writingArray = true;
        currentArrayElementsLeft = size;
    }

    @Override
    public void endArray()
    {
        assert writingArray;
        if ( currentArrayElementsLeft != 0 )
        {
            throw new IllegalStateException( "Did not fill array with data" );
        }
        writingArray = false;
        currentArrayType = -1;
        currentArrayElementsLeft = -1;
    }

    @Override
    public void writeByteArray( byte[] value )
    {
        beginArray( value.length, ArrayType.BYTE );
        buffer.put( value );
        currentArrayElementsLeft = 0;
        endArray();
    }

    @Override
    public void writeFloatingPoint( float value )
    {
        beginWriteProperty( EXTERNAL_TYPE_FLOAT );
        writeIntBits( Float.floatToIntBits( value ), EXTERNAL_TYPE_FLOAT );
        endWriteProperty();
    }

    @Override
    public void writeFloatingPoint( double value )
    {
        beginWriteProperty( EXTERNAL_TYPE_DOUBLE );
        writeLongBits( Double.doubleToRawLongBits( value), EXTERNAL_TYPE_DOUBLE );
        endWriteProperty();
    }

    @Override
    public void writePoint( CoordinateReferenceSystem crs, double[] coordinate )
    {
        beginWriteProperty( EXTERNAL_TYPE_POINT );
        int tableId = crs.getTable().getTableId();
        assert tableId >= 0 && tableId <= 0x7; //fits in 3 bits
        buffer.put( (byte) (EXTERNAL_TYPE_POINT | (tableId << 4)) );
        writeInteger( crs.getCode() );
        for ( double coord : coordinate )
        {
            writeFloatingPoint( coord );
        }
        endWriteProperty();
    }

    private static PointValue readPoint( ByteBuffer buffer, byte typeByte )
    {
        int code = (int) read( buffer ).asObject();
        CoordinateReferenceSystem crs = CoordinateReferenceSystem.get( (typeByte >>> 4) & 0x7, code );
        double[] dimensions = new double[crs.getDimension()];
        for ( int i = 0; i < dimensions.length; i++ )
        {
            dimensions[i] = (double) read( buffer ).asObject();
        }
        return Values.pointValue( crs, dimensions );
    }

    private static int sizeOfPoint( byte typeByte, ByteBuffer buffer )
    {
        int start = buffer.position();
        int code = (int) read( buffer.position( start + 1 ) ).asObject();
        int dimension = CoordinateReferenceSystem.get( (typeByte >>> 4) & 0x7, code ).getDimension();
        int size = buffer.position() - start;
        for ( int i = 0; i < dimension; i++ )
        {
            size += calculatePropertyValueSizeIncludingTypeHeader( buffer.position( start + size ) );
        }
        buffer.position( start );
        return size;
    }

    @Override
    public void writeDuration( long months, long days, long seconds, int nanos )
    {
        //TODO this can surely be better packed, using 1 extra header byte
        beginWriteProperty( EXTERNAL_TYPE_DURATION );
        buffer.put( (byte) (EXTERNAL_TYPE_DURATION
                | (months != 0 ? INTERNAL_DURATION_HAS_MONTHS : 0)
                | (days != 0 ? INTERNAL_DURATION_HAS_DAYS : 0)
                | (nanos != 0 ? INTERNAL_DURATION_HAS_NANOS : 0) ) );

        if ( months != 0 )
        {
            writeInteger( months );
        }
        if ( days != 0 )
        {
            writeInteger( days );
        }
        writeInteger( seconds );
        if ( nanos != 0 )
        {
            writeInteger( nanos );
        }
        endWriteProperty();
    }

    private static DurationValue readDuration( ByteBuffer buffer, byte typeByte )
    {
        long months = 0;
        long days = 0;
        long seconds;
        int nanos = 0;

        if ( (typeByte & INTERNAL_DURATION_HAS_MONTHS) != 0 )
        {
            months = (long) read( buffer ).asObject();
        }
        if ( (typeByte & INTERNAL_DURATION_HAS_DAYS) != 0 )
        {
            days = (long) read( buffer ).asObject();
        }
        seconds = (long) read( buffer ).asObject();
        if ( (typeByte & INTERNAL_DURATION_HAS_NANOS) != 0 )
        {
            nanos = (int) read( buffer ).asObject();
        }

        return DurationValue.duration( months, days, seconds, nanos);
    }

    private static int sizeOfDuration( byte typeByte, ByteBuffer buffer )
    {
        int start = buffer.position();
        int size = 1; //Type
        if ( (typeByte & INTERNAL_DURATION_HAS_MONTHS) != 0 )
        {
            size += calculatePropertyValueSizeIncludingTypeHeader( buffer.position( start + size ) );
        }
        if ( (typeByte & INTERNAL_DURATION_HAS_DAYS) != 0 )
        {
            size += calculatePropertyValueSizeIncludingTypeHeader( buffer.position( start + size ) );
        }
        size += calculatePropertyValueSizeIncludingTypeHeader( buffer.position( start + size ) ); // seconds
        if ( (typeByte & INTERNAL_DURATION_HAS_NANOS) != 0 )
        {
            size += calculatePropertyValueSizeIncludingTypeHeader( buffer.position( start + size ) );
        }
        buffer.position( start );
        return size;
    }

    @Override
    protected void writeDate( long epochDay )
    {
        beginWriteProperty( EXTERNAL_TYPE_DATE );
        writeLongBits( epochDay, EXTERNAL_TYPE_DATE );
        endWriteProperty();
    }

    @Override
    protected void writeLocalTime( long nanoOfDay )
    {
        beginWriteProperty( EXTERNAL_TYPE_LOCAL_TIME );
        writeLongBits( nanoOfDay, EXTERNAL_TYPE_LOCAL_TIME );
        endWriteProperty();
    }

    @Override
    protected void writeTime( long nanosOfDayUTC, int offsetSeconds )
    {
        beginWriteProperty( EXTERNAL_TYPE_ZONED_TIME );
        writeLongBits( nanosOfDayUTC, EXTERNAL_TYPE_ZONED_TIME );
        writeInteger( offsetSeconds );
        endWriteProperty();
    }

    private static TimeValue readZonedTime( ByteBuffer buffer, byte typeByte )
    {
        long nanosOfDayUTC = readScalarValue( buffer, internalType( typeByte ) );
        int offsetSeconds = (int) readScalarValue( buffer, internalType( buffer.get() ) );
        return TimeValue.time( nanosOfDayUTC, ZoneOffset.ofTotalSeconds( offsetSeconds ) );
    }

    private static int sizeOfZonedTime( byte typeByte, ByteBuffer buffer )
    {
        int size = 1 + sizeOfScalar( internalType( typeByte ) );
        return size + calculatePropertyValueSizeIncludingTypeHeader( buffer.position( buffer.position() + size ) );
    }

    @Override
    protected void writeLocalDateTime( long epochSecond, int nano )
    {
        beginWriteProperty( EXTERNAL_TYPE_LOCAL_DATE_TIME );
        writeLongBits( epochSecond, EXTERNAL_TYPE_LOCAL_DATE_TIME );
        writeInteger( nano );
        endWriteProperty();
    }

    private static int sizeOfLocalDateTime( byte typeByte, ByteBuffer buffer )
    {
        int size = 1 + sizeOfScalar( internalType( typeByte ) );
        return size + calculatePropertyValueSizeIncludingTypeHeader( buffer.position( buffer.position() + size ) );
    }

    private static LocalDateTimeValue readLocalDateTime( ByteBuffer buffer, byte typeByte )
    {
        long epochSecond = readScalarValue( buffer, internalType( typeByte ) );
        int nano = (int) readScalarValue( buffer, internalType( buffer.get() ) );
        return LocalDateTimeValue.localDateTime( epochSecond, nano );
    }

    @Override
    protected void writeDateTime( long epochSecondUTC, int nano, int offsetSeconds )
    {
        beginWriteProperty( EXTERNAL_TYPE_ZONED_DATE_TIME );
        writeDateTimeWithoutZoneOrOffset( epochSecondUTC, nano );
        writeInteger( offsetSeconds ); //int
        endWriteProperty();
    }

    @Override
    protected void writeDateTime( long epochSecondUTC, int nano, String zoneId )
    {
        beginWriteProperty( EXTERNAL_TYPE_ZONED_DATE_TIME );
        writeDateTimeWithoutZoneOrOffset( epochSecondUTC, nano );
        writeInteger( TimeZones.map( zoneId ) ); //short
        endWriteProperty();
    }

    private void writeDateTimeWithoutZoneOrOffset( long epochSecondUTC, int nano )
    {
        writeLongBits( epochSecondUTC, EXTERNAL_TYPE_ZONED_DATE_TIME );
        writeInteger( nano );
    }

    private static int sizeOfDateTime( byte typeByte, ByteBuffer buffer )
    {
        int start = buffer.position();
        int size = 1 + sizeOfScalar( internalType( typeByte ) );
        size += calculatePropertyValueSizeIncludingTypeHeader( buffer.position( start + size ) );
        size += calculatePropertyValueSizeIncludingTypeHeader( buffer.position( start + size ) );
        buffer.position( start );
        return size;
    }

    private static DateTimeValue readDateTime( ByteBuffer buffer, byte typeByte )
    {
        long epochSecond = readScalarValue( buffer, internalType( typeByte ) );
        int nano = (int) readScalarValue( buffer, internalType( buffer.get() ) );
        byte zoneOrOffsetTypeByte = buffer.get();
        if ( externalType( zoneOrOffsetTypeByte ) == EXTERNAL_TYPE_INT ) //offset in seconds
        {
            int offsetSeconds = (int) readScalarValue( buffer, internalType( zoneOrOffsetTypeByte ) );
            ZoneOffset offset = ZoneOffset.ofTotalSeconds( offsetSeconds );
            return DateTimeValue.datetime( epochSecond, nano, offset );
        }
        else if ( externalType( zoneOrOffsetTypeByte ) == EXTERNAL_TYPE_SHORT ) //offset in zoneid
        {
            String zoneId = TimeZones.map( (short) readScalarValue( buffer, internalType( zoneOrOffsetTypeByte ) ) );
            return DateTimeValue.datetime( epochSecond, nano, ZoneId.of( zoneId ) );
        }
        throw new IllegalStateException();
    }

    @Override
    public void writeInteger( byte value ) throws RuntimeException
    {
        beginWriteProperty( EXTERNAL_TYPE_BYTE );
        if ( !writingArray )
        {
            buffer.put( EXTERNAL_TYPE_BYTE );
        }
        buffer.put( value );
        endWriteProperty();
    }

    @Override
    public void writeInteger( short value ) throws RuntimeException
    {
        beginWriteProperty( EXTERNAL_TYPE_SHORT );
        byte internalScalarType = minimalInternalScalarType( value );
        buffer.put( createTypeByte( EXTERNAL_TYPE_SHORT, internalScalarType ) );
        writeScalarValue( value, internalScalarType );
        endWriteProperty();
    }

    @Override
    public void writeBoolean( boolean value ) throws RuntimeException
    {
        beginWriteProperty( EXTERNAL_TYPE_BOOL );
        buffer.put( (byte) (EXTERNAL_TYPE_BOOL | (value ? INTERNAL_BOOL_TRUE : INTERNAL_BOOL_FALSE) << 4) );
        endWriteProperty();
    }

    @Override
    public void writeInteger( int value )
    {
        beginWriteProperty( EXTERNAL_TYPE_INT );
        writeIntBits( value, EXTERNAL_TYPE_INT );
        endWriteProperty();
    }

    private void writeIntBits( int value, byte externalType )
    {
        byte internalScalarType = minimalInternalScalarType( value );
        buffer.put( createTypeByte( externalType, internalScalarType ) );
        writeScalarValue( value, internalScalarType );
    }

    @Override
    public void writeInteger( long value )
    {
        beginWriteProperty( EXTERNAL_TYPE_LONG );
        writeLongBits( value, EXTERNAL_TYPE_LONG );
        endWriteProperty();
    }

    private void writeLongBits( long value, byte externalType )
    {
        byte internalScalarType = minimalInternalScalarType( value );
        buffer.put( createTypeByte( externalType, internalScalarType ) );
        writeScalarValue( value, internalScalarType );
    }

    @Override
    public void writeString( String value )
    {
        if ( value.length() > 100 )
        {
            value = "removed string...";
        }

        beginWriteProperty( EXTERNAL_TYPE_STRING );
        byte[] bytes = UTF8.encode( value );
        buffer.put( EXTERNAL_TYPE_STRING );
        buffer.put( safeCastIntToUnsignedByte( bytes.length ) );
        buffer.put( bytes );
        endWriteProperty();
    }

    private static Value readString( ByteBuffer buffer )
    {
        int length = buffer.get() & 0xFF;
        byte[] bytes = new byte[length];
        buffer.get( bytes );
        return Values.stringValue( UTF8.decode( bytes ) );
    }

    @Override
    public void writeString( char value )
    {
        beginWriteProperty( EXTERNAL_TYPE_CHAR );
        if ( !writingArray )
        {
            buffer.put( EXTERNAL_TYPE_CHAR );
        }
        buffer.putChar( value );
        endWriteProperty();
    }

    static Value read( ByteBuffer buffer )
    {
        byte typeByte = buffer.get();
        boolean isSimpleInlinedValue = (typeByte & SPECIAL_TYPE_MASK) == 0;
        if ( isSimpleInlinedValue )
        {
            return readSimpleInlinedValue( typeByte, buffer );
        }
        else
        {
            boolean isPointer = (typeByte & SPECIAL_TYPE_POINTER) != 0;
            if ( isPointer )
            {
                throw new NotImplementedException( "Pointer values not yet implemented" );
                //TODO read and follow pointer to data
            }

            boolean isArray = (typeByte & SPECIAL_TYPE_ARRAY) != 0;
            if ( isArray )
            {
                byte externalType = externalType( typeByte );
                int length = (int) read( buffer ).asObject();
                return readArray( externalType, length, buffer );
            }

            throw new IllegalArgumentException( "Unknown special type" );
        }
    }

    private static Value readArray( byte externalType, int length, ByteBuffer buffer )
    {
        //do nothing
        if ( externalType == EXTERNAL_TYPE_BYTE )
        {
            byte[] data = new byte[length];
            buffer.get( data );
            return Values.of( data );
        }
        else if ( externalType == EXTERNAL_TYPE_CHAR )
        {
            char[] data = new char[length];
            for ( int i = 0; i < length; i++ )
            {
                data[i] = buffer.getChar();
            }
            return Values.of( data );
        }

        Object[] values = allocateTypeArray( externalType, length );
        for ( int i = 0; i < length; i++ )
        {
            values[i] = read( buffer ).asObject();
        }
        return Values.of( values );
    }

    private static Value readSimpleInlinedValue( byte typeByte, ByteBuffer buffer )
    {
        byte externalType = externalType( typeByte );
        switch ( externalType )
        {
        case EXTERNAL_TYPE_FLOAT:
            return Values.floatValue( Float.intBitsToFloat( (int) readScalarValue( buffer, internalType( typeByte ) ) ) );
        case EXTERNAL_TYPE_DOUBLE:
            return Values.doubleValue( Double.longBitsToDouble( readScalarValue( buffer, internalType( typeByte ) ) ) );
        case EXTERNAL_TYPE_BYTE:
            return Values.byteValue( buffer.get() );
        case EXTERNAL_TYPE_SHORT:
            return Values.shortValue( (short) readScalarValue( buffer, internalType( typeByte ) ) );
        case EXTERNAL_TYPE_INT:
            return Values.intValue( (int) readScalarValue( buffer, internalType( typeByte ) ) );
        case EXTERNAL_TYPE_LONG:
            return Values.longValue( readScalarValue( buffer, internalType( typeByte ) ) );
        case EXTERNAL_TYPE_STRING:
            return readString( buffer );
        case EXTERNAL_TYPE_CHAR:
            return Values.charValue( buffer.getChar() );
        case EXTERNAL_TYPE_BOOL:
            return Values.booleanValue( (typeByte & 0xF0) != 0 );
        case EXTERNAL_TYPE_DURATION:
            return readDuration( buffer, typeByte );
        case EXTERNAL_TYPE_POINT:
            return readPoint( buffer, typeByte );
        case EXTERNAL_TYPE_DATE:
            return DateValue.epochDate( readScalarValue( buffer, internalType( typeByte ) ) );
        case EXTERNAL_TYPE_LOCAL_TIME:
            return LocalTimeValue.localTime( readScalarValue( buffer, internalType( typeByte ) ) );
        case EXTERNAL_TYPE_ZONED_TIME:
            return readZonedTime( buffer, typeByte );
        case EXTERNAL_TYPE_LOCAL_DATE_TIME:
            return readLocalDateTime( buffer, typeByte );
        case EXTERNAL_TYPE_ZONED_DATE_TIME:
            return readDateTime( buffer, typeByte );
        default:
            throw new IllegalArgumentException();
        }
    }

    static int calculatePropertyValueSizeIncludingTypeHeader( ByteBuffer buffer )
    {
        int startPosition = buffer.position();
        byte typeByte = buffer.get( buffer.position() );
        boolean isSimpleInlinedValue = (typeByte & SPECIAL_TYPE_MASK) == 0;
        if ( isSimpleInlinedValue )
        {
            int size = sizeOfSimpleProperty( typeByte, buffer );
            buffer.position( startPosition );
            return size;
        }
        else
        {
            boolean isPointer = (typeByte & SPECIAL_TYPE_POINTER) != 0;
            if ( isPointer )
            {
                throw new NotImplementedException( "Pointer values size" );
            }

            boolean isArray = (typeByte & SPECIAL_TYPE_ARRAY) != 0;
            if ( isArray )
            {
                buffer.position( startPosition + 1 );
                int size = 1 + calculatePropertyValueSizeIncludingTypeHeader( buffer ); // type + length
                int length = (int) read( buffer ).asObject();
                for ( int i = 0; i < length; i++ )
                {
                    buffer.position( startPosition + size );
                    size += calculatePropertyValueSizeIncludingTypeHeader( buffer );
                }
                buffer.position( startPosition );
                return size;
            }

            throw new IllegalArgumentException( "Unknown special type" );
        }

    }

    private static int sizeOfSimpleProperty( byte typeByte, ByteBuffer buffer )
    {
        byte externalType = externalType( typeByte );
        switch ( externalType )
        {
        case EXTERNAL_TYPE_LOCAL_TIME:
        case EXTERNAL_TYPE_DATE:
        case EXTERNAL_TYPE_FLOAT:
        case EXTERNAL_TYPE_DOUBLE:
        case EXTERNAL_TYPE_SHORT:
        case EXTERNAL_TYPE_INT:
        case EXTERNAL_TYPE_LONG:
            return 1 + sizeOfScalar( internalType( typeByte ) ); //Type + Scalar
        case EXTERNAL_TYPE_STRING:
            int propertyLength = buffer.get( buffer.position() + 1 ) & 0xFF;
            return 1 + 1 + propertyLength; // Type + length + data
        case EXTERNAL_TYPE_BOOL:
            return 1; //Type (value is embedded)
        case EXTERNAL_TYPE_BYTE:
        case EXTERNAL_TYPE_CHAR:
            return 1 + 1; //Type + data
        case EXTERNAL_TYPE_DURATION:
            return sizeOfDuration( typeByte, buffer );
        case EXTERNAL_TYPE_POINT:
            return sizeOfPoint( typeByte, buffer );
        case EXTERNAL_TYPE_ZONED_TIME:
            return sizeOfZonedTime( typeByte, buffer );
        case EXTERNAL_TYPE_LOCAL_DATE_TIME:
            return sizeOfLocalDateTime( typeByte, buffer );
        case EXTERNAL_TYPE_ZONED_DATE_TIME:
            return sizeOfDateTime( typeByte, buffer );

        default:
            throw new IllegalArgumentException( "" + externalType );
        }
    }

    private static int sizeOfScalar( byte internalType )
    {
        if ( internalType == INTERNAL_SCALAR_TYPE_INT_0 )
        {
            return 0;
        }
        return 1 << (internalType - 1);
    }

    private byte minimalInternalScalarType( long value )
    {
        if ( value == 0 )
        {
            return INTERNAL_SCALAR_TYPE_INT_0;
        }
        if ( value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE )
        {
            return INTERNAL_SCALAR_TYPE_INT_8;
        }
        if ( value >= Short.MIN_VALUE && value <= Short.MAX_VALUE )
        {
            return INTERNAL_SCALAR_TYPE_INT_16;
        }
        if ( value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE )
        {
            return INTERNAL_SCALAR_TYPE_INT_32;
        }
        return INTERNAL_SCALAR_TYPE_INT_64;
    }

    private void writeScalarValue( long value, byte internalScalarType )
    {
        switch ( internalScalarType )
        {
        case INTERNAL_SCALAR_TYPE_INT_0:
            break; // write nothing
        case INTERNAL_SCALAR_TYPE_INT_64:
            buffer.putLong( value );
            break;
        case INTERNAL_SCALAR_TYPE_INT_32:
            buffer.putInt( (int) value );
            break;
        case INTERNAL_SCALAR_TYPE_INT_16:
            buffer.putShort( (short) value );
            break;
        case INTERNAL_SCALAR_TYPE_INT_8:
            buffer.put( (byte) value );
            break;
        default:
            throw new IllegalArgumentException( "" + internalScalarType );
        }
    }

    private static long readScalarValue( ByteBuffer buffer, byte internalScalarType )
    {
        switch ( internalScalarType )
        {
        case INTERNAL_SCALAR_TYPE_INT_64:
            return buffer.getLong();
        case INTERNAL_SCALAR_TYPE_INT_32:
            return buffer.getInt();
        case INTERNAL_SCALAR_TYPE_INT_16:
            return buffer.getShort();
        case INTERNAL_SCALAR_TYPE_INT_8:
            return buffer.get();
        case INTERNAL_SCALAR_TYPE_INT_0:
            return 0L;
        default:
            throw new IllegalArgumentException( "" + internalScalarType );
        }
    }

    private static byte getExternalType( ArrayType arrayType )
    {
        switch ( arrayType )
        {
        case BYTE:
            return EXTERNAL_TYPE_BYTE;
        case SHORT:
            return EXTERNAL_TYPE_SHORT;
        case INT:
            return EXTERNAL_TYPE_INT;
        case LONG:
            return EXTERNAL_TYPE_LONG;
        case FLOAT:
            return EXTERNAL_TYPE_FLOAT;
        case DOUBLE:
            return EXTERNAL_TYPE_DOUBLE;
        case BOOLEAN:
            return EXTERNAL_TYPE_BOOL;
        case STRING:
            return EXTERNAL_TYPE_STRING;
        case CHAR:
            return EXTERNAL_TYPE_CHAR;
        case POINT:
            return EXTERNAL_TYPE_POINT;
        case ZONED_DATE_TIME:
            return EXTERNAL_TYPE_ZONED_DATE_TIME;
        case LOCAL_DATE_TIME:
            return EXTERNAL_TYPE_LOCAL_DATE_TIME;
        case DATE:
            return EXTERNAL_TYPE_DATE;
        case ZONED_TIME:
            return EXTERNAL_TYPE_ZONED_TIME;
        case LOCAL_TIME:
            return EXTERNAL_TYPE_LOCAL_TIME;
        case DURATION:
            return EXTERNAL_TYPE_DURATION;
            default:
                throw new UnsupportedOperationException( "Unknown ArrayType:" + arrayType );
        }
    }

    private static Object[] allocateTypeArray( byte externalType, int length )
    {
        switch ( externalType )
        {
        case EXTERNAL_TYPE_BYTE:
            return new Byte[length];
        case EXTERNAL_TYPE_SHORT:
            return new Short[length];
        case EXTERNAL_TYPE_INT:
            return new Integer[length];
        case EXTERNAL_TYPE_LONG:
            return new Long[length];
        case EXTERNAL_TYPE_FLOAT:
            return new Float[length];
        case EXTERNAL_TYPE_DOUBLE:
            return new Double[length];
        case EXTERNAL_TYPE_BOOL:
            return new Boolean[length];
        case EXTERNAL_TYPE_STRING:
            return new String[length];
        case EXTERNAL_TYPE_CHAR:
            return new Character[length];
        case EXTERNAL_TYPE_POINT:
            return new PointValue[length];
        case EXTERNAL_TYPE_ZONED_DATE_TIME:
            return new ZonedDateTime[length];
        case EXTERNAL_TYPE_LOCAL_DATE_TIME:
            return new LocalDateTime[length];
        case EXTERNAL_TYPE_DATE:
            return new LocalDate[length];
        case EXTERNAL_TYPE_ZONED_TIME:
            return new OffsetTime[length];
        case EXTERNAL_TYPE_LOCAL_TIME:
            return new LocalTime[length];
        case EXTERNAL_TYPE_DURATION:
            return new TemporalAmount[length];
        default:
            throw new UnsupportedOperationException( "Unknown external type:" + externalType );
        }
    }
}

