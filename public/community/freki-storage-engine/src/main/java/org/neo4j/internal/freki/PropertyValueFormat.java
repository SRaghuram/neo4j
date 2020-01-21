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

import java.nio.ByteBuffer;

import org.neo4j.string.UTF8;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.utils.TemporalValueWriterAdapter;

import static org.neo4j.internal.helpers.Numbers.safeCastIntToUnsignedByte;

class PropertyValueFormat extends TemporalValueWriterAdapter<RuntimeException>
{
    private static final byte EXTERNAL_TYPE_INT = 0;
    private static final byte EXTERNAL_TYPE_LONG = 1;
    private static final byte EXTERNAL_TYPE_STRING = 2;

    private static final byte INTERNAL_SCALAR_TYPE_INT_8 = 0;
    private static final byte INTERNAL_SCALAR_TYPE_INT_16 = 1;
    private static final byte INTERNAL_SCALAR_TYPE_INT_32 = 2;
    private static final byte INTERNAL_SCALAR_TYPE_INT_64 = 3;

    private final ByteBuffer buffer;

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
        return (byte) ((typeByte & 0xE0) >> 5);
    }

    private static boolean negativeScalar( byte typeByte )
    {
        return (typeByte & 0x10) != 0;
    }

    private static byte createTypeByte( byte externalType, byte internalType, boolean negative )
    {
        return (byte) (externalType | (internalType << 5) | (negative ? 1 : 0) << 4);
    }

    @Override
    public void writeInteger( int value )
    {
        boolean negative = value < 0;
        if ( negative )
        {
            value = ~value;
        }
        byte internalScalarType = minimalInternalScalarType( value );
        buffer.put( createTypeByte( EXTERNAL_TYPE_INT, internalScalarType, negative ) );
        writeScalarValue( value, internalScalarType );
    }

    static Value read( ByteBuffer buffer )
    {
        byte typeByte = buffer.get();
        switch ( externalType( typeByte ) )
        {
        case EXTERNAL_TYPE_INT:
        {
            byte internalScalarType = internalType( typeByte );
            long value = readScalarValue( buffer, internalScalarType );
            if ( negativeScalar( typeByte ) )
            {
                value = ~value;
            }
            return Values.intValue( (int) value );
        }
        case EXTERNAL_TYPE_LONG:
        {
            byte internalScalarType = internalType( typeByte );
            return Values.longValue( readScalarValue( buffer, internalScalarType ) );
        }
        case EXTERNAL_TYPE_STRING:
            return readString( buffer );
        default:
            throw new IllegalArgumentException();
        }
    }

    static int calculatePropertyValueSizeIncludingTypeHeader( ByteBuffer buffer )
    {
        byte typeByte = buffer.get( buffer.position() );
        byte externalType = PropertyValueFormat.externalType( typeByte );
        switch ( externalType )
        {
        case PropertyValueFormat.EXTERNAL_TYPE_INT:
        case PropertyValueFormat.EXTERNAL_TYPE_LONG:
            return 1 + (1 << PropertyValueFormat.internalType( typeByte )); //Type + Scalar
        case PropertyValueFormat.EXTERNAL_TYPE_STRING:
            int propertyLength = buffer.get( buffer.position() + 1 );
            return 1 + 1 + propertyLength; // Type + length + data
        default:
            throw new IllegalArgumentException( "" + externalType );
        }
    }

    @Override
    public void writeInteger( long value )
    {
        byte internalScalarType = minimalInternalScalarType( value );
        buffer.put( createTypeByte( EXTERNAL_TYPE_LONG, internalScalarType, false ) ); //No special handling of negative longs
        writeScalarValue( value, internalScalarType );
    }

    @Override
    public void writeString( String value )
    {
        byte[] bytes = UTF8.encode( value );
        buffer.put( EXTERNAL_TYPE_STRING );
        buffer.put( safeCastIntToUnsignedByte( bytes.length ) );
        buffer.put( bytes );
    }

    private static Value readString( ByteBuffer buffer )
    {
        int length = buffer.get() & 0xFF;
        byte[] bytes = new byte[length];
        buffer.get( bytes );
        return Values.stringValue( UTF8.decode( bytes ) );
    }

    // TODO add more here of course

    private byte minimalInternalScalarType( long value )
    {
        if ( (value & 0xFFFFFFFF_80000000L) != 0 )
        {
            return INTERNAL_SCALAR_TYPE_INT_64;
        }
        if ( (value & 0x7FFF8000L) != 0 )
        {
            return INTERNAL_SCALAR_TYPE_INT_32;
        }
        if ( (value & 0x7F80L) != 0 )
        {
            return INTERNAL_SCALAR_TYPE_INT_16;
        }
        return INTERNAL_SCALAR_TYPE_INT_8;
    }

    private void writeScalarValue( long value, byte internalScalarType )
    {
        switch ( internalScalarType )
        {
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
        default:
            throw new IllegalArgumentException( "" + internalScalarType );
        }
    }
}
