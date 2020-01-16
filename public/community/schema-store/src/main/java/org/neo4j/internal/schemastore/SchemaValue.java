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
package org.neo4j.internal.schemastore;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.string.UTF8;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DoubleArray;
import org.neo4j.values.storable.IntArray;
import org.neo4j.values.storable.IntValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.Values;

import static org.neo4j.internal.schemastore.SchemaLayout.checkArrayLengthSanity;

class SchemaValue
{
    private static final byte TYPE_STRING = 0;
    private static final byte TYPE_BOOLEAN = 1;
    private static final byte TYPE_INT = 2;
    private static final byte TYPE_LONG = 3;
    private static final byte TYPE_INT_ARRAY = 4;
    private static final byte TYPE_DOUBLE_ARRAY = 5;
    Value value;

    int size()
    {
        int size = Byte.BYTES; // for the type
        if ( value.valueGroup() == ValueGroup.TEXT )
        {
            byte[] bytes = UTF8.encode( ((TextValue) value).stringValue() );
            size += Integer.BYTES + bytes.length;
        }
        else if ( value.valueGroup() == ValueGroup.BOOLEAN )
        {
            size += Byte.BYTES;
        }
        else if ( value instanceof IntValue )
        {
            size += Integer.BYTES;
        }
        else if ( value instanceof LongValue )
        {
            size += Long.BYTES;
        }
        else if ( value instanceof IntArray )
        {
            int[] array = ((IntArray) value).asObjectCopy();
            size += Integer.BYTES + array.length * Integer.BYTES;
        }
        else if ( value instanceof DoubleArray )
        {
            double[] array = ((DoubleArray) value).asObjectCopy();
            size += Integer.BYTES + array.length * Double.BYTES;
        }
        else
        {
            throw new UnsupportedOperationException( "Unsupported value type " + value + " " + value.getClass() );
        }
        return size;
    }

    void write( PageCursor cursor )
    {
        if ( value.valueGroup() == ValueGroup.TEXT )
        {
            byte[] bytes = UTF8.encode( ((TextValue) value).stringValue() );
            cursor.putByte( TYPE_STRING );
            cursor.putInt( bytes.length );
            cursor.putBytes( bytes );
        }
        else if ( value.valueGroup() == ValueGroup.BOOLEAN )
        {
            cursor.putByte( TYPE_BOOLEAN );
            cursor.putByte( (byte) (((BooleanValue) value).booleanValue() ? 1 : 0) );
        }
        else if ( value instanceof IntValue )
        {
            cursor.putByte( TYPE_INT );
            cursor.putInt( ((IntValue) value).value() );
        }
        else if ( value instanceof LongValue )
        {
            cursor.putByte( TYPE_LONG );
            cursor.putLong( ((LongValue) value).value() );
        }
        else if ( value instanceof IntArray )
        {
            cursor.putByte( TYPE_INT_ARRAY );
            int[] array = ((IntArray) value).asObjectCopy();
            cursor.putInt( array.length );
            for ( int item : array )
            {
                cursor.putInt( item );
            }
        }
        else if ( value instanceof DoubleArray )
        {
            cursor.putByte( TYPE_DOUBLE_ARRAY );
            double[] array = ((DoubleArray) value).asObjectCopy();
            cursor.putInt( array.length );
            for ( double item : array )
            {
                cursor.putLong( Double.doubleToLongBits( item ) );
            }
        }
        else
        {
            throw new UnsupportedOperationException( "Unsupported value type " + value + " " + value.getClass() );
        }
    }

    void read( PageCursor cursor, int valueSize )
    {
        byte type = cursor.getByte();
        switch ( type )
        {
        case TYPE_STRING:
            int stringLength = cursor.getInt();
            if ( !checkArrayLengthSanity( cursor, stringLength ) )
            {
                break;
            }
            byte[] stringBytes = new byte[stringLength];
            cursor.getBytes( stringBytes );
            value = Values.stringValue( UTF8.decode( stringBytes ) );
            break;
        case TYPE_BOOLEAN:
            byte booleanValue = cursor.getByte();
            if ( booleanValue != 0 && booleanValue != 1 )
            {
                cursor.setCursorException( "Unexpected boolean value " + booleanValue );
                break;
            }
            value = Values.booleanValue( booleanValue == 1 );
            break;
        case TYPE_INT:
            value = Values.intValue( cursor.getInt() );
            break;
        case TYPE_LONG:
            value = Values.longValue( cursor.getLong() );
            break;
        case TYPE_INT_ARRAY:
            int intArrayLength = cursor.getInt();
            if ( !checkArrayLengthSanity( cursor, intArrayLength ) )
            {
                break;
            }
            int[] intArray = new int[intArrayLength];
            for ( int i = 0; i < intArrayLength; i++ )
            {
                intArray[i] = cursor.getInt();
            }
            value = Values.intArray( intArray );
            break;
        case TYPE_DOUBLE_ARRAY:
            int doubleArrayLength = cursor.getInt();
            if ( !checkArrayLengthSanity( cursor, doubleArrayLength ) )
            {
                break;
            }
            double[] doubleArray = new double[doubleArrayLength];
            for ( int i = 0; i < doubleArrayLength; i++ )
            {
                doubleArray[i] = Double.longBitsToDouble( cursor.getLong() );
            }
            value = Values.doubleArray( doubleArray );
            break;
        default:
            cursor.setCursorException( "Unexpected type " + type );
        }
    }

    @Override
    public String toString()
    {
        return value.toString();
    }
}
