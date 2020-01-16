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

public class VarIntGB3B
{
    static final int[] O = new int[256];
    static
    {
        O[0b00000000] = 0;

        O[0b00000001] = O[0b00000100] = O[0b00010000] = O[0b01000000] = 1;
        O[0b00000010] = O[0b00001000] = O[0b00100000] = O[0b10000000] = 2;
        O[0b00000011] = O[0b00001100] = O[0b00110000] = O[0b11000000] = 3;

        O[0b00000101] = O[0b00010001] = O[0b01000001] = O[0b00010100] = O[0b01000100] = O[0b01010000] = 2;
        O[0b00001010] = O[0b00100010] = O[0b10000010] = O[0b00101000] = O[0b10001000] = O[0b10100000] = 4;
        O[0b00001111] = O[0b00110011] = O[0b11000011] = O[0b00111100] = O[0b11001100] = O[0b11110000] = 6;

        O[0b00010101] = O[0b01000101] = O[0b01010001] = O[0b01010100] = 3;
        O[0b00101010] = O[0b10001010] = O[0b10100010] = O[0b10101000] = 6;
        O[0b00111111] = O[0b11001111] = O[0b11110011] = O[0b11111100] = 9;

        O[0b01010101] = 4;
        O[0b10101010] = 8;
        O[0b11111111] = 12;
    }

    static int write( int[] values, byte[] into, int initialIntoOffset )
    {
        // Write 4 and 4 values
        // First a header byte with the sizes of the 4 next values: 0b00 00 00 00
        //                                                value[n]     3  2  1  0
        // Then the actual (max) 4 values
        int offset = initialIntoOffset;
        for ( int i = 0; i < values.length; )
        {
            int headerOffset = offset++;
            for ( int j = 0; i < values.length && j < 4; j++, i++ )
            {
                offset = writeValue( into, offset, headerOffset, j, values[i] );
            }
        }
        if ( values.length % 4 == 0 )
        {
            into[offset++] = 0;
        }
        return offset;
    }

    static int writeValue( byte[] into, int offset, int headerOffset, int j, int value )
    {
        if ( (value & 0xFF0000) != 0 )
        {
            into[headerOffset] |= 0x3 << (j * 2);
            into[offset++] = (byte) value;
            into[offset++] = (byte) (value >>> 8);
            into[offset++] = (byte) (value >>> 16);
        }
        else if ( (value & 0xFF00) != 0 )
        {
            into[headerOffset] |= 0x2 << (j * 2);
            into[offset++] = (byte) value;
            into[offset++] = (byte) (value >>> 8);
        }
        else
        {
            into[headerOffset] |= 0x1 << (j * 2);
            into[offset++] = (byte) value;
        }
        return offset;
    }

    /**
     * @return [nnno,oooo][oooo,oooo][oooo,oooo][oooo,oooo]
     * i: number of values read
     * o: offset into from to start reading more from in next call
     */
    static int readNext4( int[] into, int intoOffset, byte[] from, int initialOffset )
    {
        int offset = initialOffset;
        int headerByte = from[offset++] & 0xFF;
        for ( int j = 0; j < 4; j++ )
        {
            int size = (headerByte >>> (j * 2)) & 0x3;
            switch ( size )
            {
            case 0:
                return offset | (j << 29);
            case 1:
                into[intoOffset + j] = from[offset++] & 0xFF;
                break;
            case 2:
                into[intoOffset + j] = from[offset++] & 0xFF | ((from[offset++] & 0xFF) << 8);
                break;
            case 3:
                into[intoOffset + j] = from[offset++] & 0xFF | ((from[offset++] & 0xFF) << 8) | ((from[offset++] & 0xFF) << 16);
                break;
            default:
                throw new IllegalStateException( "Cannot be" );
            }
        }
        return offset | (4 << 29);
    }

    static int offsetFromReadResult( int readResult )
    {
        return readResult & 0x1FFFFFFF;
    }

    static int numberOfValuesFromReadResult( int readResult )
    {
        return (readResult & 0xE0000000) >>> 29;
    }
}
