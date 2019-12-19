/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

public class VByte
{
    static void write( int value, ByteBuffer buffer )
    {
        for ( int i = 0; i < 4; i++ )
        {
            int oneByte = value & 0x7F;
            if ( (value & 0x7F) != value )
            {
                oneByte |= 0x80;
                value >>>= 7;
            }
            else
            {
                i = 4;
            }
            buffer.put( (byte) oneByte );
        }
    }

    static int read( ByteBuffer cursor )
    {
        int value = 0;
        for ( int i = 0; i < 4; i++ )
        {
            int oneByte = cursor.get() & 0xFF;
            value |= (oneByte & 0x7F) << (i * 7);
            if ( (oneByte & 0x80) == 0 )
            {
                break;
            }
        }
        return value;
    }
}
