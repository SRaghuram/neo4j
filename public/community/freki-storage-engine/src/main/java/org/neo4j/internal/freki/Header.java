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

import static java.lang.Integer.numberOfTrailingZeros;

class Header
{
    static final int FLAG_LABELS = 0x1;
    static final int FLAG_IS_DENSE = 0x2;

    static final int NUM_OFFSETS = 6;
    private static final int MASK_OFFSET_BITS = (1 << NUM_OFFSETS) - 1;
    private static final int BITS_PER_OFFSET = 10;
    private static final int MASK_OFFSET = (1 << BITS_PER_OFFSET) - 1;
    static final int OFFSET_PROPERTIES = 0;
    static final int OFFSET_RELATIONSHIPS = 1;
    static final int OFFSET_DEGREES = 2;
    static final int OFFSET_RELATIONSHIPS_TYPE_OFFSETS = 3;
    static final int OFFSET_RECORD_POINTER = 4;
    static final int OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID = 5;

    private byte flags;
    private byte offsetBits;
    private int[] offsets = new int[NUM_OFFSETS];

    void setFlag( int flag )
    {
        flags |= flag;
    }

    void setFlag( int flag, boolean set )
    {
        if ( set )
        {
            setFlag( flag );
        }
        else
        {
            removeFlag( flag );
        }
    }

    boolean hasFlag( int flag )
    {
        return (flags & flag) != 0;
    }

    void removeFlag( int flag )
    {
        flags &= ~flag;
    }

    void markHasOffset( int offsetSlot )
    {
        offsetBits |= offsetBit( offsetSlot );
    }

    void markHasOffset( int offsetSlot, boolean hasOffset )
    {
        if ( hasOffset )
        {
            markHasOffset( offsetSlot );
        }
        else
        {
            unmarkHasOffset( offsetSlot );
        }
    }

    void unmarkHasOffset( int offsetSlot )
    {
        offsetBits &= ~offsetBit( offsetSlot );
    }

    private byte offsetBit( int offsetSlot )
    {
        return (byte) (1 << offsetSlot);
    }

    boolean hasOffset( int offsetSlot )
    {
        return (offsetBits & offsetBit( offsetSlot )) != 0;
    }

    void setOffset( int offsetSlot, int offset )
    {
        assert (offset & ~MASK_OFFSET) == 0;
        assert hasOffset( offsetSlot );
        offsets[offsetSlot] = offset;
    }

    int getOffset( int offsetSlot )
    {
        return offsets[offsetSlot];
    }

    void allocateSpace( ByteBuffer buffer )
    {
        buffer.position( buffer.position() + spaceNeeded() );
    }

    int spaceNeeded()
    {
        return 1 + offsetBytesNeeded();
    }

    private int offsetBytesNeeded()
    {
        int numOffsets = Integer.bitCount( offsetBits );
        return numOffsets == 0 ? 0 : ((numOffsets * BITS_PER_OFFSET) - 1) / Byte.SIZE + 1;
    }

    void serialize( ByteBuffer buffer )
    {
        byte allBits = (byte) ((flags << NUM_OFFSETS) | offsetBits);
        buffer.put( allBits );
        long data = 0;
        for ( int offsetSlot = NUM_OFFSETS - 1; offsetSlot >= 0; offsetSlot-- )
        {
            if ( hasOffset( offsetSlot ) )
            {
                data = data << BITS_PER_OFFSET | getOffset( offsetSlot );
            }
        }
        int bytesNeeded = offsetBytesNeeded();
        for ( int i = 0; i < bytesNeeded; i++ )
        {
            buffer.put( (byte) (data & 0xFF) );
            data >>>= Byte.SIZE;
        }
    }

    void deserialize( ByteBuffer buffer )
    {
        byte allBits = buffer.get();
        flags = (byte) (allBits >>> NUM_OFFSETS);
        offsetBits = (byte) (allBits & MASK_OFFSET_BITS);
        int bytesNeeded = offsetBytesNeeded();
        long data = 0;
        for ( int i = 0; i < bytesNeeded; i++ )
        {
            long b = buffer.get() & 0xFF;
            data |= b << (i * Byte.SIZE);
        }

        int bits = offsetBits;
        while ( bits > 0 )
        {
            int offsetSlot = numberOfTrailingZeros( bits );
            offsets[offsetSlot] = (int) (data & MASK_OFFSET);
            data >>>= BITS_PER_OFFSET;
            bits &= bits - 1;
        }
    }

    void clearOffsetMarksAndFlags()
    {
        flags = 0;
        offsetBits = 0;
    }
}
