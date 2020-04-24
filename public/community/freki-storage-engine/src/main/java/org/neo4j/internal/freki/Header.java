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

import static java.lang.Integer.min;
import static java.lang.Integer.numberOfTrailingZeros;

class Header
{
    static final int NUM_OFFSETS = 6;
    private static final int MASK_OFFSET_MARKERS = (1 << NUM_OFFSETS) - 1;
    private static final int BITS_PER_OFFSET = 10;
    private static final int MASK_OFFSET_BITS = (1 << BITS_PER_OFFSET) - 1;
    static final int MARKERS_SIZE = 2;

    static final int OFFSET_PROPERTIES = 0;
    static final int OFFSET_RELATIONSHIPS = 1;
    static final int OFFSET_DEGREES = 2;
    static final int OFFSET_RELATIONSHIPS_TYPE_OFFSETS = 3;
    static final int OFFSET_RECORD_POINTER = 4;
    static final int OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID = 5;
    static final int FLAG_LABELS = 6;
    static final int FLAG_HAS_DENSE_RELATIONSHIPS = 7;

    private byte markers;
    private byte referenceMarkers;
    private int[] offsets = new int[NUM_OFFSETS];
    private int[] sizes = new int[NUM_OFFSETS + 1/*labels*/];

    void mark( int slot, boolean marked )
    {
        markers = marked
                  ? (byte) ((int) markers | slotBit( slot ))
                  : (byte) ((int) markers & ~slotBit( slot ));
    }

    private static byte slotBit( int slot )
    {
        return (byte) (1 << slot);
    }

    boolean hasMark( int slot )
    {
        return hasMark( markers, slot );
    }

    boolean hasReferenceMark( int slot )
    {
        return hasMark( referenceMarkers, slot );
    }

    private static boolean hasMark( int markers, int slot )
    {
        return (markers & slotBit( slot )) != 0;
    }

    void setOffset( int slot, int offset )
    {
        assert (offset & ~MASK_OFFSET_BITS) == 0;
        assert hasMark( slot );
        offsets[slot] = offset;
    }

    int getOffset( int slot )
    {
        if ( slot == FLAG_LABELS )
        {
            return spaceNeeded();
        }
        return offsets[slot];
    }

    void allocateSpace( ByteBuffer buffer )
    {
        buffer.position( buffer.position() + spaceNeeded() );
    }

    int spaceNeeded()
    {
        return MARKERS_SIZE + offsetBytesNeeded();
    }

    private int offsetBytesNeeded()
    {
        int numOffsets = Integer.bitCount( markers & MASK_OFFSET_MARKERS );
        return numOffsets == 0 ? 0 : ((numOffsets * BITS_PER_OFFSET) - 1) / Byte.SIZE + 1;
    }

    int sizeOf( int slot )
    {
        assert hasMark( slot );
        return sizes[slot];
    }

    void serialize( ByteBuffer buffer, Header referenceHeader )
    {
        referenceMarkers = referenceHeader != null ? referenceHeader.markers : 0;
        assert ((markers & referenceMarkers) & ~slotBit( FLAG_HAS_DENSE_RELATIONSHIPS ) & ~slotBit( OFFSET_RECORD_POINTER )) == 0 :
                toString() + " vs " + referenceHeader;
        buffer.put( markers );
        buffer.put( referenceMarkers );
        long data = 0;
        for ( int slot = NUM_OFFSETS - 1; slot >= 0; slot-- )
        {
            if ( hasMark( slot ) )
            {
                data = data << BITS_PER_OFFSET | getOffset( slot );
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
        markers = buffer.get();
        referenceMarkers = buffer.get();
        int bytesNeeded = offsetBytesNeeded();
        long data = 0;
        for ( int i = 0; i < bytesNeeded; i++ )
        {
            long b = buffer.get() & 0xFF;
            data |= b << (i * Byte.SIZE);
        }

        int bits = markers & MASK_OFFSET_MARKERS;
        while ( bits > 0 )
        {
            int offsetSlot = numberOfTrailingZeros( bits );
            offsets[offsetSlot] = (int) (data & MASK_OFFSET_BITS);
            data >>>= BITS_PER_OFFSET;
            bits &= bits - 1;
        }
    }

    void deserializeWithSizes( ByteBuffer buffer )
    {
        deserialize( buffer );

        for ( int slot = 0; slot < sizes.length; slot++ )
        {
            int startOffset = getOffset( slot );
            int smallestOtherOffset = Integer.MAX_VALUE;
            for ( int otherSlot = 0; otherSlot < offsets.length; otherSlot++ )
            {
                if ( otherSlot != slot && hasMark( otherSlot ) && offsets[otherSlot] > startOffset )
                {
                    smallestOtherOffset = min( smallestOtherOffset, offsets[otherSlot] );
                }
            }
            int endOffset = smallestOtherOffset != Integer.MAX_VALUE ? smallestOtherOffset : buffer.limit();
            sizes[slot] = endOffset - startOffset;
        }
    }

    void clearMarks()
    {
        markers = 0;
        referenceMarkers = 0;
    }

    @Override
    public String toString()
    {
        return String.format( "Header{labels:%b/%b,dense:%b,properties:%s/%b,relationships:%s/%b,relTypeOffsets:%s/%b,degrees:%s/%b," +
                "nextInternalRelId:%s,recordPointer:%s}",
                hasMark( FLAG_LABELS ), hasReferenceMark( FLAG_LABELS ),
                hasMark( FLAG_HAS_DENSE_RELATIONSHIPS ),
                hasMark( OFFSET_PROPERTIES ) ? getOffset( OFFSET_PROPERTIES ) : "-", hasReferenceMark( OFFSET_PROPERTIES ),
                hasMark( OFFSET_RELATIONSHIPS ) ? getOffset( OFFSET_RELATIONSHIPS ) : "-", hasReferenceMark( OFFSET_RELATIONSHIPS ),
                hasMark( OFFSET_RELATIONSHIPS_TYPE_OFFSETS ) ? getOffset( OFFSET_RELATIONSHIPS_TYPE_OFFSETS ) : "-",
                hasReferenceMark( OFFSET_RELATIONSHIPS_TYPE_OFFSETS ),
                hasMark( OFFSET_DEGREES ) ? getOffset( OFFSET_DEGREES ) : "-", hasReferenceMark( OFFSET_DEGREES ),
                hasMark( OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID ) ? getOffset( OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID ) : "-",
                hasMark( OFFSET_RECORD_POINTER ) ? getOffset( OFFSET_RECORD_POINTER ) : "-"
        );
    }
}
