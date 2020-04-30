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
    static final int NUM_OFFSETS = 7;
    private static final int MASK_OFFSET_MARKERS = (1 << NUM_OFFSETS) - 1;
    private static final int BITS_PER_OFFSET = 10;
    private static final int MASK_OFFSET_BITS = (1 << BITS_PER_OFFSET) - 1;
    static final int MARKERS_SIZE = 3; // 1B markers, 1B referenceMarkers, 1B higher bits for both markers and referenceMarkers

    static final int OFFSET_PROPERTIES = 0;
    static final int OFFSET_RELATIONSHIPS = 1;
    static final int OFFSET_DEGREES = 2;
    static final int OFFSET_RELATIONSHIPS_TYPE_OFFSETS = 3;
    static final int OFFSET_RECORD_POINTER = 4;
    static final int OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID = 5;
    static final int OFFSET_END = 6;
    static final int FLAG_LABELS = 7;
    static final int FLAG_HAS_DENSE_RELATIONSHIPS = 8;

    private int markers;
    private int referenceMarkers;
    private int[] offsets = new int[NUM_OFFSETS];
    private int[] sizes = new int[NUM_OFFSETS + 1/*labels*/];

    static Header shallowCopy( Header from )
    {
        //This is not a full copy!!
        Header header = new Header();
        header.markers = from.markers;
        return header;
    }

    void mark( int slot, boolean marked )
    {
        markers = marked
                  ? markers | slotBit( slot )
                  : markers & ~slotBit( slot );
    }

    private static int slotBit( int slot )
    {
        return 1 << slot;
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
        assert slot != FLAG_LABELS;
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
        referenceMarkers = referenceHeader != null ? referenceHeader.markers & ~markers : 0;
        assert Integer.bitCount( markers & MASK_OFFSET_MARKERS ) <= 6 :
                "Even though there are 7 types of offsets there can only be 6 active concurrently (RELATIONSHIPS vs DEGREES) so long data is fine for now";
        buffer.put( (byte) markers );
        buffer.put( (byte) referenceMarkers );
        byte highMarks = (byte) (markers >>> Byte.SIZE | (referenceMarkers >>> 4) & 0xF0);
        buffer.put( highMarks );
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
        int markersLsb = buffer.get() & 0xFF;
        int referenceMarkersLsb = buffer.get() & 0xFF;
        int highMarks = buffer.get() & 0xFF;
        markers = markersLsb | (highMarks & 0xF) << Byte.SIZE;
        referenceMarkers = referenceMarkersLsb | (highMarks & 0xF0) << 4;
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

    public boolean hasMarkers()
    {
        return markers != 0;
    }

    @Override
    public String toString()
    {
        return String.format( "Header{labels:%b/%b,dense:%b,properties:%s/%b,relationships:%s/%b,relTypeOffsets:%s/%b,degrees:%s/%b," +
                "nextInternalRelId:%s,recordPointer:%s,end:%s}",
                hasMark( FLAG_LABELS ), hasReferenceMark( FLAG_LABELS ),
                hasMark( FLAG_HAS_DENSE_RELATIONSHIPS ),
                hasMark( OFFSET_PROPERTIES ) ? getOffset( OFFSET_PROPERTIES ) : "-", hasReferenceMark( OFFSET_PROPERTIES ),
                hasMark( OFFSET_RELATIONSHIPS ) ? getOffset( OFFSET_RELATIONSHIPS ) : "-", hasReferenceMark( OFFSET_RELATIONSHIPS ),
                hasMark( OFFSET_RELATIONSHIPS_TYPE_OFFSETS ) ? getOffset( OFFSET_RELATIONSHIPS_TYPE_OFFSETS ) : "-",
                hasReferenceMark( OFFSET_RELATIONSHIPS_TYPE_OFFSETS ),
                hasMark( OFFSET_DEGREES ) ? getOffset( OFFSET_DEGREES ) : "-", hasReferenceMark( OFFSET_DEGREES ),
                hasMark( OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID ) ? getOffset( OFFSET_NEXT_INTERNAL_RELATIONSHIP_ID ) : "-",
                hasMark( OFFSET_RECORD_POINTER ) ? getOffset( OFFSET_RECORD_POINTER ) : "-",
                hasMark( OFFSET_END ) ? getOffset( OFFSET_END ) : "-"
        );
    }
}
