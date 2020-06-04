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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.freki.Header.FLAG_HAS_DENSE_RELATIONSHIPS;
import static org.neo4j.internal.freki.Header.FLAG_LABELS;
import static org.neo4j.internal.freki.Header.MARKERS_SIZE;
import static org.neo4j.internal.freki.Header.NUM_OFFSETS;
import static org.neo4j.internal.freki.Header.OFFSET_PROPERTIES;
import static org.neo4j.internal.freki.Header.OFFSET_RELATIONSHIPS;

@ExtendWith( RandomExtension.class )
class HeaderTest
{
    @Inject
    private RandomRule random;

    @Test
    void shouldReadAndWriteMarksAndOffsets()
    {
        // given
        Header header = new Header();
        header.mark( FLAG_LABELS, true );
        header.mark( FLAG_HAS_DENSE_RELATIONSHIPS, true );

        // when/then
        assertReadAndWrite( header, MARKERS_SIZE );

        List<Integer> offsets = new ArrayList<>( List.of( 0, 1, 2, 3, 4, 5, 6 ) );
        Collections.shuffle( offsets, random.random() );

        assertAddOffsetAndReadAndWrite( header, offsets.get( 0 ), MARKERS_SIZE + 2 );
        assertAddOffsetAndReadAndWrite( header, offsets.get( 1 ), MARKERS_SIZE + 3 );
        assertAddOffsetAndReadAndWrite( header, offsets.get( 2 ), MARKERS_SIZE + 4 );
        assertAddOffsetAndReadAndWrite( header, offsets.get( 3 ), MARKERS_SIZE + 5 );
        assertAddOffsetAndReadAndWrite( header, offsets.get( 4 ), MARKERS_SIZE + 7 );
        assertAddOffsetAndReadAndWrite( header, offsets.get( 5 ), MARKERS_SIZE + 8 );
        // The header currently only supports 6 active offsets, so to set this last one first unset another
        header.mark( offsets.get( random.nextInt( 6 ) ), false );
        assertAddOffsetAndReadAndWrite( header, offsets.get( 6 ), MARKERS_SIZE + 8 );
    }

    @Test
    void shouldAlsoReadAndWriteReferenceMarkers()
    {
        // given
        Header header = new Header();
        Header referenceHeader = new Header();
        header.mark( FLAG_LABELS, false );
        referenceHeader.mark( FLAG_LABELS, true );
        header.mark( OFFSET_PROPERTIES, true );
        referenceHeader.mark( OFFSET_PROPERTIES, false );
        header.mark( OFFSET_RELATIONSHIPS, false );
        referenceHeader.mark( OFFSET_RELATIONSHIPS, true );

        // when
        ByteBuffer buffer = ByteBuffer.allocate( header.spaceNeeded() );
        header.setReference( referenceHeader );
        header.serialize( buffer );
        Header readHeader = new Header();
        readHeader.deserialize( buffer.position( 0 ) );

        // then
        assertThat( readHeader.hasMark( FLAG_LABELS ) ).isEqualTo( header.hasMark( FLAG_LABELS ) );
        assertThat( readHeader.hasReferenceMark( FLAG_LABELS ) ).isEqualTo( header.hasReferenceMark( FLAG_LABELS ) );
        assertThat( readHeader.hasMark( OFFSET_PROPERTIES ) ).isEqualTo( header.hasMark( OFFSET_PROPERTIES ) );
        assertThat( readHeader.hasReferenceMark( OFFSET_PROPERTIES ) ).isEqualTo( header.hasReferenceMark( OFFSET_PROPERTIES ) );
        assertThat( readHeader.hasMark( OFFSET_RELATIONSHIPS ) ).isEqualTo( header.hasMark( OFFSET_RELATIONSHIPS ) );
        assertThat( readHeader.hasReferenceMark( OFFSET_RELATIONSHIPS ) ).isEqualTo( header.hasReferenceMark( OFFSET_RELATIONSHIPS ) );
    }

    private void assertReadAndWrite( Header header, int expectedSize )
    {
        ByteBuffer buffer = ByteBuffer.allocate( expectedSize );
        header.allocateSpace( buffer );
        assertThat( buffer.position() ).isEqualTo( expectedSize );
        buffer.clear();
        header.serialize( buffer );
        buffer.flip();
        Header readHeader = new Header();
        readHeader.deserialize( buffer );
        assertThat( buffer.position() ).isEqualTo( expectedSize );
        assertThat( readHeader.hasMark( FLAG_LABELS ) ).isEqualTo( header.hasMark( FLAG_LABELS ) );
        assertThat( readHeader.hasMark( FLAG_HAS_DENSE_RELATIONSHIPS ) ).isEqualTo( header.hasMark( FLAG_HAS_DENSE_RELATIONSHIPS ) );
        for ( int offsetSlot = 0; offsetSlot < NUM_OFFSETS; offsetSlot++ )
        {
            assertThat( readHeader.hasMark( offsetSlot ) ).isEqualTo( header.hasMark( offsetSlot ) );
            if ( readHeader.hasMark( offsetSlot ) )
            {
                assertThat( readHeader.getOffset( offsetSlot ) ).isEqualTo( header.getOffset( offsetSlot ) );
            }
        }
    }

    private void assertAddOffsetAndReadAndWrite( Header header, int slot, int expectedSize )
    {
        int offset = random.nextInt( 0x3FF );
        header.mark( slot, true );
        header.setOffset( slot, offset );
        assertReadAndWrite( header, expectedSize );
    }
}
