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

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith( RandomExtension.class )
class HeaderTest
{
    @Inject
    private RandomRule random;

    @Test
    void shouldReadAndWriteFlagsAndOffsets()
    {
        // given
        Header header = new Header();
        header.setFlag( Header.FLAG_LABELS );
        header.setFlag( Header.FLAG_IS_DENSE );

        // when/then
        assertReadAndWrite( header, 1 );
        assertAddOffsetAndReadAndWrite( header, 0, 3 );
        assertAddOffsetAndReadAndWrite( header, 1, 4 );
        assertAddOffsetAndReadAndWrite( header, 2, 5 );
        assertAddOffsetAndReadAndWrite( header, 3, 6 );
        assertAddOffsetAndReadAndWrite( header, 4, 8 );
        assertAddOffsetAndReadAndWrite( header, 5, 9 );
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
        assertThat( readHeader.hasFlag( Header.FLAG_LABELS ) ).isEqualTo( header.hasFlag( Header.FLAG_LABELS ) );
        assertThat( readHeader.hasFlag( Header.FLAG_IS_DENSE ) ).isEqualTo( header.hasFlag( Header.FLAG_IS_DENSE ) );
        for ( int offsetSlot = 0; offsetSlot < Header.NUM_OFFSETS; offsetSlot++ )
        {
            assertThat( readHeader.hasOffset( offsetSlot ) ).isEqualTo( header.hasOffset( offsetSlot ) );
            if ( readHeader.hasOffset( offsetSlot ) )
            {
                assertThat( readHeader.getOffset( offsetSlot ) ).isEqualTo( header.getOffset( offsetSlot ) );
            }
        }
    }

    private void assertAddOffsetAndReadAndWrite( Header header, int offsetSlot, int expectedSize )
    {
        int offset = random.nextInt( 0x3FF );
        header.markHasOffset( offsetSlot );
        header.setOffset( offsetSlot, offset );
        assertReadAndWrite( header, expectedSize );
    }
}
