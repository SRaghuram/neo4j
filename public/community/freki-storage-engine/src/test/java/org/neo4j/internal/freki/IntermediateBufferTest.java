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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.freki.IntermediateBuffer.isFirstFromPieceHeader;
import static org.neo4j.internal.freki.IntermediateBuffer.isLastFromPieceHeader;
import static org.neo4j.internal.freki.IntermediateBuffer.ordinalFromPieceHeader;
import static org.neo4j.internal.freki.IntermediateBuffer.versionFromPieceHeader;

class IntermediateBufferTest
{
    @Test
    void shouldCreateCorrectPieceHeader()
    {
        // given
        IntermediateBuffer buffer = new IntermediateBuffer( 100 );
        int numBuffers = 3;
        byte preVersion = 33;
        buffer.setVersion( preVersion );
        for ( int i = 0; i < numBuffers; i++ )
        {
            buffer.add();
        }
        byte expectedVersion = (byte) (preVersion + 1);

        // when/then
        for ( byte expectedOrdinal = 0; expectedOrdinal == 0 || buffer.next(); expectedOrdinal++ )
        {
            assertPieceHeader( numBuffers, expectedVersion, expectedOrdinal, buffer.getPieceHeader() );
        }
        for ( byte expectedOrdinal = (byte) (numBuffers - 1); expectedOrdinal == numBuffers - 1 || buffer.prev(); expectedOrdinal-- )
        {
            assertPieceHeader( numBuffers, expectedVersion, expectedOrdinal, buffer.getPieceHeader() );
        }
    }

    private void assertPieceHeader( int numBuffers, byte expectedVersion, byte expectedOrdinal, short pieceHeader )
    {
        assertThat( versionFromPieceHeader( pieceHeader ) ).isEqualTo( expectedVersion );
        assertThat( ordinalFromPieceHeader( pieceHeader ) ).isEqualTo( expectedOrdinal );
        assertThat( isFirstFromPieceHeader( pieceHeader ) ).isEqualTo( expectedOrdinal == 0 );
        assertThat( isLastFromPieceHeader( pieceHeader ) ).isEqualTo( expectedOrdinal == numBuffers - 1 );
    }
}
