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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

@ExtendWith( RandomExtension.class )
class VByteTest
{
    @Inject
    private RandomRule random;

    @Test
    void shouldWriteAndRead()
    {
        // given
        int[] testArray = new int[random.nextInt( 50 ) + 10];
        ByteBuffer buffer = ByteBuffer.wrap( new byte[1_000] );
        for ( int i = 0; i < testArray.length; i++ )
        {
            testArray[i] = random.nextInt( 1 << 19 );
        }

        // when
        for ( int i = 0; i < testArray.length; i++ )
        {
            VByte.write( testArray[i], buffer );
        }
        buffer.flip();

        // then
        int[] readArray = new int[testArray.length];
        for ( int i = 0; i < readArray.length; i++ )
        {
            readArray[i] = VByte.read( buffer );
        }
        assertArrayEquals( testArray, readArray );
    }
}
