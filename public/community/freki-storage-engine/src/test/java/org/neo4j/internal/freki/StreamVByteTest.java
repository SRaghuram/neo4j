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

import org.neo4j.internal.freki.StreamVByte.IntArrayTarget;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.internal.freki.StreamVByte.intArrayTarget;

@ExtendWith( RandomExtension.class )
class StreamVByteTest
{
    @Inject
    private RandomRule random;

    @Test
    void shouldWriteAndRead()
    {
        // given
        int[] values = new int[random.nextInt( 0, 1_000 )];
        int[] sizes = {0xFF, 0xFFFF, 0x1FFFFFF};
        for ( int i = 0, prev = 0; i < values.length; i++ )
        {
            int diff = random.nextInt( 1, sizes[random.nextInt( sizes.length )] + 1 );
            values[i] = prev + diff;
            prev = values[i];
        }

        // when
        byte[] data = new byte[10_000];
        int writeOffset = StreamVByte.writeDeltas( values, data, 0 );

        // then
        IntArrayTarget target = intArrayTarget();
        int readOffset = StreamVByte.readDeltas( target, data, 0 );
        assertArrayEquals( values, target.array() );
        assertEquals( writeOffset, readOffset );
    }

    @Test
    void shouldWriteAndReadSmall()
    {
        // given
        int[] values = new int[random.nextInt( 0, 2 )];
        for ( int i = 0, prev = 0; i < values.length; i++ )
        {
            int diff = random.nextInt( 1, random.nextInt( 256 ) );
            values[i] = prev + diff;
            prev = values[i];
        }

        // when
        byte[] data = new byte[50];
        int writeOffset = StreamVByte.writeDeltas( values, data, 0 );

        // then
        IntArrayTarget target = intArrayTarget();
        int readOffset = StreamVByte.readDeltas( target, data, 0 );
        assertArrayEquals( values, target.array() );
        assertEquals( writeOffset, readOffset );
        assertEquals( values.length + 1, writeOffset );
    }
}
