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

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( RandomExtension.class )
class VarIntGB3BTest
{
    @Inject
    private RandomRule random;

    @Test
    void shouldWriteAndRead()
    {
        // given
        int[] array =new int[random.nextInt( 10, 50 )];
        for ( int i = 0; i < array.length; i++ )
        {
            array[i] = random.nextInt( 1_000 );
        }
        byte[] target = new byte[1_000];

        // when
        int writeEndOffset = VarIntGB3B.write( array, target, 0 );

        // then
        int[] read = new int[array.length];
        int readOffset = 0;
        for ( int numberOfValues = 4, i = 0; numberOfValues == 4; i+= numberOfValues )
        {
            int readResult = VarIntGB3B.readNext4( read, i, target, readOffset );
            numberOfValues = VarIntGB3B.numberOfValuesFromReadResult( readResult );
            readOffset = VarIntGB3B.offsetFromReadResult( readResult );
        }
        assertEquals( writeEndOffset, readOffset );
        assertArrayEquals( array, read );
    }
}
