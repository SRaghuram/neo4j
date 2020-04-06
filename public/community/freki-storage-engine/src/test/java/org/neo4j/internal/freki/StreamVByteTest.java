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

import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.ByteBuffer;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith( RandomExtension.class )
class StreamVByteTest
{
    @Inject
    private RandomRule random;

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldWriteAndRead( boolean randomExistingData )
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
        ByteBuffer data = newTargetBuffer( 10_000, randomExistingData );
        StreamVByte.writeIntDeltas( values, data );
        int writeOffset = data.position();

        // then
        data.position( 0 );
        int[] read = StreamVByte.readIntDeltas( data );
        int readOffset = data.position();
        assertArrayEquals( values, read );
        assertEquals( writeOffset, readOffset );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldWriteAndReadSmall( boolean randomExistingData )
    {
        // given
        int[] values = new int[random.nextInt( 0, 2 )];
        for ( int i = 0, prev = 0; i < values.length; i++ )
        {
            int diff = random.nextInt( 1, 255 );
            values[i] = prev + diff;
            prev = values[i];
        }

        // when
        ByteBuffer data = newTargetBuffer( 50, randomExistingData );
        StreamVByte.writeIntDeltas( values, data );
        int writeOffset = data.position();

        // then
        data.position( 0 );
        int[] read = StreamVByte.readIntDeltas( data );
        int readOffset = data.position();
        assertArrayEquals( values, read );
        assertEquals( writeOffset, readOffset );
        assertEquals( values.length + 1, writeOffset );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldWriteAndReadLongs( boolean randomExistingData )
    {
        // given
        long[] values = new long[random.nextInt( 0, 1_000 )];
        long[] sizes = {0xFFFFFF, 0xFFFFFFFF, 0xFFFFFFFFFFL};
        for ( int i = 0; i < values.length; i++ )
        {
            values[i] = random.nextLong( 0, sizes[random.nextInt( sizes.length )] + 1 );
        }

        // when
        ByteBuffer data = newTargetBuffer( 50_000, randomExistingData );
        StreamVByte.writeLongs( values, data );
        int writeOffset = data.position();

        // then
        data.position( 0 );
        long[] readValues = StreamVByte.readLongs( data );
        assertArrayEquals( values, readValues );
        int readOffset = data.position();
        assertEquals( writeOffset, readOffset );
    }

    private ByteBuffer newTargetBuffer( int length, boolean randomExistingData )
    {
        ByteBuffer data = ByteBuffer.wrap( new byte[length] );
        if ( randomExistingData )
        {
            random.nextBytes( data.array() );
        }
        return data;
    }
}
