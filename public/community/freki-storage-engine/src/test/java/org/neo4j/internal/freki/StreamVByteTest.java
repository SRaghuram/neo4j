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

import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.freki.StreamVByte.readInts;
import static org.neo4j.internal.freki.StreamVByte.readLongs;
import static org.neo4j.internal.freki.StreamVByte.writeInts;
import static org.neo4j.internal.freki.StreamVByte.writeLongs;

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

        // when/then
        assertWriteAndReadCorrectly( values, randomExistingData,
                ( writer, data ) -> writeInts( writer, data, true, values.length ),
                data -> readInts( data, true ) );
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

        // when/then
        assertWriteAndReadCorrectly( values, randomExistingData,
                ( writer, data ) -> writeInts( writer, data, true, values.length ),
                data -> readInts( data, true ) );
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

        // when/then
        assertWriteAndReadCorrectly( values, randomExistingData,
                ( writer, data ) -> writeLongs( writer, data, values.length ),
                data -> readLongs( data ) );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void shouldWriteLessValuesThanWorstCaseCountAndUndoLastWrittenLong( boolean randomExistingData )
    {
        // given
        long[] values = new long[random.nextInt( 1, 1_000 )];
        long[] sizes = {0xFFFFFF, 0xFFFFFFFF, 0xFFFFFFFFFFL};
        for ( int i = 0; i < values.length; i++ )
        {
            values[i] = random.nextLong( 0, sizes[random.nextInt( sizes.length )] + 1 );
        }

        // when
        ByteBuffer buffer = newTargetBuffer( 10_000, randomExistingData );
        int numValuesToWrite = random.nextInt( 0, values.length - 1 );
        long[] subset = Arrays.copyOf( values, numValuesToWrite );
        StreamVByte.Writer writer = new StreamVByte.Writer();
        // numValuesToWrite+1, the +1 here is to ensure that they both write the header in the same style
        writeLongs( writer, buffer, numValuesToWrite + 1 );
        for ( int i = 0; i < numValuesToWrite; i++ )
        {
            writer.writeNext( values[i] );
        }
        writer.done();
        int expectedOffset = buffer.position();
        buffer.position( 0 );

        writer = new StreamVByte.Writer();
        writeLongs( writer, buffer, numValuesToWrite + 1 );
        for ( int i = 0; i < numValuesToWrite + 1; i++ )
        {
            writer.writeNext( values[i] );
        }
        writer.undoWrite();
        writer.done();

        // then
        assertThat( buffer.position() ).isEqualTo( expectedOffset );
        assertThat( readLongs( buffer.position( 0 ) ) ).isEqualTo( subset );
    }

    private void assertWriteAndReadCorrectly( Object values, boolean randomExistingData, BiConsumer<StreamVByte.Writer,ByteBuffer> writeInitializer,
            Function<ByteBuffer,Object> reader )
    {
        // when
        ByteBuffer data = newTargetBuffer( 10_000, randomExistingData );
        StreamVByte.Writer writer = new StreamVByte.Writer();
        int length = Array.getLength( values );
        writeInitializer.accept( writer, data );
        for ( int i = 0; i < length; i++ )
        {
            writer.writeNext( ((Number) Array.get( values, i )).longValue() );
        }
        writer.done();
        int writeOffset = data.position();

        // then
        Object read = reader.apply( data.position( 0 ) );
        int readOffset = data.position();
        assertThat( read ).isEqualTo( values );
        assertThat( readOffset ).isEqualTo( writeOffset );
        // TODO and also verify using Reader
    }

    private ByteBuffer newTargetBuffer( int length, boolean randomExistingData )
    {
        ByteBuffer data = ByteBuffer.wrap( new byte[length] );
        if ( randomExistingData )
        {
            random.nextBytes( data.array() );
        }
        data.limit( data.capacity() - 20 );
        return data;
    }
}
