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

import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.ByteBuffer.wrap;
import static java.util.Arrays.sort;
import static java.util.Comparator.comparingLong;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.test.Race.throwing;

@ExtendWith( RandomExtension.class )
@PageCacheExtension
class BigPropertyValueStoreTest
{
    @Inject
    private PageCache pageCache;
    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory directory;
    @Inject
    private RandomRule random;

    @Test
    void shouldWriteAndReadArbitraryData() throws IOException
    {
        try ( Lifespan life = new Lifespan() )
        {
            BigPropertyValueStore store = new BigPropertyValueStore( directory.file( "dude" ), pageCache, false, true );
            life.add( store );
            byte[][] datas = new byte[100][];
            long[] positions = new long[datas.length];
            try ( PageCursor cursor = store.openWriteCursor( PageCursorTracer.NULL ) )
            {
                for ( int i = 0; i < datas.length; i++ )
                {
                    byte[] data = datas[i] = randomData( random.random() );
                    positions[i] = store.allocateSpace( data.length );
                    store.write( cursor, wrap( data ), positions[i] );
                }
            }

            for ( int i = 0; i < datas.length; i++ )
            {
                readAndVerify( store, datas[i], positions[i] );
            }
        }
    }

    @Test
    void shouldWriteAndReadConcurrently() throws IOException
    {
        try ( Lifespan life = new Lifespan() )
        {
            // given
            BigPropertyValueStore store = new BigPropertyValueStore( directory.file( "dude" ), pageCache, false, true );
            life.add( store );
            Race race = new Race();
            MutableLongObjectMap<byte[]>[] expectedData = new MutableLongObjectMap[4];
            for ( int i = 0; i < expectedData.length; i++ )
            {
                expectedData[i] = LongObjectMaps.mutable.empty();
            }

            // when
            race.addContestants( expectedData.length, i -> throwing( () ->
            {
                byte[] data = randomData( ThreadLocalRandom.current() );
                try ( PageCursor cursor = store.openWriteCursor( PageCursorTracer.NULL ) )
                {
                    long position = store.allocateSpace( data.length );
                    store.write( cursor, ByteBuffer.wrap( data ), position );
                    expectedData[i].put( position, data );
                }
            } ), 100 );
            race.goUnchecked();

            // then
            for ( MutableLongObjectMap<byte[]> expected : expectedData )
            {
                MutableLongIterator positions = expected.keySet().longIterator();
                while ( positions.hasNext() )
                {
                    long position = positions.next();
                    readAndVerify( store, expected.get( position ), position );
                }
            }
        }
    }

    @Test
    void shouldRememberWhereToWriteAfterRestart() throws IOException
    {
        //given
        byte[] firstProp = "A B C D E F G H I J K L M N O P Q R S T U V W X Y Z".getBytes();
        byte[] secondProp = randomData( ThreadLocalRandom.current() );
        byte[] thirdProp = "1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20".getBytes();
        //when
        long firstPropertyPointer = writeDataAndShutDown( firstProp );
        long secondPropertyPointer = writeDataAndShutDown( secondProp );
        long thirdPropertyPointer = writeDataAndShutDown( thirdProp );

        //then
        assertArrayEquals( firstProp, readDataAndShutDown( firstPropertyPointer, firstProp.length ) );
        assertArrayEquals( secondProp, readDataAndShutDown( secondPropertyPointer, secondProp.length ) );
        assertArrayEquals( thirdProp, readDataAndShutDown( thirdPropertyPointer, thirdProp.length ) );

        assertTrue( secondPropertyPointer >= firstPropertyPointer + firstProp.length, "Pointers are not overlapping" );
        assertTrue( thirdPropertyPointer >= secondPropertyPointer + secondProp.length, "Pointers are not overlapping" );

    }

    @Test
    void shouldBeAbleToFetchDataLength() throws IOException
    {
        try ( Lifespan life = new Lifespan() )
        {
            // given
            BigPropertyValueStore store = new BigPropertyValueStore( directory.file( "dude" ), pageCache, false, true );
            life.add( store );

            byte[] data = randomData( ThreadLocalRandom.current() );
            try ( PageCursor cursor = store.openWriteCursor( PageCursorTracer.NULL ) )
            {
                long pointer = store.allocateSpace( data.length );
                store.write( cursor, ByteBuffer.wrap( data ), pointer );

                //then
                assertEquals( data.length, store.length( cursor, pointer ) );
            }
        }
    }

    @Test
    void shouldAllocateSpaceConcurrently()
    {
        try ( Lifespan life = new Lifespan() )
        {
            // given
            BigPropertyValueStore store = new BigPropertyValueStore( directory.file( "dude" ), pageCache, false, true );
            life.add( store );

            // when
            Race race = new Race().withEndCondition( () -> false );
            ConcurrentLinkedQueue<Pair<Long,Integer>> allocations = new ConcurrentLinkedQueue<>();
            race.addContestants( 8, i ->
            {
                Random rng = new Random( random.nextLong() );
                return () ->
                {
                    int length = rng.nextInt( 200 ) + 1;
                    long position = store.allocateSpace( length );
                    allocations.add( Pair.of( position, length ) );
                };
            }, 100 );
            race.goUnchecked();

            // then
            Pair<Long,Integer>[] allocationsArray = allocations.toArray( new Pair[allocations.size()] );
            sort( allocationsArray, comparingLong( Pair::getLeft ) );
            long prevHighPosition = 0;
            for ( Pair<Long,Integer> allocation : allocationsArray )
            {
                assertThat( allocation.getLeft() ).isGreaterThanOrEqualTo( prevHighPosition );
                prevHighPosition = allocation.getLeft() + allocation.getRight();
            }
        }
    }

    private long writeDataAndShutDown( byte[] data ) throws IOException
    {
        try ( Lifespan life = new Lifespan() )
        {
            BigPropertyValueStore store = new BigPropertyValueStore( directory.file( "dude" ), pageCache, false, true );
            life.add( store );
            long pointer;
            try ( PageCursor cursor = store.openWriteCursor( PageCursorTracer.NULL ) )
            {
                pointer = store.allocateSpace( data.length );
                store.write( cursor, ByteBuffer.wrap( data ), pointer );
            }
            store.flush( IOLimiter.UNLIMITED, PageCursorTracer.NULL );
            return pointer;
        }
    }

    private byte[] readDataAndShutDown( long pointer, int size ) throws IOException
    {
        try ( Lifespan life = new Lifespan() )
        {
            BigPropertyValueStore store = new BigPropertyValueStore( directory.file( "dude" ), pageCache, true, true );
            life.add( store );
            try ( PageCursor cursor = store.openReadCursor( PageCursorTracer.NULL ) )
            {
                byte[] data = new byte[size];
                ByteBuffer buffer = ByteBuffer.wrap( data );
                store.read( cursor, buffer, pointer );
                return data;
            }
        }
    }

    private void readAndVerify( BigPropertyValueStore store, byte[] expectedData, long position ) throws IOException
    {
        try ( PageCursor cursor = store.openReadCursor( PageCursorTracer.NULL ) )
        {
            byte[] readData = new byte[expectedData.length];
            store.read( cursor, ByteBuffer.wrap( readData ), position );
            assertArrayEquals( expectedData, readData );
        }
    }

    private byte[] randomData( Random random )
    {
        int length = random.nextInt( 50_000 ) + 10;
        byte[] bytes = new byte[length];
        random.nextBytes( bytes );
        return bytes;
    }
}
