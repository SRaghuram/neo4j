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

<<<<<<< HEAD
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
=======
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.junit.jupiter.api.BeforeEach;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.ByteBuffer;
<<<<<<< HEAD
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;

=======
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.Lifespan;
<<<<<<< HEAD
=======
import org.neo4j.storageengine.util.IdUpdateListener;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.ByteBuffer.wrap;
<<<<<<< HEAD
import static java.util.Arrays.sort;
import static java.util.Comparator.comparingLong;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
=======
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.freki.BigPropertyValueStore.RECORD_DATA_SIZE;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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

<<<<<<< HEAD
=======
    private IdGeneratorFactory idGeneratorFactory;

    @BeforeEach
    void setUp()
    {
        idGeneratorFactory = new DefaultIdGeneratorFactory( fs, immediate(), false );
    }

    @Test
    void shouldWriteAndReadSingleRecordData() throws IOException
    {
        try ( Lifespan life = new Lifespan() )
        {
            // given
            BigPropertyValueStore store = instantiateStore( life, false );
            byte[] data = randomData( random.random(), RECORD_DATA_SIZE / 2 );

            // when
            long id = allocateAndWrite( store, data );

            // then
            readAndVerify( store, data, id );
        }
    }

    @Test
    void shouldWriteAndReadThreeRecordData() throws IOException
    {
        try ( Lifespan life = new Lifespan() )
        {
            // given
            BigPropertyValueStore store = instantiateStore( life, false );
            byte[] data = randomData( random.random(), (int) (RECORD_DATA_SIZE * 2.3) );

            // when
            long id = allocateAndWrite( store, data );

            // then
            readAndVerify( store, data, id );
        }
    }

>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    @Test
    void shouldWriteAndReadArbitraryData() throws IOException
    {
        try ( Lifespan life = new Lifespan() )
        {
<<<<<<< HEAD
            BigPropertyValueStore store = new BigPropertyValueStore( directory.file( "dude" ), pageCache, false, true );
            life.add( store );
            byte[][] datas = new byte[100][];
            long[] positions = new long[datas.length];
=======
            BigPropertyValueStore store = instantiateStore( life, false );
            byte[][] datas = new byte[100][];
            long[] firstIds = new long[datas.length];
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
            try ( PageCursor cursor = store.openWriteCursor( PageCursorTracer.NULL ) )
            {
                for ( int i = 0; i < datas.length; i++ )
                {
                    byte[] data = datas[i] = randomData( random.random() );
<<<<<<< HEAD
                    positions[i] = store.allocateSpace( data.length );
                    store.write( cursor, wrap( data ), positions[i] );
=======
                    firstIds[i] = allocateAndWrite( cursor, store, data );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
                }
            }

            for ( int i = 0; i < datas.length; i++ )
            {
<<<<<<< HEAD
                readAndVerify( store, datas[i], positions[i] );
=======
                readAndVerify( store, datas[i], firstIds[i] );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
            }
        }
    }

    @Test
    void shouldWriteAndReadConcurrently() throws IOException
    {
        try ( Lifespan life = new Lifespan() )
        {
            // given
<<<<<<< HEAD
            BigPropertyValueStore store = new BigPropertyValueStore( directory.file( "dude" ), pageCache, false, true );
            life.add( store );
=======
            BigPropertyValueStore store = instantiateStore( life, false );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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
<<<<<<< HEAD
                try ( PageCursor cursor = store.openWriteCursor( PageCursorTracer.NULL ) )
                {
                    long position = store.allocateSpace( data.length );
                    store.write( cursor, ByteBuffer.wrap( data ), position );
                    expectedData[i].put( position, data );
                }
=======
                expectedData[i].put( allocateAndWrite( store, data ), data );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
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
<<<<<<< HEAD
=======

>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        //when
        long firstPropertyPointer = writeDataAndShutDown( firstProp );
        long secondPropertyPointer = writeDataAndShutDown( secondProp );
        long thirdPropertyPointer = writeDataAndShutDown( thirdProp );

        //then
<<<<<<< HEAD
        assertArrayEquals( firstProp, readDataAndShutDown( firstPropertyPointer, firstProp.length ) );
        assertArrayEquals( secondProp, readDataAndShutDown( secondPropertyPointer, secondProp.length ) );
        assertArrayEquals( thirdProp, readDataAndShutDown( thirdPropertyPointer, thirdProp.length ) );

        assertTrue( secondPropertyPointer >= firstPropertyPointer + firstProp.length, "Pointers are not overlapping" );
        assertTrue( thirdPropertyPointer >= secondPropertyPointer + secondProp.length, "Pointers are not overlapping" );

    }

    @Test
    void shouldBeAbleToFetchDataLength() throws IOException
=======
        assertArrayEquals( firstProp, readDataAndShutDown( firstPropertyPointer ) );
        assertArrayEquals( secondProp, readDataAndShutDown( secondPropertyPointer ) );
        assertArrayEquals( thirdProp, readDataAndShutDown( thirdPropertyPointer ) );

        assertTrue( secondPropertyPointer >= firstPropertyPointer, "Pointers are not overlapping" );
        assertTrue( thirdPropertyPointer >= secondPropertyPointer, "Pointers are not overlapping" );
    }

    @Test
    void shouldDeleteValues() throws IOException
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
    {
        try ( Lifespan life = new Lifespan() )
        {
            // given
<<<<<<< HEAD
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
=======
            BigPropertyValueStore store = instantiateStore( life, false );
            byte[][] datas = new byte[10][];
            long[] firstIds = new long[datas.length];
            try ( PageCursor cursor = store.openWriteCursor( PageCursorTracer.NULL ) )
            {
                for ( int i = 0; i < datas.length; i++ )
                {
                    byte[] data = datas[i] = randomData( random.random() );
                    firstIds[i] = allocateAndWrite( cursor, store, data );
                }
            }

            // when
            int indexToRemove = random.nextInt( datas.length );
            List<Record> records = new ArrayList<>();
            try ( PageCursor cursor = store.openReadCursor( PageCursorTracer.NULL ) )
            {
                store.visitRecordChainIds( cursor, firstIds[indexToRemove], chainId -> records.add( Record.deletedRecord( (byte) 0, chainId ) ) );
            }
            try ( PageCursor cursor = store.openWriteCursor( PageCursorTracer.NULL ) )
            {
                store.write( cursor, records, IdUpdateListener.DIRECT, PageCursorTracer.NULL );
            }

            // then
            try ( PageCursor cursor = store.openReadCursor( PageCursorTracer.NULL ) )
            {
                for ( int i = 0; i < firstIds.length; i++ )
                {
                    boolean expectedToExist = i != indexToRemove;
                    assertThat( store.exists( cursor, firstIds[i] ) ).isEqualTo( expectedToExist );
                }
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
            }
        }
    }

    private long writeDataAndShutDown( byte[] data ) throws IOException
    {
        try ( Lifespan life = new Lifespan() )
        {
<<<<<<< HEAD
            BigPropertyValueStore store = new BigPropertyValueStore( directory.file( "dude" ), pageCache, false, true );
            life.add( store );
            long pointer;
            try ( PageCursor cursor = store.openWriteCursor( PageCursorTracer.NULL ) )
            {
                pointer = store.allocateSpace( data.length );
                store.write( cursor, ByteBuffer.wrap( data ), pointer );
            }
=======
            BigPropertyValueStore store = instantiateStore( life, false );
            long pointer = allocateAndWrite( store, data );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
            store.flush( IOLimiter.UNLIMITED, PageCursorTracer.NULL );
            return pointer;
        }
    }

<<<<<<< HEAD
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
=======
    private long allocateAndWrite( BigPropertyValueStore store, byte[] data ) throws IOException
    {
        try ( PageCursor cursor = store.openWriteCursor( PageCursorTracer.NULL ) )
        {
            return allocateAndWrite( cursor, store, data );
        }
    }

    private long allocateAndWrite( PageCursor cursor, BigPropertyValueStore store, byte[] data ) throws IOException
    {
        List<Record> records = store.allocate( wrap( data ), PageCursorTracer.NULL );
        long id = records.get( 0 ).id;
        assertThat( store.exists( cursor, id ) ).isFalse();
        store.write( cursor, records, IdUpdateListener.DIRECT, PageCursorTracer.NULL );
        assertThat( store.exists( cursor, id ) ).isTrue();
        // Do an additional test of visitRecordChainIds while we have all the records anyway
        MutableLongList chainIds = LongLists.mutable.empty();
        store.visitRecordChainIds( cursor, id, chainIds::add );
        assertThat( chainIds.size() ).isEqualTo( records.size() );
        for ( int i = 0; i < records.size(); i++ )
        {
            assertThat( chainIds.get( i ) ).isEqualTo( records.get( i ).id );
        }
        return id;
    }

    private byte[] readDataAndShutDown( long pointer ) throws IOException
    {
        try ( Lifespan life = new Lifespan() )
        {
            BigPropertyValueStore store = instantiateStore( life, true );
            try ( PageCursor cursor = store.openReadCursor( PageCursorTracer.NULL ) )
            {
                return store.read( cursor, length -> ByteBuffer.wrap( new byte[length] ), pointer ).array();
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
            }
        }
    }

<<<<<<< HEAD
    private void readAndVerify( BigPropertyValueStore store, byte[] expectedData, long position ) throws IOException
    {
        try ( PageCursor cursor = store.openReadCursor( PageCursorTracer.NULL ) )
        {
            byte[] readData = new byte[expectedData.length];
            store.read( cursor, ByteBuffer.wrap( readData ), position );
            assertArrayEquals( expectedData, readData );
=======
    private void readAndVerify( BigPropertyValueStore store, byte[] expectedData, long id ) throws IOException
    {
        try ( PageCursor cursor = store.openReadCursor( PageCursorTracer.NULL ) )
        {
            ByteBuffer readBuffer = store.read( cursor, id );
            byte[] readData = new byte[readBuffer.remaining()];
            readBuffer.get( readData );
            assertThat( readData ).isEqualTo( expectedData );
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        }
    }

    private byte[] randomData( Random random )
    {
<<<<<<< HEAD
        int length = random.nextInt( 50_000 ) + 10;
=======
        return randomData( random, random.nextInt( 50_000 ) + 10 );
    }

    private byte[] randomData( Random random, int length )
    {
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
        byte[] bytes = new byte[length];
        random.nextBytes( bytes );
        return bytes;
    }
<<<<<<< HEAD
=======

    private BigPropertyValueStore instantiateStore( Lifespan life, boolean readOnly )
    {
        BigPropertyValueStore store =
                new BigPropertyValueStore( directory.file( "dude" ), pageCache, idGeneratorFactory, IdType.STRING_BLOCK, readOnly, true, NULL );
        life.add( store );
        return store;
    }
>>>>>>> f26a3005d9b9a7f42b480941eb059582c7469aaa
}
