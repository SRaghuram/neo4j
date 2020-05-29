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

import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.internal.id.IdType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.ByteBuffer.wrap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.freki.BigPropertyValueStore.RECORD_DATA_SIZE;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
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

    @Test
    void shouldWriteAndReadArbitraryData() throws IOException
    {
        try ( Lifespan life = new Lifespan() )
        {
            BigPropertyValueStore store = instantiateStore( life, false );
            byte[][] datas = new byte[100][];
            long[] firstIds = new long[datas.length];
            try ( PageCursor cursor = store.openWriteCursor( PageCursorTracer.NULL ) )
            {
                for ( int i = 0; i < datas.length; i++ )
                {
                    byte[] data = datas[i] = randomData( random.random() );
                    firstIds[i] = allocateAndWrite( cursor, store, data );
                }
            }

            for ( int i = 0; i < datas.length; i++ )
            {
                readAndVerify( store, datas[i], firstIds[i] );
            }
        }
    }

    @Test
    void shouldWriteAndReadConcurrently() throws IOException
    {
        try ( Lifespan life = new Lifespan() )
        {
            // given
            BigPropertyValueStore store = instantiateStore( life, false );
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
                expectedData[i].put( allocateAndWrite( store, data ), data );
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
        assertArrayEquals( firstProp, readDataAndShutDown( firstPropertyPointer ) );
        assertArrayEquals( secondProp, readDataAndShutDown( secondPropertyPointer ) );
        assertArrayEquals( thirdProp, readDataAndShutDown( thirdPropertyPointer ) );

        assertTrue( secondPropertyPointer >= firstPropertyPointer, "Pointers are not overlapping" );
        assertTrue( thirdPropertyPointer >= secondPropertyPointer, "Pointers are not overlapping" );
    }

    @Test
    void shouldDeleteValues() throws IOException
    {
        try ( Lifespan life = new Lifespan() )
        {
            // given
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
            }
        }
    }

    private long writeDataAndShutDown( byte[] data ) throws IOException
    {
        try ( Lifespan life = new Lifespan() )
        {
            BigPropertyValueStore store = instantiateStore( life, false );
            long pointer = allocateAndWrite( store, data );
            store.flush( IOLimiter.UNLIMITED, PageCursorTracer.NULL );
            return pointer;
        }
    }

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
            }
        }
    }

    private void readAndVerify( BigPropertyValueStore store, byte[] expectedData, long id ) throws IOException
    {
        try ( PageCursor cursor = store.openReadCursor( PageCursorTracer.NULL ) )
        {
            ByteBuffer readBuffer = store.read( cursor, id );
            byte[] readData = new byte[readBuffer.remaining()];
            readBuffer.get( readData );
            assertThat( readData ).isEqualTo( expectedData );
        }
    }

    private byte[] randomData( Random random )
    {
        return randomData( random, random.nextInt( 50_000 ) + 10 );
    }

    private byte[] randomData( Random random, int length )
    {
        byte[] bytes = new byte[length];
        random.nextBytes( bytes );
        return bytes;
    }

    private BigPropertyValueStore instantiateStore( Lifespan life, boolean readOnly )
    {
        BigPropertyValueStore store =
                new BigPropertyValueStore( directory.file( "dude" ), pageCache, idGeneratorFactory, IdType.STRING_BLOCK, readOnly, true, NULL );
        life.add( store );
        return store;
    }
}
