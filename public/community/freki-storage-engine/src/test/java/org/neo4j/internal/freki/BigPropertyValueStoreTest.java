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
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.test.Race;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;

import static java.nio.ByteBuffer.wrap;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.neo4j.io.pagecache.tracing.cursor.DefaultPageCursorTracerSupplier.TRACER_SUPPLIER;
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
            BigPropertyValueStore store = new BigPropertyValueStore( fs, directory.file( "dude" ), pageCache, false, true, TRACER_SUPPLIER );
            life.add( store );
            byte[][] datas = new byte[100][];
            long[] positions = new long[datas.length];
            try ( PageCursor cursor = store.openWriteCursor() )
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
            BigPropertyValueStore store = new BigPropertyValueStore( fs, directory.file( "dude" ), pageCache, false, true, TRACER_SUPPLIER );
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
                try ( PageCursor cursor = store.openWriteCursor() )
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

    private void readAndVerify( BigPropertyValueStore store, byte[] expectedData, long position ) throws IOException
    {
        try ( PageCursor cursor = store.openReadCursor() )
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
