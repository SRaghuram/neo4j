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
package org.neo4j.index.internal.gbptree;

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.index.internal.gbptree.FreeListIdProvider.Monitor;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.index.internal.gbptree.FreeListIdProvider.NO_MONITOR;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

@ExtendWith( RandomExtension.class )
class FreeListIdProviderTest
{
    private static final int PAGE_SIZE = 128;
    private static final long GENERATION_ONE = GenerationSafePointer.MIN_GENERATION;
    private static final long GENERATION_TWO = GENERATION_ONE + 1;
    private static final long GENERATION_THREE = GENERATION_TWO + 1;
    private static final long GENERATION_FOUR = GENERATION_THREE + 1;
    private static final long BASE_ID = 5;

    private PageAwareByteArrayCursor cursor;
    private final PagedFile pagedFile = mock( PagedFile.class );
    private final FreelistPageMonitor monitor = new FreelistPageMonitor();
    private FreeListIdProvider freelist;

    @Inject
    private RandomRule random;

    @BeforeEach
    void setUpPagedFile() throws IOException
    {
        cursor = new PageAwareByteArrayCursor( PAGE_SIZE );

        when( pagedFile.io( anyLong(), anyInt(), any() ) ).thenAnswer(
                invocation -> cursor.duplicate( invocation.getArgument( 0 ) ) );
        when( pagedFile.pageSize() ).thenReturn( PAGE_SIZE );

        freelist = new FreeListIdProvider( pagedFile, BASE_ID, monitor );
        freelist.initialize( BASE_ID + 1, BASE_ID + 1, BASE_ID + 1, 0, 0 );
    }

    @Test
    void shouldReleaseAndAcquireId() throws Exception
    {
        // GIVEN
        long releasedId = 11;
        fillPageWithRandomBytes( releasedId );

        // WHEN
        long acquiredId;
        try ( IdProvider.Writer ids = freelist.writer( NULL ) )
        {
            ids.releaseId( GENERATION_ONE, GENERATION_TWO, releasedId );
            acquiredId = ids.acquireNewId( GENERATION_TWO, GENERATION_THREE, cursor.duplicate() );
        }

        // THEN
        assertEquals( releasedId, acquiredId );
        cursor.next( acquiredId );
        assertEmpty( cursor );
    }

    @Test
    void shouldReleaseAndAcquireIdsFromMultiplePages() throws Exception
    {
        // GIVEN
        int entries = freelist.entriesPerPage() + freelist.entriesPerPage() / 2;
        long baseId = 101;
        try ( IdProvider.Writer ids = freelist.writer( NULL ) )
        {
            for ( int i = 0; i < entries; i++ )
            {
                ids.releaseId( GENERATION_ONE, GENERATION_TWO, baseId + i );
            }
        }

        // WHEN/THEN
        try ( IdProvider.Writer ids = freelist.writer( NULL ) )
        {
            for ( int i = 0; i < entries; i++ )
            {
                long acquiredId = ids.acquireNewId( GENERATION_TWO, GENERATION_THREE, cursor.duplicate() );
                assertEquals( baseId + i, acquiredId );
            }
        }
    }

    @Test
    void shouldPutFreedFreeListPagesIntoFreeListAsWell() throws Exception
    {
        // GIVEN
        long prevId;
        long acquiredId = BASE_ID + 1;
        long freelistPageId = BASE_ID + 1;
        MutableLongSet released = new LongHashSet();
        try ( IdProvider.Writer ids = freelist.writer( NULL ) )
        {
            do
            {
                prevId = acquiredId;
                acquiredId = ids.acquireNewId( GENERATION_ONE, GENERATION_TWO, cursor.duplicate() );
                ids.releaseId( GENERATION_ONE, GENERATION_TWO, acquiredId );
                released.add( acquiredId );
            }
            while ( acquiredId - prevId == 1 );
        }

        // WHEN
        try ( IdProvider.Writer ids = freelist.writer( NULL ) )
        {
            while ( !released.isEmpty() )
            {
                long reAcquiredId = ids.acquireNewId( GENERATION_TWO, GENERATION_THREE, cursor.duplicate() );
                released.remove( reAcquiredId );
            }

            // THEN
            assertEquals( freelistPageId, ids.acquireNewId( GENERATION_THREE, GENERATION_FOUR, cursor.duplicate() ) );
        }
    }

    @Test
    void shouldStayBoundUnderStress() throws Exception
    {
        // GIVEN
        MutableLongSet acquired = new LongHashSet();
        List<Long> acquiredList = new ArrayList<>(); // for quickly finding random to remove
        long stableGeneration = GenerationSafePointer.MIN_GENERATION;
        long unstableGeneration = stableGeneration + 1;
        int iterations = 100;

        // WHEN
        try ( IdProvider.Writer ids = freelist.writer( NULL ) )
        {
            for ( int i = 0; i < iterations; i++ )
            {
                for ( int j = 0; j < 10; j++ )
                {
                    if ( random.nextBoolean() )
                    {
                        // acquire
                        int count = random.intBetween( 5, 10 );
                        for ( int k = 0; k < count; k++ )
                        {
                            long acquiredId = ids.acquireNewId( stableGeneration, unstableGeneration, cursor.duplicate() );
                            assertTrue( acquired.add( acquiredId ) );
                            acquiredList.add( acquiredId );
                        }
                    }
                    else
                    {
                        // release
                        int count = random.intBetween( 5, 20 );
                        for ( int k = 0; k < count && !acquired.isEmpty(); k++ )
                        {
                            long id = acquiredList.remove( random.nextInt( acquiredList.size() ) );
                            assertTrue( acquired.remove( id ) );
                            ids.releaseId( stableGeneration, unstableGeneration, id );
                        }
                    }
                }

                for ( long id : acquiredList )
                {
                    ids.releaseId( stableGeneration, unstableGeneration, id );
                }
                acquiredList.clear();
                acquired.clear();

                // checkpoint, sort of
                stableGeneration = unstableGeneration;
                unstableGeneration++;
            }
        }

        // THEN
        assertTrue( freelist.lastId() < 200, String.valueOf( freelist.lastId() ) );
    }

    @Test
    void shouldVisitUnacquiredIds() throws Exception
    {
        // GIVEN a couple of released ids
        MutableLongSet expected = new LongHashSet();
        try ( IdProvider.Writer ids = freelist.writer( NULL ) )
        {
            for ( int i = 0; i < 100; i++ )
            {
                expected.add( ids.acquireNewId( GENERATION_ONE, GENERATION_TWO, cursor.duplicate() ) );
            }
            expected.forEach( id ->
            {
                try
                {
                    ids.releaseId( GENERATION_ONE, GENERATION_TWO, id );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            } );
            // and only a few acquired
            for ( int i = 0; i < 10; i++ )
            {
                long acquiredId = ids.acquireNewId( GENERATION_TWO, GENERATION_THREE, cursor.duplicate() );
                assertTrue( expected.remove( acquiredId ) );
            }
        }

        // WHEN/THEN
        freelist.visitFreelist( new IdProvider.IdProviderVisitor.Adaptor()
        {
            @Override
            public void freelistEntry( long pageId, long generation, int pos )
            {
                assertTrue( expected.remove( pageId ) );
            }
        }, NULL );
        assertTrue( expected.isEmpty() );
    }

    @Test
    void shouldVisitFreelistPageIds() throws Exception
    {
        // GIVEN a couple of released ids
        MutableLongSet expected = new LongHashSet();
        // Add the already allocated free-list page id
        expected.add( BASE_ID + 1 );
        monitor.set( new Monitor()
        {
            @Override
            public void acquiredFreelistPageId( long freelistPageId )
            {
                expected.add( freelistPageId );
            }
        } );
        try ( IdProvider.Writer ids = freelist.writer( NULL ) )
        {
            for ( int i = 0; i < 100; i++ )
            {
                long id = ids.acquireNewId( GENERATION_ONE, GENERATION_TWO, cursor.duplicate() );
                ids.releaseId( GENERATION_ONE, GENERATION_TWO, id );
            }
        }
        assertTrue( expected.size() > 0 );

        // WHEN/THEN
        freelist.visitFreelist( new IdProvider.IdProviderVisitor.Adaptor()
        {
            @Override
            public void beginFreelistPage( long pageId )
            {
                assertTrue( expected.remove( pageId ) );
            }
        }, NULL );
        assertTrue( expected.isEmpty() );
    }

    private void fillPageWithRandomBytes( long releasedId )
    {
        cursor.next( releasedId );
        byte[] crapData = new byte[PAGE_SIZE];
        ThreadLocalRandom.current().nextBytes( crapData );
        cursor.putBytes( crapData );
    }

    private static void assertEmpty( PageCursor cursor )
    {
        byte[] data = new byte[PAGE_SIZE];
        cursor.getBytes( data );
        for ( byte b : data )
        {
            assertEquals( 0, b );
        }
    }

    private static class FreelistPageMonitor implements Monitor
    {
        private Monitor actual = NO_MONITOR;

        void set( Monitor actual )
        {
            this.actual = actual;
        }

        @Override
        public void acquiredFreelistPageId( long freelistPageId )
        {
            actual.acquiredFreelistPageId( freelistPageId );
        }

        @Override
        public void releasedFreelistPageId( long freelistPageId )
        {
            actual.releasedFreelistPageId( freelistPageId );
        }
    }
}
