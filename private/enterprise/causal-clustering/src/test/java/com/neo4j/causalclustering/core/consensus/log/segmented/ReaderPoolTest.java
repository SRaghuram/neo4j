/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;

import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.logging.NullLogProvider.getInstance;

class ReaderPoolTest
{
    private final Path base = Path.of( "base" );
    private final FileNames fileNames = new FileNames( base );
    private final EphemeralFileSystemAbstraction fsa = spy( new EphemeralFileSystemAbstraction() );
    private final FakeClock clock = Clocks.fakeClock();

    private ReaderPool pool = new ReaderPool( 2, getInstance(), fileNames, fsa, clock );

    @BeforeEach
    void before() throws Exception
    {
        fsa.mkdirs( base );
    }

    @AfterEach
    void tearDown() throws Exception
    {
        fsa.close();
    }

    @Test
    void shouldReacquireReaderFromPool() throws Exception
    {
        // given
        Reader reader = pool.acquire( 0, 0 );
        pool.release( reader );

        // when
        Reader newReader = pool.acquire( 0, 0 );

        // then
        verify( fsa ).write( any( Path.class ) );
        assertThat( reader, is( newReader ) );
    }

    @Test
    void shouldPruneOldReaders() throws Exception
    {
        // given
        Reader readerA = spy( pool.acquire( 0, 0 ) );
        Reader readerB = spy( pool.acquire( 0, 0 ) );

        pool.release( readerA );

        clock.forward( 2, MINUTES );
        pool.release( readerB );

        // when
        clock.forward( 1, MINUTES );
        pool.prune( 2, MINUTES );

        // then
        verify( readerA ).close();
        verify( readerB, never() ).close();
    }

    @Test
    void shouldNotReturnPrunedReaders() throws Exception
    {
        Reader readerA = pool.acquire( 0, 0 );
        Reader readerB = pool.acquire( 0, 0 );

        pool.release( readerA );
        pool.release( readerB );

        clock.forward( 2, MINUTES );
        pool.prune( 1, MINUTES );

        // when
        Reader readerC = pool.acquire( 0, 0 );
        Reader readerD = pool.acquire( 0, 0 );

        // then
        assertThat( asSet( readerC, readerD ), not( containsInAnyOrder( readerA, readerB ) ) );
    }

    @Test
    void shouldDisposeSuperfluousReaders() throws Exception
    {
        // given
        Reader readerA = spy( pool.acquire( 0, 0 ) );
        Reader readerB = spy( pool.acquire( 0, 0 ) );
        Reader readerC = spy( pool.acquire( 0, 0 ) );
        Reader readerD = spy( pool.acquire( 0, 0 ) );

        pool.release( readerA );
        pool.release( readerB );

        // when
        pool.release( readerC );
        pool.release( readerD );

        // then
        verify( readerA ).close();
        verify( readerB ).close();
        verify( readerC, never() ).close();
        verify( readerD, never() ).close();
    }

    @Test
    void shouldDisposeAllReleasedReaders() throws Exception
    {
        // given
        Reader readerA = spy( pool.acquire( 0, 0 ) );
        Reader readerB = spy( pool.acquire( 0, 0 ) );
        Reader readerC = spy( pool.acquire( 0, 0 ) );

        pool.release( readerA );
        pool.release( readerB );
        pool.release( readerC );

        // when
        pool.close();

        // then
        verify( readerA ).close();
        verify( readerB ).close();
        verify( readerC ).close();
    }

    @Test
    void shouldPruneReadersOfVersion() throws Exception
    {
        // given
        pool = new ReaderPool( 8, getInstance(), fileNames, fsa, clock );

        Reader readerA = spy( pool.acquire( 0, 0 ) );
        Reader readerB = spy( pool.acquire( 1, 0 ) );
        Reader readerC = spy( pool.acquire( 1, 0 ) );
        Reader readerD = spy( pool.acquire( 2, 0 ) );

        pool.release( readerA );
        pool.release( readerB );
        pool.release( readerC );
        pool.release( readerD );

        // when
        pool.prune( 1 );

        // then
        verify( readerA, never() ).close();
        verify( readerB ).close();
        verify( readerC ).close();
        verify( readerD, never() ).close();

        // when
        pool.prune( 0 );
        // then
        verify( readerA ).close();
        verify( readerD, never() ).close();

        // when
        pool.prune( 2 );
        // then
        verify( readerD ).close();
    }
}
