/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.consensus.log.RaftLogCursor;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import com.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import com.neo4j.causalclustering.core.consensus.log.cache.InFlightCache;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class InFlightLogEntryReaderTest
{
    private final ReadableRaftLog raftLog = mock( ReadableRaftLog.class );
    private final InFlightCache inFlightCache = mock( ConsecutiveInFlightCache.class );
    private final long logIndex = 42L;
    private final RaftLogEntry entry = mock( RaftLogEntry.class );

    @TestWithCacheClearing
    void shouldUseTheCacheWhenTheIndexIsPresent( boolean clearCache ) throws Exception
    {
        // given
        var reader = new InFlightLogEntryReader( raftLog, inFlightCache, clearCache );
        startingFromIndexReturnEntries( inFlightCache, logIndex, entry );
        startingFromIndexReturnEntries( raftLog, -1, null );

        // when
        var raftLogEntry = reader.get( logIndex );

        // then
        assertEquals( entry, raftLogEntry );
        verify( inFlightCache ).get( logIndex );
        assertCacheIsUpdated( inFlightCache, logIndex, clearCache );
        verifyNoMoreInteractions( inFlightCache );
        verifyNoInteractions( raftLog );
    }

    @TestWithCacheClearing
    void shouldUseTheRaftLogWhenTheIndexIsNotPresent( boolean clearCache ) throws Exception
    {
        // given
        var reader = new InFlightLogEntryReader( raftLog, inFlightCache, clearCache );
        startingFromIndexReturnEntries( inFlightCache, logIndex, null );
        startingFromIndexReturnEntries( raftLog, logIndex, entry );

        // when
        var raftLogEntry = reader.get( logIndex );

        // then
        assertEquals( entry, raftLogEntry );
        verify( inFlightCache ).get( logIndex );
        verify( raftLog ).getEntryCursor( logIndex );
        assertCacheIsUpdated( inFlightCache, logIndex, clearCache );

        verifyNoMoreInteractions( inFlightCache );
        verifyNoMoreInteractions( raftLog );
    }

    @TestWithCacheClearing
    void shouldNeverUseCacheAgainAfterHavingFallenBackToTheRaftLog( boolean clearCache ) throws Exception
    {
        // given
        var entriesCount = 10;
        var entries = new RaftLogEntry[entriesCount];
        Arrays.setAll( entries, i -> mock( RaftLogEntry.class ) );
        entries[0] = entry;

        var reader = new InFlightLogEntryReader( raftLog, inFlightCache, clearCache );
        startingFromIndexReturnEntries( inFlightCache, logIndex, entry, null, entries[0] );
        startingFromIndexReturnEntries( raftLog, logIndex + 1, entries[1], Arrays.copyOfRange( entries, 2, entriesCount ) );

        for ( var offset = 0; offset < entriesCount; offset++ )
        {
            // when
            var raftLogEntry = reader.get( offset + logIndex );

            // then
            assertEquals( entries[offset], raftLogEntry );

            if ( offset <= 1 )
            {
                verify( inFlightCache ).get( offset + logIndex );
            }

            if ( offset == 1 )
            {
                verify( raftLog ).getEntryCursor( offset + logIndex );
            }

            assertCacheIsUpdated( inFlightCache, offset + logIndex, clearCache );
        }

        // then one hit, one miss and 8 skipped accesses
        verify( inFlightCache, times( 8 ) ).reportSkippedCacheAccess();
        verifyNoMoreInteractions( raftLog );
    }

    private static void startingFromIndexReturnEntries( InFlightCache inFlightCache, long startIndex,
            RaftLogEntry entry, RaftLogEntry... otherEntries )
    {
        when( inFlightCache.get( startIndex ) ).thenReturn( entry );
        for ( var offset = 0; offset < otherEntries.length; offset++ )
        {
            when( inFlightCache.get( startIndex + offset + 1L ) ).thenReturn( otherEntries[offset] );
        }
    }

    private static void startingFromIndexReturnEntries( ReadableRaftLog raftLog, long startIndex,
            RaftLogEntry entry, RaftLogEntry... otherEntries ) throws IOException
    {
        var cursor = mock( RaftLogCursor.class );
        when( raftLog.getEntryCursor( startIndex ) ).thenReturn( cursor, (RaftLogCursor) null );

        var bools = new Boolean[otherEntries.length + 1];
        Arrays.fill( bools, Boolean.TRUE );
        bools[otherEntries.length] = Boolean.FALSE;

        when( cursor.next() ).thenReturn( true, bools );

        var indexes = new Long[otherEntries.length + 1];
        for ( var offset = 0; offset < indexes.length; offset++ )
        {
            indexes[offset] = startIndex + 1 + offset;
        }
        indexes[otherEntries.length] = -1L;

        when( cursor.index() ).thenReturn( startIndex, indexes );

        var raftLogEntries = Arrays.copyOf( otherEntries, otherEntries.length + 1 );
        when( cursor.get() ).thenReturn( entry, raftLogEntries );
    }

    private static void assertCacheIsUpdated( InFlightCache inFlightCache, long key, boolean clearCache )
    {
        if ( clearCache )
        {
            verify( inFlightCache ).prune( key );
        }
        else
        {
            verify( inFlightCache, never() ).prune( key );
        }
    }

    @Target( ElementType.METHOD )
    @Retention( RetentionPolicy.RUNTIME )
    @ParameterizedTest( name = "clearCache = {0}" )
    @ValueSource( booleans = {false, true} )
    @interface TestWithCacheClearing
    {
    }
}
