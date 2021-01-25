/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import org.junit.jupiter.api.Test;

import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;

import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.LogAssertions.assertThat;

@SuppressWarnings( "unchecked" )
class OperationProgressMonitorTest
{
    private final Supplier<OptionalLong> zeroMillisSinceLastResponse = () -> OptionalLong.of( 0 );
    private final Supplier<OptionalLong> incrementingMillisSinceLastResponse = new IncrementingLastResponseTimer( 0 );
    private final Supplier<OptionalLong> foreverSinceLastResponse = new IncrementingLastResponseTimer();
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final Log log = logProvider.getLog( OperationProgressMonitor.class );

    @Test
    void getShouldBeCalledMultipleTimesThenSucceed() throws Throwable
    {
        // given

        long inactivityTimeout = 1;
        Future<Object> future = mock( Future.class );
        when( future.get( anyLong(), any() ) )
                .thenThrow( TimeoutException.class )
                .thenReturn( "expected" );

        OperationProgressMonitor<Object> retryFuture = OperationProgressMonitor.of( future, inactivityTimeout, zeroMillisSinceLastResponse, log );

        // when
        Object actual = retryFuture.get();

        // then
        verify( future, atLeast( 2 ) ).get( anyLong(), any() );
        assertEquals( "expected", actual );
    }

    @Test
    void shouldEventuallyTimeoutAndLog() throws Throwable
    {
        // given
        //millisSinceLasResponse supplier gets called (and incremented) twice per loop, so a timeout of 5 is needed to cause 3 get() calls to the inner future.
        long inactivityTimeout = 5;
        Future<Object> future = mock( Future.class );
        when( future.get( anyLong(), any() ) )
                .thenThrow( TimeoutException.class );

        OperationProgressMonitor<Object> retryFuture = OperationProgressMonitor.of( future, inactivityTimeout, incrementingMillisSinceLastResponse, log );

        // when
        assertThrows( TimeoutException.class, retryFuture::get );

        // then
        assertThat( logProvider ).containsMessages( "Request timed out" );
        verify( future, atLeast( 3 ) ).get( anyLong(), any() );
    }

    @Test
    void shouldStopImmediatelyForNonTimeoutErrors()
    {
        // given
        long inactivityTimeout = 1;
        RuntimeException cause = new RuntimeException( "Future failed" );
        Future<Object> future = failedFuture( cause );

        OperationProgressMonitor<Object> retryFuture = OperationProgressMonitor.of( future, inactivityTimeout, zeroMillisSinceLastResponse, log );

        // when
        ExecutionException error = assertThrows( ExecutionException.class, retryFuture::get );

        // then
        assertEquals( cause, error.getCause() );
    }

    @Test
    void allErrorsShouldCancelInnerFuture() throws Throwable
    {
        long inactivityTimeout = 1;
        Future<Object> future = mock( Future.class );
        when( future.get( anyLong(), any() ) )
                .thenThrow( ExecutionException.class )
                .thenThrow( InterruptedException.class )
                .thenThrow( TimeoutException.class );

        OperationProgressMonitor<Object> retryFuture = OperationProgressMonitor.of( future, inactivityTimeout, foreverSinceLastResponse, log );

        assertThrows( ExecutionException.class, retryFuture::get );
        verify( future, times( 1 ) ).cancel( anyBoolean() );

        assertThrows( InterruptedException.class, retryFuture::get );
        verify( future, times( 2 ) ).cancel( anyBoolean() );

        assertThrows( TimeoutException.class, retryFuture::get );
        verify( future, times( 3 ) ).cancel( anyBoolean() );
    }

    private static class IncrementingLastResponseTimer implements Supplier<OptionalLong>
    {
        private long noResponseSince;
        private final boolean noResponseEver;

        IncrementingLastResponseTimer()
        {
            noResponseEver = true;
        }

        IncrementingLastResponseTimer( long initial )
        {
            noResponseEver = false;
            noResponseSince = initial;
        }

        @Override
        public OptionalLong get()
        {
            return noResponseEver ? OptionalLong.empty() : OptionalLong.of( ++noResponseSince );
        }
    }
}
