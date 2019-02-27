/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.helper;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SuppressWarnings( "unchecked" )
class OperationProgressMonitorTest
{

    private final Supplier<Optional<Long>> zeroMillisSinceLastResponse = () -> Optional.of( 0L );
    private final Supplier<Optional<Long>> incrementingMillisSinceLastResponse = new IncrementingLastResponseTimer( 0 );
    private final Supplier<Optional<Long>> foreverSinceLastResponse = new IncrementingLastResponseTimer();
    private final Log log = NullLog.getInstance();

    @Test
    void getShouldBeCalledMultipleTimesThenSucceed() throws Throwable
    {
        // given

        long inactivityTimeout = 1;
        Future<Object> future = mock( Future.class );
        when( future.get( anyLong(), any() ) )
                .thenThrow( TimeoutException.class )
                .thenReturn( "expected" );

        OperationProgressMonitor<Object> monitoredFuture = OperationProgressMonitor.of( future, inactivityTimeout, zeroMillisSinceLastResponse, log );

        // when
        Object actual = monitoredFuture.get();

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

        OperationProgressMonitor<Object> monitoredFuture = OperationProgressMonitor.of( future, inactivityTimeout, incrementingMillisSinceLastResponse, log );
        AssertableLogProvider logProvider = new AssertableLogProvider();

        // when
        try
        {
            monitoredFuture.get( FooException::new, logProvider.getLog( this.getClass() ) );
            fail( "Should have eventually timed out and thrown!" );
        }
        catch ( FooException e )
        { //do nothing
        }

        // then
        logProvider.assertContainsLogCallContaining( "Request timed out" );
        verify( future, atLeast( 3 ) ).get( anyLong(), any() );
    }

    @Test
    void shouldStopImmediatelyForNonTimeoutErrors() throws Throwable
    {
        // given
        long inactivityTimeout = 1;
        Future<Object> future = mock( Future.class );
        when( future.get( anyLong(), any() ) )
                .thenThrow( ExecutionException.class );

        OperationProgressMonitor<Object> monitoredFuture = OperationProgressMonitor.of( future, inactivityTimeout, zeroMillisSinceLastResponse, log );

        // when
        try
        {
            monitoredFuture.get( FooException::new, NullLog.getInstance() );
            fail( "Should have thrown immediately!" );
        }
        catch ( FooException e )
        { //do nothing
        }

        // then
        verify( future, times( 1 ) ).get( anyLong(), any() );
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

        OperationProgressMonitor<Object> monitoredFuture = OperationProgressMonitor.of( future, inactivityTimeout, foreverSinceLastResponse, log );

        for ( int i = 0; i < 3; i++ )
        {
            // when
            try
            {
                monitoredFuture.get( FooException::new, NullLog.getInstance() );
                fail( "Should have thrown immediately!" );
            }
            catch ( FooException e )
            { //do nothing
            }
            finally
            {
                // make sure interruption status of the current thread is cleared
                // retrier re-interrupts the thread when InterruptedException is thrown
                Thread.interrupted();
            }

            // then
            verify( future, times( i + 1 ) ).cancel( anyBoolean() );
        }
    }

    private static class IncrementingLastResponseTimer implements Supplier<Optional<Long>>
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

        public Optional<Long> get()
        {
            return noResponseEver ? Optional.empty() : Optional.of( ++noResponseSince );
        }
    }

    private static class FooException extends Exception
    {
        FooException( Throwable cause )
        {
            super( cause );
        }
    }
}
