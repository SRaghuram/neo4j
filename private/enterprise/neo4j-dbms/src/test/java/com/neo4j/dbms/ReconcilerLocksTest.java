/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

class ReconcilerLocksTest
{
    private final JobScheduler scheduler = new ThreadPoolJobScheduler();

    @AfterEach
    void cleanup() throws Exception
    {
        scheduler.shutdown();
    }

    @Test
    void shouldCorrectlyBlockAcquireForLockedDatabases() throws InterruptedException
    {
        // given
        var locks = new ReconcilerLocks();
        var executor = scheduler.executor( Group.DATABASE_RECONCILER );
        var started = new CountDownLatch( 1 );
        var stopped = new CountDownLatch( 1 );

        // when
        locks.acquireLockOn( ReconcilerRequest.simple(), "foo" );
        executor.execute( () ->
        {
            try
            {
                started.countDown();
                locks.acquireLockOn( ReconcilerRequest.simple(), "foo" );
                stopped.countDown();
            }
            catch ( InterruptedException e )
            {
               throw new AssertionError( e );
            }
        } );

        // then
        started.await();
        assertFalse( stopped.await( 500, MILLISECONDS ) );
        locks.releaseLockOn( "foo" );
        stopped.await();
    }

    @Test
    void shouldAllowDifferentThreadsToLockUnlock() throws InterruptedException, TimeoutException, ExecutionException
    {
        // given
        var locks = new ReconcilerLocks();
        var executor = scheduler.executor( Group.DATABASE_RECONCILER );

        // when
        locks.acquireLockOn( ReconcilerRequest.simple(), "foo" );
        executor.execute( () -> locks.releaseLockOn( "foo" ) );

        // then
        var reLock = CompletableFuture.runAsync( () ->
        {
            try
            {
                locks.acquireLockOn( ReconcilerRequest.simple(), "foo" );
            }
            catch ( InterruptedException e )
            {
                throw new CompletionException( e );
            }
        }, executor );

        reLock.get( 30, SECONDS );
        assertTrue( reLock.isDone() );
        assertFalse( reLock.isCompletedExceptionally() );
    }

    @Test
    void shouldThrowIfReleasingANonExistentLock()
    {
        // given
        var locks = new ReconcilerLocks();

        // when/then
        assertThrows( IllegalMonitorStateException.class, () -> locks.releaseLockOn( "foo" ) );
    }

    @Test
    void shouldPreferAcquireByPriorityRequest() throws InterruptedException
    {
        // given
        var locks = new ReconcilerLocks();
        var executor = scheduler.executor( Group.DATABASE_RECONCILER );
        var foo = DatabaseIdFactory.from( "foo", UUID.randomUUID() );

        var markerA = mock( AcquiredMarker.class );
        var markerB = mock( AcquiredMarker.class );
        var order = inOrder( markerA, markerB );
        var acquired = new CountDownLatch( 2 );
        var acquiring = new CountDownLatch( 2 );

        locks.acquireLockOn( ReconcilerRequest.simple(), foo.name() );

        executor.execute( () ->
        {
            try
            {
                acquiring.countDown();
                locks.acquireLockOn( ReconcilerRequest.simple(), foo.name() );
                markerB.acquired();
                locks.releaseLockOn( foo.name() );
            }
            catch ( InterruptedException e )
            {
                throw new AssertionError( e );
            }
            finally
            {
                acquired.countDown();
            }
        } );

        executor.execute( () ->
        {
            try
            {
                acquiring.countDown();
                locks.acquireLockOn( ReconcilerRequest.priorityTarget( foo ).build(), foo.name() );
                markerA.acquired();
                locks.releaseLockOn( foo.name() );
            }
            catch ( InterruptedException e )
            {
                throw new AssertionError( e );
            }
            finally
            {
                acquired.countDown();
            }
        } );

        // when/then
        acquiring.await();
        Thread.sleep( 500 );
        locks.releaseLockOn( foo.name() );

        // then
        assertTrue( acquired.await( 30, SECONDS ) );
        order.verify( markerA ).acquired();
        order.verify( markerB ).acquired();
    }

    private interface AcquiredMarker
    {
        void acquired();
    }
}
