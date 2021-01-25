/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Simple service for providing named locks. Used by the reconciler for preventing more than one database from reconciling at a time.
 *
 * These locks must never provide re-entrancy or prevent 2 different threads from performing the acquire/release of a given lock.
 * This is because all parts of a reconciler job (including lock acquisition and release) are chained asynchronous tasks submitted to an executor
 * so 1 job may execute over several threads.
 *
 * When waking threads to attempt acquisition, threads executing priority jobs are preferred
 */
final class ReconcilerLocks
{
    private final Set<String> reconciling;
    private final Map<String,Integer> priorityWaiting;
    private final ReentrantLock lock;
    private final Condition ableToReconcile;

    ReconcilerLocks()
    {
        reconciling = new HashSet<>();
        priorityWaiting = new HashMap<>();
        lock = new ReentrantLock( true );
        ableToReconcile = lock.newCondition();
    }

    public void acquireLockOn( ReconcilerRequest request, String databaseName ) throws InterruptedException
    {
        if ( request.shouldBeExecutedAsPriorityFor( databaseName ) )
        {
            acquirePriority( databaseName );
        }
        else
        {
            acquire( databaseName );
        }
    }

    public void releaseLockOn( String databaseName )
    {
        lock.lock();
        try
        {
            if ( !reconciling.remove( databaseName ) )
            {
                throw new IllegalMonitorStateException( "No thread is currently holding a lock to database " + databaseName );
            }
            ableToReconcile.signalAll();
        }
        finally
        {
            lock.unlock();
        }
    }

    private void acquirePriority( String databaseName ) throws InterruptedException
    {
        lock.lock();
        try
        {
            incrementPriorityWaiting( databaseName );
            while ( reconciling.contains( databaseName ) )
            {
                ableToReconcile.await();
            }
            reconciling.add( databaseName );
        }
        finally
        {
            decrementPriorityWaiting( databaseName );
            lock.unlock();
        }
    }

    private void acquire( String databaseName ) throws InterruptedException
    {
        lock.lock();
        try
        {
            while ( reconciling.contains( databaseName ) || priorityWaiting.getOrDefault( databaseName, 0 ) > 0 )
            {
                ableToReconcile.await();
            }
            reconciling.add( databaseName );
        }
        finally
        {
            lock.unlock();
        }
    }

    private void incrementPriorityWaiting( String databaseName )
    {
        priorityWaiting.compute( databaseName, ( key, value ) -> value == null ? 1 : value + 1 );
    }

    private void decrementPriorityWaiting( String databaseName )
    {
        priorityWaiting.compute( databaseName, ( key, value ) -> value == null || value <= 1 ? null : value - 1 );
    }
}
