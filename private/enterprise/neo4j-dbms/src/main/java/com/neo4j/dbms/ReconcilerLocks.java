/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
        if ( request.isPriorityRequestForDatabase( databaseName ) )
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
            while( reconciling.contains( databaseName ) || priorityWaiting.getOrDefault( databaseName, 0 ) > 0 )
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
