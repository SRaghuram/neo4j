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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Simple service for providing named locks.
 * Used by the reconciler for preventing more than one database from reconciling at a time.
 */
final class ReconcilerLocks
{
    private final Set<String> reconciling;
    private final ReentrantLock lock;
    private final Condition ableToReconcile;

    ReconcilerLocks()
    {
        reconciling = new HashSet<>();
        lock = new ReentrantLock( true );
        ableToReconcile = lock.newCondition();
    }

    public void acquireLockOn( String databaseName ) throws InterruptedException
    {
        lock.lock();
        try
        {
            while( reconciling.contains( databaseName ) )
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

    public void releaseLockOn( String databaseName )
    {
        lock.lock();
        try
        {
            reconciling.remove( databaseName );
            ableToReconcile.signalAll();
        }
        finally
        {
            lock.unlock();
        }
    }
}
