/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.locking;

import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.kernel.impl.locking.LockClientStoppedException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.AcquireLockTimeoutException;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceType;

import static java.util.function.Function.identity;
import static org.neo4j.lock.LockType.EXCLUSIVE;
import static org.neo4j.lock.LockType.SHARED;

public class DeferringLockClient implements Locks.Client
{
    private final Locks.Client clientDelegate;
    private final Map<LockUnit,MutableInt> locks = new TreeMap<>();
    private volatile boolean stopped;

    public DeferringLockClient( Locks.Client clientDelegate )
    {
        this.clientDelegate = clientDelegate;
    }

    @Override
    public void initialize( LeaseClient leaseClient )
    {
        // we don't need leases here
    }

    @Override
    public void acquireShared( LockTracer tracer, ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
    {
        assertNotStopped();

        for ( long resourceId : resourceIds )
        {
            addLock( resourceType, SHARED, resourceId );
        }
    }

    @Override
    public void acquireExclusive( LockTracer tracer, ResourceType resourceType, long... resourceIds )
            throws AcquireLockTimeoutException
    {
        assertNotStopped();

        for ( long resourceId : resourceIds )
        {
            addLock( resourceType, EXCLUSIVE, resourceId );
        }
    }

    @Override
    public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
    {
        throw new UnsupportedOperationException( "Should not be needed" );
    }

    @Override
    public boolean trySharedLock( ResourceType resourceType, long resourceId )
    {
        throw new UnsupportedOperationException( "Should not be needed" );
    }

    @Override
    public boolean reEnterShared( ResourceType resourceType, long resourceId )
    {
        throw new UnsupportedOperationException( "Should not be needed" );
    }

    @Override
    public boolean reEnterExclusive( ResourceType resourceType, long resourceId )
    {
        throw new UnsupportedOperationException( "Should not be needed" );
    }

    @Override
    public void releaseShared( ResourceType resourceType, long... resourceIds )
    {
        assertNotStopped();
        for ( long resourceId : resourceIds )
        {
            removeLock( resourceType, SHARED, resourceId );
        }

    }

    @Override
    public void releaseExclusive( ResourceType resourceType, long... resourceIds )
    {
        assertNotStopped();
        for ( long resourceId : resourceIds )
        {
            removeLock( resourceType, EXCLUSIVE, resourceId );
        }
    }

    void acquireDeferredLocks( LockTracer lockTracer )
    {
        assertNotStopped();

        long[] current = new long[10];
        int cursor = 0;
        ResourceType currentType = null;
        boolean currentExclusive = false;
        for ( LockUnit lockUnit : locks.keySet() )
        {
            if ( currentType == null ||
                 (currentType.typeId() != lockUnit.resourceType().typeId() ||
                  currentExclusive != lockUnit.isExclusive()) )
            {
                // New type, i.e. flush the current array down to delegate in one call
                flushLocks( lockTracer, current, cursor, currentType, currentExclusive );

                cursor = 0;
                currentType = lockUnit.resourceType();
                currentExclusive = lockUnit.isExclusive();
            }

            // Queue into current batch
            if ( cursor == current.length )
            {
                current = Arrays.copyOf( current, cursor * 2 );
            }
            current[cursor++] = lockUnit.resourceId();
        }
        flushLocks( lockTracer, current, cursor, currentType, currentExclusive );
    }

    private void flushLocks( LockTracer lockTracer, long[] current, int cursor, ResourceType currentType, boolean
            exclusive )
    {
        if ( cursor > 0 )
        {
            long[] resourceIds = Arrays.copyOf( current, cursor );
            if ( exclusive )
            {
                clientDelegate.acquireExclusive( lockTracer, currentType, resourceIds );
            }
            else
            {
                clientDelegate.acquireShared( lockTracer, currentType, resourceIds );
            }
        }
    }

    @Override
    public void prepare()
    {
        clientDelegate.prepare();
    }

    @Override
    public void stop()
    {
        stopped = true;
        clientDelegate.stop();
    }

    @Override
    public void close()
    {
        stopped = true;
        clientDelegate.close();
    }

    @Override
    public int getLockSessionId()
    {
        return clientDelegate.getLockSessionId();
    }

    @Override
    public Stream<ActiveLock> activeLocks()
    {
        return locks.keySet().stream().map( identity() );
    }

    @Override
    public long activeLockCount()
    {
        return locks.size();
    }

    private void assertNotStopped()
    {
        if ( stopped )
        {
            throw new LockClientStoppedException( this );
        }
    }

    private void addLock( ResourceType resourceType, LockType lockType, long resourceId )
    {
        LockUnit lockUnit = new LockUnit( resourceType, lockType, resourceId );
        MutableInt lockCount = locks.computeIfAbsent( lockUnit, k -> new MutableInt() );
        lockCount.increment();
    }

    private void removeLock( ResourceType resourceType, LockType lockType, long resourceId )
    {
        LockUnit lockUnit = new LockUnit( resourceType, lockType, resourceId );
        MutableInt lockCount = locks.get( lockUnit );
        if ( lockCount == null )
        {
            throw new IllegalStateException(
                    "Cannot release " + lockType + " lock that it " +
                    "does not hold: " + resourceType + "[" + resourceId + "]." );
        }

        lockCount.decrement();

        if ( lockCount.intValue() == 0 )
        {
            locks.remove( lockUnit );
        }
    }
}
