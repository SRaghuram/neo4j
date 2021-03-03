/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.api.exceptions.ReadOnlyDbException;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.AcquireLockTimeoutException;
import org.neo4j.lock.ActiveLock;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceType;
import org.neo4j.memory.MemoryTracker;

public class ReadReplicaLockManager implements Locks
{
    @Override
    public Locks.Client newClient()
    {
        return new Client();
    }

    @Override
    public void accept( Visitor visitor )
    {
    }

    @Override
    public void close()
    {
    }

    private static class Client implements Locks.Client
    {
        @Override
        public void initialize( LeaseClient leaseClient, long transactionId, MemoryTracker memoryTracker, Config config )
        {
        }

        @Override
        public void acquireShared( LockTracer tracer, ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
        {
        }

        @Override
        public void acquireExclusive( LockTracer tracer, ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
        {
            throw new RuntimeException( new ReadOnlyDbException() );
        }

        @Override
        public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
        {
            throw new RuntimeException( new ReadOnlyDbException() );
        }

        @Override
        public boolean trySharedLock( ResourceType resourceType, long resourceId )
        {
            return false;
        }

        @Override
        public void releaseShared( ResourceType resourceType, long... resourceIds )
        {
        }

        @Override
        public void releaseExclusive( ResourceType resourceType, long... resourceIds )
        {
            throw new IllegalStateException( "Should never happen" );
        }

        @Override
        public void prepareForCommit()
        {
        }

        @Override
        public void stop()
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        public int getLockSessionId()
        {
            return 0;
        }

        @Override
        public Stream<ActiveLock> activeLocks()
        {
            return Stream.empty();
        }

        @Override
        public long activeLockCount()
        {
            return 0;
        }
    }
}
