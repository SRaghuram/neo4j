/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import org.junit.Test;

import org.neo4j.kernel.impl.locking.Locks;

import static org.junit.Assert.assertEquals;
import static org.neo4j.kernel.api.exceptions.Status.General.ForbiddenOnReadOnlyDatabase;
import static org.neo4j.kernel.api.exceptions.Status.statusCodeOf;
import static org.neo4j.kernel.impl.locking.ResourceTypes.NODE;
import static org.neo4j.storageengine.api.lock.LockTracer.NONE;

public class ReadReplicaLockManagerTest
{
    private ReadReplicaLockManager lockManager = new ReadReplicaLockManager();
    private Locks.Client lockClient = lockManager.newClient();

    @Test
    public void shouldThrowOnAcquireExclusiveLock()
    {
        try
        {
            // when
            lockClient.acquireExclusive( NONE, NODE, 1 );
        }
        catch ( Exception e )
        {
            assertEquals( ForbiddenOnReadOnlyDatabase, statusCodeOf( e ) );
        }
    }

    @Test
    public void shouldThrowOnTryAcquireExclusiveLock()
    {
        try
        {
            // when
            lockClient.tryExclusiveLock( NODE, 1 );
        }
        catch ( Exception e )
        {
            assertEquals( ForbiddenOnReadOnlyDatabase, statusCodeOf( e ) );
        }
    }

    @Test
    public void shouldAcceptSharedLocks()
    {
        lockClient.acquireShared( NONE, NODE, 1 );
        lockClient.trySharedLock( NODE, 1 );
    }
}
