/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.locking.Locks;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.kernel.api.exceptions.Status.General.ForbiddenOnReadOnlyDatabase;
import static org.neo4j.kernel.api.exceptions.Status.statusCodeOf;
import static org.neo4j.lock.LockTracer.NONE;
import static org.neo4j.lock.ResourceTypes.NODE;

class ReadReplicaLockManagerTest
{
    private final ReadReplicaLockManager lockManager = new ReadReplicaLockManager();
    private final Locks.Client lockClient = lockManager.newClient();

    @Test
    void shouldThrowOnAcquireExclusiveLock()
    {
        var e = assertThrows( Exception.class, () -> lockClient.acquireExclusive( NONE, NODE, 1 ) );
        assertEquals( ForbiddenOnReadOnlyDatabase, statusCodeOf( e ) );
    }

    @Test
    void shouldThrowOnTryAcquireExclusiveLock()
    {
        var e = assertThrows( Exception.class, () -> lockClient.tryExclusiveLock( NODE, 1 ) );
        assertEquals( ForbiddenOnReadOnlyDatabase, statusCodeOf( e ) );
    }

    @Test
    void shouldAcceptSharedLocks()
    {
        assertDoesNotThrow( () ->
        {
            lockClient.acquireShared( NONE, NODE, 1 );
            lockClient.trySharedLock( NODE, 1 );
        } );
    }
}
