/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.lease;

import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.api.LeaseException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceTypes;

import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LeaderOnlyLockManagerTest
{
    @Test
    void shouldIssueLocksOnLeader()
    {
        // given
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );
        LeaseClient leaseClient = mock( LeaseClient.class );

        LeaderOnlyLockManager lockManager = new LeaderOnlyLockManager( locks );

        // when
        Locks.Client leaderLockClient = lockManager.newClient();
        leaderLockClient.initialize( leaseClient );
        leaderLockClient.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 0L );
    }

    @Test
    void shouldNotIssueLocksOnNonLeader() throws LeaseException
    {
        // given
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );
        LeaseClient leaseClient = mock( LeaseClient.class );
        doThrow( LeaseException.class ).when( leaseClient ).ensureValid();

        LeaderOnlyLockManager lockManager = new LeaderOnlyLockManager( locks );

        // when
        Locks.Client lockClient = lockManager.newClient();
        lockClient.initialize( leaseClient );
        try
        {
            lockClient.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 0L );
            fail( "Should have thrown exception" );
        }
        catch ( LeaseException e )
        {
            // expected
        }
    }
}
