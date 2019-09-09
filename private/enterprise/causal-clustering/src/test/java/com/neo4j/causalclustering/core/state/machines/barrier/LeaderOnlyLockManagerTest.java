/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.barrier;

import org.junit.Test;

import org.neo4j.kernel.impl.api.EpochException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceTypes;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LeaderOnlyLockManagerTest
{
    @Test
    public void shouldIssueLocksOnLeader()
    {
        // given
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );
        BarrierState barrierState = mock( BarrierState.class );

        LeaderOnlyLockManager lockManager = new LeaderOnlyLockManager( locks );

        // when
        Locks.Client leaderLockClient = lockManager.newClient();
        leaderLockClient.initialize( barrierState );
        leaderLockClient.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 0L );
    }

    @Test
    public void shouldNotIssueLocksOnNonLeader() throws EpochException
    {
        // given
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );
        BarrierState barrierState = mock( BarrierState.class );
        doThrow( EpochException.class ).when( barrierState ).ensureHoldingToken();

        LeaderOnlyLockManager lockManager = new LeaderOnlyLockManager( locks );

        // when
        Locks.Client lockClient = lockManager.newClient();
        lockClient.initialize( barrierState );
        try
        {
            lockClient.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 0L );
            fail( "Should have thrown exception" );
        }
        catch ( EpochException e )
        {
            // expected
        }
    }
}
