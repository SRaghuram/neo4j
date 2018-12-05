/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.locks;

import org.junit.Test;

import org.neo4j.causalclustering.core.consensus.LeaderLocator;
import org.neo4j.causalclustering.core.replication.DirectReplicator;
import org.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
import org.neo4j.storageengine.api.lock.LockTracer;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;

@SuppressWarnings( "unchecked" )
public class LeaderOnlyLockManagerTest
{
    private final String databaseName = GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

    @Test
    public void shouldIssueLocksOnLeader() throws Exception
    {
        // given
        MemberId me = member( 0 );

        ReplicatedLockTokenStateMachine replicatedLockStateMachine =
                new ReplicatedLockTokenStateMachine( new InMemoryStateStorage( ReplicatedLockTokenState.INITIAL_LOCK_TOKEN ) );

        DirectReplicator replicator = new DirectReplicator( replicatedLockStateMachine );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( me );
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        LeaderOnlyLockManager lockManager =
                new LeaderOnlyLockManager( me, replicator, leaderLocator, locks, replicatedLockStateMachine, databaseName );

        // when
        lockManager.newClient().acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 0L );

        // then
    }

    @Test
    public void shouldNotIssueLocksOnNonLeader() throws Exception
    {
        // given
        MemberId me = member( 0 );
        MemberId leader = member( 1 );

        ReplicatedLockTokenStateMachine replicatedLockStateMachine =
                new ReplicatedLockTokenStateMachine( new InMemoryStateStorage( ReplicatedLockTokenState.INITIAL_LOCK_TOKEN ) );
        DirectReplicator replicator = new DirectReplicator( replicatedLockStateMachine );

        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( leader );
        Locks locks = mock( Locks.class );
        Locks.Client client = mock( Locks.Client.class );
        when( locks.newClient() ).thenReturn( client );

        LeaderOnlyLockManager lockManager =
                new LeaderOnlyLockManager( me, replicator, leaderLocator, locks, replicatedLockStateMachine, databaseName );

        // when
        Locks.Client lockClient = lockManager.newClient();
        try
        {
            lockClient.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 0L );
            fail( "Should have thrown exception" );
        }
        catch ( AcquireLockTimeoutException e )
        {
            // expected
        }
    }
}
