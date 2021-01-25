/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.lease;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.replication.DirectReplicator;
import com.neo4j.causalclustering.core.replication.ReplicationResult;
import com.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import org.neo4j.function.Suppliers.Lazy;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.api.LeaseException;

import static com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseState.INITIAL_LEASE_STATE;
import static com.neo4j.causalclustering.identity.RaftTestMember.lazyRaftMember;
import static com.neo4j.causalclustering.identity.RaftTestMember.leader;
import static com.neo4j.causalclustering.identity.RaftTestMember.raftMember;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.LeaseService.NO_LEASE;

class ClusterLeaseCoordinatorTest
{
    private final NamedDatabaseId namedDatabaseId = new TestDatabaseIdRepository().defaultDatabase();

    private final Lazy<RaftMemberId> myself = lazyRaftMember( 0 );
    private final RaftMemberId other = raftMember( 1 );
    private final LeaderInfo myselfAsLeader = leader( 0, 1 );
    private final LeaderInfo otherAsLeader = leader( 1, 1 );

    private final ReplicatedLeaseStateMachine stateMachine = new ReplicatedLeaseStateMachine( new InMemoryStateStorage<>( INITIAL_LEASE_STATE ) );
    private final DirectReplicator replicator = new DirectReplicator<>( stateMachine );

    private final LeaderLocator leaderLocator = mock( LeaderLocator.class );

    private final ClusterLeaseCoordinator coordinator = new ClusterLeaseCoordinator( myself, replicator, leaderLocator, stateMachine, namedDatabaseId );

    @Test
    void shouldHaveNoLeaseInitially()
    {
        LeaseClient client = coordinator.newClient();
        assertEquals( NO_LEASE, client.leaseId() );
    }

    @Test
    void shouldAcquireLeaseAsLeader() throws Exception
    {
        // given
        when( leaderLocator.getLeaderInfo() ).thenReturn( Optional.of( myselfAsLeader ) );
        LeaseClient client = coordinator.newClient();

        // when
        client.ensureValid();

        // then
        assertEquals( 0, client.leaseId() );
    }

    @Test
    void shouldNotAcquireLeaseWhenNotLeader() throws Exception
    {
        // given
        when( leaderLocator.getLeaderInfo() ).thenReturn( Optional.of( otherAsLeader ) );

        LeaseClient client = coordinator.newClient();

        // when / then
        assertThrows( LeaseException.class, client::ensureValid );
    }

    @Test
    void shouldConsiderInvalidatedLeaseInvalid() throws Exception
    {
        // given
        when( leaderLocator.getLeaderInfo() ).thenReturn( Optional.of( myselfAsLeader ) );
        LeaseClient client = coordinator.newClient();

        client.ensureValid();
        assertFalse( coordinator.isInvalid( client.leaseId() ) );

        // when
        coordinator.invalidateLease( client.leaseId() );

        // then
        assertTrue( coordinator.isInvalid( client.leaseId() ) );
    }

    @Test
    void shouldReturnValidLeaseToNewClient() throws Exception
    {
        // given
        when( leaderLocator.getLeaderInfo() ).thenReturn( Optional.of( myselfAsLeader ) );
        LeaseClient clientA = coordinator.newClient();
        clientA.ensureValid();

        // when
        LeaseClient clientB = coordinator.newClient();
        clientB.ensureValid();

        // then
        assertEquals( clientA.leaseId(), clientB.leaseId() );
        assertFalse( coordinator.isInvalid( clientA.leaseId() ) );
        assertFalse( coordinator.isInvalid( clientB.leaseId() ) );
    }

    @Test
    void shouldNotReturnInvalidatedLeaseToNewClient() throws Exception
    {
        // given
        when( leaderLocator.getLeaderInfo() ).thenReturn( Optional.of( myselfAsLeader ) );
        LeaseClient clientA = coordinator.newClient();
        clientA.ensureValid();
        assertEquals( 0, clientA.leaseId() );

        // when
        coordinator.invalidateLease( clientA.leaseId() );

        LeaseClient clientB = coordinator.newClient();
        clientB.ensureValid();

        // then
        assertEquals( 1, clientB.leaseId() );
        assertTrue( coordinator.isInvalid( clientA.leaseId() ) );
        assertFalse( coordinator.isInvalid( clientB.leaseId() ) );

        // when
        coordinator.invalidateLease( clientB.leaseId() );

        LeaseClient clientC = coordinator.newClient();
        clientC.ensureValid();

        // then
        assertEquals( 2, clientC.leaseId() );
        assertTrue( coordinator.isInvalid( clientA.leaseId() ) );
        assertTrue( coordinator.isInvalid( clientB.leaseId() ) );
        assertFalse( coordinator.isInvalid( clientC.leaseId() ) );
    }

    @Test
    void shouldConsiderLeaseFromPreviousLifeInvalid() throws Exception
    {
        // given
        when( leaderLocator.getLeaderInfo() ).thenReturn( Optional.of( myselfAsLeader ) );
        ReplicatedLeaseRequest oldLease = new ReplicatedLeaseRequest( myself.get(), 0, namedDatabaseId.databaseId() );
        ReplicationResult result = replicator.replicate( oldLease );
        assertEquals( true, result.stateMachineResult().consume() );

        // when
        LeaseClient clientA = coordinator.newClient();
        clientA.ensureValid();

        // then
        assertEquals( 1, clientA.leaseId() );
        assertFalse( coordinator.isInvalid( 1 ) );
    }

    @Test
    void shouldAllocateAfterSwitch() throws Exception
    {
        // when
        when( leaderLocator.getLeaderInfo() ).thenReturn( Optional.of( myselfAsLeader ) );
        LeaseClient clientA = coordinator.newClient();
        clientA.ensureValid();
        assertEquals( 0, clientA.leaseId() );

        // when
        replicator.replicate( new ReplicatedLeaseRequest( other, 1, namedDatabaseId.databaseId() ) );

        // then
        assertThrows( LeaseException.class, clientA::ensureValid );

        // when
        LeaseClient clientB = coordinator.newClient();
        clientB.ensureValid();

        // then
        assertEquals( 2, clientB.leaseId() );
    }
}
