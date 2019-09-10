/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.lease;

import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.replication.DirectReplicator;
import com.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.api.LeaseException;

import static com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseState.INITIAL_LEASE_STATE;
import static com.neo4j.causalclustering.identity.RaftTestMember.member;
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

    private final MemberId myself = member( 0 );
    private final MemberId other = member( 1 );

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
        when( leaderLocator.getLeader() ).thenReturn( myself );
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
        LeaderLocator leaderLocator = mock( LeaderLocator.class );
        when( leaderLocator.getLeader() ).thenReturn( other );

        LeaseClient client = coordinator.newClient();

        // when / then
        assertThrows( LeaseException.class, client::ensureValid );
    }

    @Test
    void shouldConsiderInvalidatedLeaseInvalid() throws Exception
    {
        // given
        when( leaderLocator.getLeader() ).thenReturn( myself );
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
        when( leaderLocator.getLeader() ).thenReturn( myself );
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
        when( leaderLocator.getLeader() ).thenReturn( myself );
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
}
