/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.lease;

import com.neo4j.causalclustering.core.state.CoreStateFiles;
import com.neo4j.causalclustering.core.state.storage.DurableStateStorage;
import com.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import com.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.state.StateStorage;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectorySupportExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.identity.RaftTestMember.raftMember;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.api.LeaseService.NO_LEASE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectorySupportExtension.class} )
class ReplicatedLeaseStateMachineTest
{
    private final NamedDatabaseId namedDatabaseId = new TestDatabaseIdRepository().defaultDatabase();
    private final DatabaseId databaseId = namedDatabaseId.databaseId();

    @Inject
    private EphemeralFileSystemAbstraction fsa;

    @Inject
    public TestDirectory testDir;

    @Test
    void shouldStartWithSpecifiedState()
    {
        // given
        ReplicatedLeaseStateMachine stateMachine = new ReplicatedLeaseStateMachine(
                new InMemoryStateStorage<>( ReplicatedLeaseState.INITIAL_LEASE_STATE ) );

        // when
        ReplicatedLeaseState state = stateMachine.snapshot();

        // then
        assertNull( state.owner() );
        assertEquals( NO_LEASE, state.leaseId() );
        assertEquals( -1, state.ordinal() );
    }

    @Test
    void shouldIssueNextLeaseCandidateId()
    {
        // given
        ReplicatedLeaseStateMachine stateMachine = new ReplicatedLeaseStateMachine(
                new InMemoryStateStorage<>( ReplicatedLeaseState.INITIAL_LEASE_STATE ) );
        int firstCandidateId = Lease.nextCandidateId( stateMachine.snapshot().leaseId() );

        // when
        stateMachine.applyCommand( new ReplicatedLeaseRequest( raftMember( 0 ), firstCandidateId, databaseId ), 0, r -> {} );

        // then
        assertEquals( firstCandidateId + 1, Lease.nextCandidateId( stateMachine.snapshot().leaseId() ) );
    }

    @Test
    void shouldKeepTrackOfCurrentLeaseId()
    {
        // given
        ReplicatedLeaseStateMachine stateMachine = new ReplicatedLeaseStateMachine(
                new InMemoryStateStorage<>( ReplicatedLeaseState.INITIAL_LEASE_STATE ) );
        int firstCandidateId = Lease.nextCandidateId( stateMachine.snapshot().leaseId() );

        // when
        stateMachine.applyCommand( new ReplicatedLeaseRequest( raftMember( 0 ), firstCandidateId, databaseId ), 1, r -> {} );

        // then
        assertEquals( firstCandidateId, stateMachine.snapshot().leaseId() );

        // when
        stateMachine.applyCommand( new ReplicatedLeaseRequest( raftMember( 0 ), firstCandidateId + 1, databaseId ), 2, r -> {} );

        // then
        assertEquals( firstCandidateId + 1, stateMachine.snapshot().leaseId() );
    }

    @Test
    void shouldKeepTrackOfLeaseOwner()
    {
        // given
        ReplicatedLeaseStateMachine stateMachine = new ReplicatedLeaseStateMachine(
                new InMemoryStateStorage<>( ReplicatedLeaseState.INITIAL_LEASE_STATE ) );
        int firstCandidateId = Lease.nextCandidateId( stateMachine.snapshot().leaseId() );

        // when
        stateMachine.applyCommand( new ReplicatedLeaseRequest( raftMember( 0 ), firstCandidateId, databaseId ), 1, r -> {} );

        // then
        assertEquals( raftMember( 0 ), stateMachine.snapshot().owner() );

        // when
        stateMachine.applyCommand( new ReplicatedLeaseRequest( raftMember( 1 ), firstCandidateId + 1, databaseId ), 2, r -> {} );

        // then
        assertEquals( raftMember( 1 ), stateMachine.snapshot().owner() );
    }

    @Test
    void shouldAcceptOnlyFirstRequestWithSameId()
    {
        // given
        ReplicatedLeaseStateMachine stateMachine = new ReplicatedLeaseStateMachine(
                new InMemoryStateStorage<>( ReplicatedLeaseState.INITIAL_LEASE_STATE ) );
        int firstCandidateId = Lease.nextCandidateId( stateMachine.snapshot().leaseId() );

        // when
        stateMachine.applyCommand( new ReplicatedLeaseRequest( raftMember( 0 ), firstCandidateId, databaseId ), 1, r -> {} );
        stateMachine.applyCommand( new ReplicatedLeaseRequest( raftMember( 1 ), firstCandidateId, databaseId ), 2, r -> {} );

        // then
        assertEquals( 0, stateMachine.snapshot().leaseId() );
        assertEquals( raftMember( 0 ), stateMachine.snapshot().owner() );

        // when
        stateMachine.applyCommand( new ReplicatedLeaseRequest( raftMember( 1 ), firstCandidateId + 1, databaseId ), 3, r -> {} );
        stateMachine.applyCommand( new ReplicatedLeaseRequest( raftMember( 0 ), firstCandidateId + 1, databaseId ), 4, r -> {} );

        // then
        assertEquals( 1, stateMachine.snapshot().leaseId() );
        assertEquals( raftMember( 1 ), stateMachine.snapshot().owner() );
    }

    @Test
    void shouldOnlyAcceptNextImmediateId()
    {
        // given
        ReplicatedLeaseStateMachine stateMachine = new ReplicatedLeaseStateMachine(
                new InMemoryStateStorage<>( ReplicatedLeaseState.INITIAL_LEASE_STATE ) );
        int firstCandidateId = Lease.nextCandidateId( stateMachine.snapshot().leaseId() );

        // when
        stateMachine.applyCommand( new ReplicatedLeaseRequest( raftMember( 0 ), firstCandidateId + 1, databaseId ), 1, r -> {} ); // not accepted

        // then
        assertEquals( NO_LEASE, stateMachine.snapshot().leaseId() );

        // when
        stateMachine.applyCommand( new ReplicatedLeaseRequest( raftMember( 0 ), firstCandidateId, databaseId ), 2, r -> {} ); // accepted

        // then
        assertEquals( stateMachine.snapshot().leaseId(), firstCandidateId );

        // when
        stateMachine.applyCommand( new ReplicatedLeaseRequest( raftMember( 0 ), firstCandidateId + 1, databaseId ), 3, r -> {} ); // accepted

        // then
        assertEquals( stateMachine.snapshot().leaseId(), firstCandidateId + 1 );

        // when
        stateMachine.applyCommand( new ReplicatedLeaseRequest( raftMember( 0 ), firstCandidateId, databaseId ), 4, r -> {} ); // not accepted

        // then
        assertEquals( stateMachine.snapshot().leaseId(), firstCandidateId + 1 );

        // when
        stateMachine.applyCommand( new ReplicatedLeaseRequest( raftMember( 0 ), firstCandidateId + 3, databaseId ), 5, r -> {} ); // not accepted

        // then
        assertEquals( stateMachine.snapshot().leaseId(), firstCandidateId + 1 );
    }

    @Test
    void shouldPersistAndRecoverState() throws Exception
    {
        // given
        fsa.mkdir( testDir.homeDir() );

        SafeStateMarshal<ReplicatedLeaseState> marshal = new ReplicatedLeaseState.Marshal();

        var memberA = raftMember( 0 );
        var memberB = raftMember( 1 );
        int candidateId;

        DurableStateStorage<ReplicatedLeaseState> storage = new DurableStateStorage<>( fsa, testDir.homePath(),
                CoreStateFiles.DUMMY( marshal ), 100, NullLogProvider.getInstance(), INSTANCE );
        try ( Lifespan ignored = new Lifespan( storage ) )
        {
            ReplicatedLeaseStateMachine stateMachine = new ReplicatedLeaseStateMachine( storage );

            // when
            candidateId = 0;
            stateMachine.applyCommand( new ReplicatedLeaseRequest( memberA, candidateId, databaseId ), 0, r -> {} );
            candidateId = 1;
            stateMachine.applyCommand( new ReplicatedLeaseRequest( memberB, candidateId, databaseId ), 1, r -> {} );

            stateMachine.flush();
            fsa.crash();
        }

        // then
        DurableStateStorage<ReplicatedLeaseState> storage2 = new DurableStateStorage<>( fsa, testDir.homePath(),
                CoreStateFiles.DUMMY( marshal ), 100, NullLogProvider.getInstance(), INSTANCE );
        try ( Lifespan ignored = new Lifespan( storage2 ) )
        {
            ReplicatedLeaseState initialState = storage2.getInitialState();
            ReplicatedLeaseRequest request = new ReplicatedLeaseRequest( initialState, namedDatabaseId );
            assertEquals( memberB, request.owner() );
            assertEquals( candidateId, request.id() );
        }
    }

    @Test
    void shouldBeIdempotent()
    {
        // given
        fsa.mkdir( testDir.homeDir() );

        SafeStateMarshal<ReplicatedLeaseState> marshal = new ReplicatedLeaseState.Marshal();

        DurableStateStorage<ReplicatedLeaseState> storage = new DurableStateStorage<>( fsa, testDir.homePath(),
                CoreStateFiles.DUMMY( marshal ), 100, NullLogProvider.getInstance(), INSTANCE );

        try ( Lifespan ignored = new Lifespan( storage ) )
        {
            ReplicatedLeaseStateMachine stateMachine = new ReplicatedLeaseStateMachine( storage );

            var memberA = raftMember( 0 );
            var memberB = raftMember( 1 );

            stateMachine.applyCommand( new ReplicatedLeaseRequest( memberA, 0, databaseId ), 3, r ->
            {
            } );

            // when
            stateMachine.applyCommand( new ReplicatedLeaseRequest( memberB, 1, databaseId ), 2, r ->
            {
            } );

            // then
            assertEquals( memberA, stateMachine.snapshot().owner() );
        }
    }

    @Test
    void shouldSetInitialPendingRequestToInitialState()
    {
        // Given
        @SuppressWarnings( "unchecked" )
        StateStorage<ReplicatedLeaseState> storage = mock( StateStorage.class );
        var initialHoldingCoreMember = raftMember( 0 );
        ReplicatedLeaseState initialState = new ReplicatedLeaseState( 123,
                new ReplicatedLeaseRequest( initialHoldingCoreMember, 3, databaseId ) );
        when( storage.getInitialState() ).thenReturn( initialState );
        ReplicatedLeaseRequest initialRequest = new ReplicatedLeaseRequest( initialState, namedDatabaseId );
        // When
        ReplicatedLeaseStateMachine stateMachine = new ReplicatedLeaseStateMachine( storage );

        // Then
        ReplicatedLeaseState state = stateMachine.snapshot();
        Lease lease = new ReplicatedLeaseRequest( state, namedDatabaseId );
        assertEquals( initialRequest.owner(), lease.owner() );
        assertEquals( initialRequest.id(), lease.id() );
    }
}
