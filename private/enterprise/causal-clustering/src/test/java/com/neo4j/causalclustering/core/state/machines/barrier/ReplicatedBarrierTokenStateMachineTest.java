/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.barrier;

import com.neo4j.causalclustering.core.state.CoreStateFiles;
import com.neo4j.causalclustering.core.state.storage.DurableStateStorage;
import com.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import com.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import com.neo4j.causalclustering.core.state.storage.StateStorage;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReplicatedBarrierTokenStateMachineTest
{
    private final DatabaseId databaseId = new TestDatabaseIdRepository().defaultDatabase();

    @Rule
    public final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Test
    public void shouldStartWithInvalidTokenId()
    {
        // given
        ReplicatedBarrierTokenStateMachine stateMachine = new ReplicatedBarrierTokenStateMachine(
                new InMemoryStateStorage<>( ReplicatedBarrierTokenState.INITIAL_BARRIER_TOKEN ) );

        // when
        int initialTokenId = stateMachine.snapshot().candidateId();

        // then
        assertEquals( initialTokenId, BarrierToken.INVALID_BARRIER_TOKEN_ID );
    }

    @Test
    public void shouldIssueNextLockTokenCandidateId()
    {
        // given
        ReplicatedBarrierTokenStateMachine stateMachine = new ReplicatedBarrierTokenStateMachine(
                new InMemoryStateStorage<>( ReplicatedBarrierTokenState.INITIAL_BARRIER_TOKEN ) );
        int firstCandidateId = BarrierToken.nextCandidateId( stateMachine.snapshot().candidateId() );

        // when
        stateMachine.applyCommand( new ReplicatedBarrierTokenRequest( member( 0 ), firstCandidateId, databaseId ), 0, r -> {} );

        // then
        assertEquals( firstCandidateId + 1, BarrierToken.nextCandidateId( stateMachine.snapshot().candidateId() ) );
    }

    @Test
    public void shouldKeepTrackOfCurrentLockTokenId()
    {
        // given
        ReplicatedBarrierTokenStateMachine stateMachine = new ReplicatedBarrierTokenStateMachine(
                new InMemoryStateStorage<>( ReplicatedBarrierTokenState.INITIAL_BARRIER_TOKEN ) );
        int firstCandidateId = BarrierToken.nextCandidateId( stateMachine.snapshot().candidateId() );

        // when
        stateMachine.applyCommand( new ReplicatedBarrierTokenRequest( member( 0 ), firstCandidateId, databaseId ), 1, r -> {} );

        // then
        assertEquals( firstCandidateId, stateMachine.snapshot().candidateId() );

        // when
        stateMachine.applyCommand( new ReplicatedBarrierTokenRequest( member( 0 ), firstCandidateId + 1, databaseId ), 2, r -> {} );

        // then
        assertEquals( firstCandidateId + 1, stateMachine.snapshot().candidateId() );
    }

    @Test
    public void shouldKeepTrackOfLockTokenOwner()
    {
        // given
        ReplicatedBarrierTokenStateMachine stateMachine = new ReplicatedBarrierTokenStateMachine(
                new InMemoryStateStorage<>( ReplicatedBarrierTokenState.INITIAL_BARRIER_TOKEN ) );
        int firstCandidateId = BarrierToken.nextCandidateId( stateMachine.snapshot().candidateId() );

        // when
        stateMachine.applyCommand( new ReplicatedBarrierTokenRequest( member( 0 ), firstCandidateId, databaseId ), 1, r -> {} );

        // then
        assertEquals( member( 0 ), stateMachine.snapshot().owner() );

        // when
        stateMachine.applyCommand( new ReplicatedBarrierTokenRequest( member( 1 ), firstCandidateId + 1, databaseId ), 2, r -> {} );

        // then
        assertEquals( member( 1 ), stateMachine.snapshot().owner() );
    }

    @Test
    public void shouldAcceptOnlyFirstRequestWithSameId()
    {
        // given
        ReplicatedBarrierTokenStateMachine stateMachine = new ReplicatedBarrierTokenStateMachine(
                new InMemoryStateStorage<>( ReplicatedBarrierTokenState.INITIAL_BARRIER_TOKEN ) );
        int firstCandidateId = BarrierToken.nextCandidateId( stateMachine.snapshot().candidateId() );

        // when
        stateMachine.applyCommand( new ReplicatedBarrierTokenRequest( member( 0 ), firstCandidateId, databaseId ), 1, r -> {} );
        stateMachine.applyCommand( new ReplicatedBarrierTokenRequest( member( 1 ), firstCandidateId, databaseId ), 2, r -> {} );

        // then
        assertEquals( 0, stateMachine.snapshot().candidateId() );
        assertEquals( member( 0 ), stateMachine.snapshot().owner() );

        // when
        stateMachine.applyCommand( new ReplicatedBarrierTokenRequest( member( 1 ), firstCandidateId + 1, databaseId ), 3, r -> {} );
        stateMachine.applyCommand( new ReplicatedBarrierTokenRequest( member( 0 ), firstCandidateId + 1, databaseId ), 4, r -> {} );

        // then
        assertEquals( 1, stateMachine.snapshot().candidateId() );
        assertEquals( member( 1 ), stateMachine.snapshot().owner() );
    }

    @Test
    public void shouldOnlyAcceptNextImmediateId()
    {
        // given
        ReplicatedBarrierTokenStateMachine stateMachine = new ReplicatedBarrierTokenStateMachine(
                new InMemoryStateStorage<>( ReplicatedBarrierTokenState.INITIAL_BARRIER_TOKEN ) );
        int firstCandidateId = BarrierToken.nextCandidateId( stateMachine.snapshot().candidateId() );

        // when
        stateMachine.applyCommand( new ReplicatedBarrierTokenRequest( member( 0 ), firstCandidateId + 1, databaseId ), 1, r -> {} ); // not accepted

        // then
        assertEquals( stateMachine.snapshot().candidateId(), BarrierToken.INVALID_BARRIER_TOKEN_ID );

        // when
        stateMachine.applyCommand( new ReplicatedBarrierTokenRequest( member( 0 ), firstCandidateId, databaseId ), 2, r -> {} ); // accepted

        // then
        assertEquals( stateMachine.snapshot().candidateId(), firstCandidateId );

        // when
        stateMachine.applyCommand( new ReplicatedBarrierTokenRequest( member( 0 ), firstCandidateId + 1, databaseId ), 3, r -> {} ); // accepted

        // then
        assertEquals( stateMachine.snapshot().candidateId(), firstCandidateId + 1 );

        // when
        stateMachine.applyCommand( new ReplicatedBarrierTokenRequest( member( 0 ), firstCandidateId, databaseId ), 4, r -> {} ); // not accepted

        // then
        assertEquals( stateMachine.snapshot().candidateId(), firstCandidateId + 1 );

        // when
        stateMachine.applyCommand( new ReplicatedBarrierTokenRequest( member( 0 ), firstCandidateId + 3, databaseId ), 5, r -> {} ); // not accepted

        // then
        assertEquals( stateMachine.snapshot().candidateId(), firstCandidateId + 1 );
    }

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

    @Test
    public void shouldPersistAndRecoverState() throws Exception
    {
        // given
        EphemeralFileSystemAbstraction fsa = fileSystemRule.get();
        fsa.mkdir( testDir.homeDir() );

        SafeStateMarshal<ReplicatedBarrierTokenState> marshal = new ReplicatedBarrierTokenState.Marshal();

        MemberId memberA = member( 0 );
        MemberId memberB = member( 1 );
        int candidateId;

        DurableStateStorage<ReplicatedBarrierTokenState> storage = new DurableStateStorage<>( fsa, testDir.homeDir(),
                CoreStateFiles.DUMMY( marshal ), 100, NullLogProvider.getInstance() );
        try ( Lifespan ignored = new Lifespan( storage ) )
        {
            ReplicatedBarrierTokenStateMachine stateMachine = new ReplicatedBarrierTokenStateMachine( storage );

            // when
            candidateId = 0;
            stateMachine.applyCommand( new ReplicatedBarrierTokenRequest( memberA, candidateId, databaseId ), 0, r -> {} );
            candidateId = 1;
            stateMachine.applyCommand( new ReplicatedBarrierTokenRequest( memberB, candidateId, databaseId ), 1, r -> {} );

            stateMachine.flush();
            fsa.crash();
        }

        // then
        DurableStateStorage<ReplicatedBarrierTokenState> storage2 = new DurableStateStorage<>( fsa, testDir.homeDir(),
                CoreStateFiles.DUMMY( marshal ), 100, NullLogProvider.getInstance() );
        try ( Lifespan ignored = new Lifespan( storage2 ) )
        {
            ReplicatedBarrierTokenState initialState = storage2.getInitialState();
            ReplicatedBarrierTokenRequest request = new ReplicatedBarrierTokenRequest( initialState, databaseId );
            assertEquals( memberB, request.owner() );
            assertEquals( candidateId, request.id() );
        }
    }

    @Test
    public void shouldBeIdempotent()
    {
        // given
        EphemeralFileSystemAbstraction fsa = fileSystemRule.get();
        fsa.mkdir( testDir.homeDir() );

        SafeStateMarshal<ReplicatedBarrierTokenState> marshal = new ReplicatedBarrierTokenState.Marshal();

        DurableStateStorage<ReplicatedBarrierTokenState> storage = new DurableStateStorage<>( fsa, testDir.homeDir(),
                CoreStateFiles.DUMMY( marshal ), 100, NullLogProvider.getInstance() );

        try ( Lifespan ignored = new Lifespan( storage ) )
        {
            ReplicatedBarrierTokenStateMachine stateMachine = new ReplicatedBarrierTokenStateMachine( storage );

            MemberId memberA = member( 0 );
            MemberId memberB = member( 1 );

            stateMachine.applyCommand( new ReplicatedBarrierTokenRequest( memberA, 0, databaseId ), 3, r ->
            {
            } );

            // when
            stateMachine.applyCommand( new ReplicatedBarrierTokenRequest( memberB, 1, databaseId ), 2, r ->
            {
            } );

            // then
            assertEquals( memberA, stateMachine.snapshot().owner() );
        }
    }

    @Test
    public void shouldSetInitialPendingRequestToInitialState()
    {
        // Given
        @SuppressWarnings( "unchecked" )
        StateStorage<ReplicatedBarrierTokenState> storage = mock( StateStorage.class );
        MemberId initialHoldingCoreMember = member( 0 );
        ReplicatedBarrierTokenState initialState = new ReplicatedBarrierTokenState( 123,
                new ReplicatedBarrierTokenRequest( initialHoldingCoreMember, 3, databaseId ) );
        when( storage.getInitialState() ).thenReturn( initialState );
        ReplicatedBarrierTokenRequest initialRequest = new ReplicatedBarrierTokenRequest( initialState, databaseId );
        // When
        ReplicatedBarrierTokenStateMachine stateMachine = new ReplicatedBarrierTokenStateMachine( storage );

        // Then
        ReplicatedBarrierTokenState state = stateMachine.snapshot();
        BarrierToken token = new ReplicatedBarrierTokenRequest( state, databaseId );
        assertEquals( initialRequest.owner(), token.owner() );
        assertEquals( initialRequest.id(), token.id() );
    }
}
