/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.locks;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.causalclustering.core.state.CoreStateFiles;
import org.neo4j.causalclustering.core.state.storage.DurableStateStorage;
import org.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import org.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import org.neo4j.causalclustering.core.state.storage.StateStorage;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.identity.RaftTestMember.member;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class ReplicatedLockTokenStateMachineTest
{
    private final String databaseName = DEFAULT_DATABASE_NAME;

    @Rule
    public final EphemeralFileSystemRule fileSystemRule = new EphemeralFileSystemRule();

    @Test
    public void shouldStartWithInvalidTokenId()
    {
        // given
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine(
                new InMemoryStateStorage<>( ReplicatedLockTokenState.INITIAL_LOCK_TOKEN ) );

        // when
        int initialTokenId = stateMachine.snapshot().candidateId();

        // then
        assertEquals( initialTokenId, LockToken.INVALID_LOCK_TOKEN_ID );
    }

    @Test
    public void shouldIssueNextLockTokenCandidateId()
    {
        // given
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine(
                new InMemoryStateStorage<>( ReplicatedLockTokenState.INITIAL_LOCK_TOKEN ) );
        int firstCandidateId = LockToken.nextCandidateId( stateMachine.snapshot().candidateId() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId, databaseName ), 0, r -> {} );

        // then
        assertEquals( firstCandidateId + 1, LockToken.nextCandidateId( stateMachine.snapshot().candidateId() ) );
    }

    @Test
    public void shouldKeepTrackOfCurrentLockTokenId()
    {
        // given
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine(
                new InMemoryStateStorage<>( ReplicatedLockTokenState.INITIAL_LOCK_TOKEN ) );
        int firstCandidateId = LockToken.nextCandidateId( stateMachine.snapshot().candidateId() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId, databaseName ), 1, r -> {} );

        // then
        assertEquals( firstCandidateId, stateMachine.snapshot().candidateId() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId + 1, databaseName ), 2, r -> {} );

        // then
        assertEquals( firstCandidateId + 1, stateMachine.snapshot().candidateId() );
    }

    @Test
    public void shouldKeepTrackOfLockTokenOwner()
    {
        // given
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine(
                new InMemoryStateStorage<>( ReplicatedLockTokenState.INITIAL_LOCK_TOKEN ) );
        int firstCandidateId = LockToken.nextCandidateId( stateMachine.snapshot().candidateId() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId, databaseName ), 1, r -> {} );

        // then
        assertEquals( member( 0 ), stateMachine.snapshot().owner() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 1 ), firstCandidateId + 1, databaseName ), 2, r -> {} );

        // then
        assertEquals( member( 1 ), stateMachine.snapshot().owner() );
    }

    @Test
    public void shouldAcceptOnlyFirstRequestWithSameId()
    {
        // given
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine(
                new InMemoryStateStorage<>( ReplicatedLockTokenState.INITIAL_LOCK_TOKEN ) );
        int firstCandidateId = LockToken.nextCandidateId( stateMachine.snapshot().candidateId() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId, databaseName ), 1, r -> {} );
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 1 ), firstCandidateId, databaseName ), 2, r -> {} );

        // then
        assertEquals( 0, stateMachine.snapshot().candidateId() );
        assertEquals( member( 0 ), stateMachine.snapshot().owner() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 1 ), firstCandidateId + 1, databaseName ), 3, r -> {} );
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId + 1, databaseName ), 4, r -> {} );

        // then
        assertEquals( 1, stateMachine.snapshot().candidateId() );
        assertEquals( member( 1 ), stateMachine.snapshot().owner() );
    }

    @Test
    public void shouldOnlyAcceptNextImmediateId()
    {
        // given
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine(
                new InMemoryStateStorage<>( ReplicatedLockTokenState.INITIAL_LOCK_TOKEN ) );
        int firstCandidateId = LockToken.nextCandidateId( stateMachine.snapshot().candidateId() );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId + 1, databaseName ), 1, r -> {} ); // not accepted

        // then
        assertEquals( stateMachine.snapshot().candidateId(), LockToken.INVALID_LOCK_TOKEN_ID );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId, databaseName ), 2, r -> {} ); // accepted

        // then
        assertEquals( stateMachine.snapshot().candidateId(), firstCandidateId );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId + 1, databaseName ), 3, r -> {} ); // accepted

        // then
        assertEquals( stateMachine.snapshot().candidateId(), firstCandidateId + 1 );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId, databaseName ), 4, r -> {} ); // not accepted

        // then
        assertEquals( stateMachine.snapshot().candidateId(), firstCandidateId + 1 );

        // when
        stateMachine.applyCommand( new ReplicatedLockTokenRequest( member( 0 ), firstCandidateId + 3, databaseName ), 5, r -> {} ); // not accepted

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
        fsa.mkdir( testDir.directory() );

        SafeStateMarshal<ReplicatedLockTokenState> marshal = new ReplicatedLockTokenState.Marshal();

        MemberId memberA = member( 0 );
        MemberId memberB = member( 1 );
        int candidateId;

        DurableStateStorage<ReplicatedLockTokenState> storage = new DurableStateStorage<>( fsa, testDir.directory(),
                CoreStateFiles.DUMMY( marshal ), 100, NullLogProvider.getInstance() );
        try ( Lifespan ignored = new Lifespan( storage ) )
        {
            ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine( storage );

            // when
            candidateId = 0;
            stateMachine.applyCommand( new ReplicatedLockTokenRequest( memberA, candidateId, databaseName ), 0, r -> {} );
            candidateId = 1;
            stateMachine.applyCommand( new ReplicatedLockTokenRequest( memberB, candidateId, databaseName ), 1, r -> {} );

            stateMachine.flush();
            fsa.crash();
        }

        // then
        DurableStateStorage<ReplicatedLockTokenState> storage2 = new DurableStateStorage<>( fsa, testDir.directory(),
                CoreStateFiles.DUMMY( marshal ), 100, NullLogProvider.getInstance() );
        try ( Lifespan ignored = new Lifespan( storage2 ) )
        {
            ReplicatedLockTokenState initialState = storage2.getInitialState();
            ReplicatedLockTokenRequest request = new ReplicatedLockTokenRequest( initialState, databaseName );
            assertEquals( memberB, request.owner() );
            assertEquals( candidateId, request.id() );
        }
    }

    @Test
    public void shouldBeIdempotent()
    {
        // given
        EphemeralFileSystemAbstraction fsa = fileSystemRule.get();
        fsa.mkdir( testDir.directory() );

        SafeStateMarshal<ReplicatedLockTokenState> marshal = new ReplicatedLockTokenState.Marshal();

        DurableStateStorage<ReplicatedLockTokenState> storage = new DurableStateStorage<>( fsa, testDir.directory(),
                CoreStateFiles.DUMMY( marshal ), 100, NullLogProvider.getInstance() );

        try ( Lifespan ignored = new Lifespan( storage ) )
        {
            ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine( storage );

            MemberId memberA = member( 0 );
            MemberId memberB = member( 1 );

            stateMachine.applyCommand( new ReplicatedLockTokenRequest( memberA, 0, databaseName ), 3, r ->
            {
            } );

            // when
            stateMachine.applyCommand( new ReplicatedLockTokenRequest( memberB, 1, databaseName ), 2, r ->
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
        StateStorage<ReplicatedLockTokenState> storage = mock( StateStorage.class );
        MemberId initialHoldingCoreMember = member( 0 );
        ReplicatedLockTokenState initialState = new ReplicatedLockTokenState( 123,
                new ReplicatedLockTokenRequest( initialHoldingCoreMember, 3, databaseName ) );
        when( storage.getInitialState() ).thenReturn( initialState );
        ReplicatedLockTokenRequest initialRequest = new ReplicatedLockTokenRequest( initialState, databaseName );
        // When
        ReplicatedLockTokenStateMachine stateMachine = new ReplicatedLockTokenStateMachine( storage );

        // Then
        ReplicatedLockTokenState state = stateMachine.snapshot();
        LockToken token = new ReplicatedLockTokenRequest( state, databaseName );
        assertEquals( initialRequest.owner(), token.owner() );
        assertEquals( initialRequest.id(), token.id() );
    }
}
