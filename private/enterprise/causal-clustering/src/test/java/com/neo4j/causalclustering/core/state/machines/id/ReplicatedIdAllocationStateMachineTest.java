/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.id;

import com.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.Test;

import java.util.UUID;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.store.id.IdType;

import static org.junit.Assert.assertEquals;

public class ReplicatedIdAllocationStateMachineTest
{
    private MemberId me = new MemberId( UUID.randomUUID() );

    private IdType someType = IdType.NODE;
    private IdType someOtherType = IdType.RELATIONSHIP;
    private String databaseName = GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

    @Test
    public void shouldNotHaveAnyIdsInitially()
    {
        // given
        ReplicatedIdAllocationStateMachine stateMachine = new ReplicatedIdAllocationStateMachine( new InMemoryStateStorage<>( new IdAllocationState() ) );

        // then
        assertEquals( 0, stateMachine.firstUnallocated( someType ) );
    }

    @Test
    public void shouldUpdateStateOnlyForTypeRequested()
    {
        // given
        ReplicatedIdAllocationStateMachine stateMachine = new ReplicatedIdAllocationStateMachine( new InMemoryStateStorage<>( new IdAllocationState() ) );
        ReplicatedIdAllocationRequest idAllocationRequest = new ReplicatedIdAllocationRequest( me, someType, 0, 1024, databaseName );

        // when
        stateMachine.applyCommand( idAllocationRequest, 0, r -> {} );

        // then
        assertEquals( 1024, stateMachine.firstUnallocated( someType ) );
        assertEquals( 0, stateMachine.firstUnallocated( someOtherType ) );
    }

    @Test
    public void severalDistinctRequestsShouldIncrementallyUpdate()
    {
        // given
        ReplicatedIdAllocationStateMachine stateMachine = new ReplicatedIdAllocationStateMachine( new InMemoryStateStorage<>( new IdAllocationState() ) );
        long index = 0;

        // when
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0, 1024, databaseName ), index++, r -> {} );
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 1024, 1024, databaseName ), index++, r -> {} );
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 2048, 1024, databaseName ), index, r -> {} );

        // then
        assertEquals( 3072, stateMachine.firstUnallocated( someType ) );
    }

    @Test
    public void severalEqualRequestsShouldOnlyUpdateOnce()
    {
        // given
        ReplicatedIdAllocationStateMachine stateMachine = new ReplicatedIdAllocationStateMachine( new InMemoryStateStorage<>( new IdAllocationState() ) );

        // when
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0, 1024, databaseName ), 0, r -> {} );
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0, 1024, databaseName ), 0, r -> {} );
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0, 1024, databaseName ), 0, r -> {} );

        // then
        assertEquals( 1024, stateMachine.firstUnallocated( someType ) );
    }

    @Test
    public void outOfOrderRequestShouldBeIgnored()
    {
        // given
        ReplicatedIdAllocationStateMachine stateMachine = new ReplicatedIdAllocationStateMachine( new InMemoryStateStorage<>( new IdAllocationState() ) );

        // when
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0, 1024, databaseName ), 0, r -> {} );
        // apply command that doesn't consume ids because the requested range is non-contiguous
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 2048, 1024, databaseName ), 0, r -> {} );

        // then
        assertEquals( 1024, stateMachine.firstUnallocated( someType ) );
    }

    @Test
    public void shouldIgnoreNotContiguousRequestAndAlreadySeenIndex()
    {
        ReplicatedIdAllocationStateMachine stateMachine = new ReplicatedIdAllocationStateMachine( new InMemoryStateStorage<>( new IdAllocationState() ) );

        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0L, 10, databaseName ), 0L, r -> {} );
        assertEquals( 10L, stateMachine.firstUnallocated( someType ) );

        // apply command that doesn't consume ids because the requested range is non-contiguous
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 20L, 10, databaseName ), 1L, r -> {} );
        assertEquals( 10L, stateMachine.firstUnallocated( someType ) );

        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 10L, 10, databaseName ), 2L, r -> {} );
        assertEquals( 20L, stateMachine.firstUnallocated( someType ) );

        // try applying the same command again. The requested range is now contiguous, but the log index
        // has already been exceeded
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 20L, 10, databaseName ), 1L, r -> {} );
        assertEquals( 20L, stateMachine.firstUnallocated( someType ) );
    }
}
