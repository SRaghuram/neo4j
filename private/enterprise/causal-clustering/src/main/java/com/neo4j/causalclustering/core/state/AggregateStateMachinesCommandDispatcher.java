/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.common.ClusteredDatabaseManager;
import com.neo4j.causalclustering.core.CoreDatabaseContext;
import com.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import com.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import com.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import com.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import com.neo4j.causalclustering.core.state.machines.tx.CoreReplicatedContent;
import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;

import java.util.function.Consumer;

class AggregateStateMachinesCommandDispatcher implements CommandDispatcher
{
    private final ClusteredDatabaseManager<CoreDatabaseContext> databaseManager;
    private final CoreStateRepository coreStateRepository;

    AggregateStateMachinesCommandDispatcher( ClusteredDatabaseManager<CoreDatabaseContext> databaseManager,
            CoreStateRepository coreStateRepository )
    {
        this.databaseManager = databaseManager;
        this.coreStateRepository = coreStateRepository;
    }

    @Override
    public void dispatch( ReplicatedTransaction transaction, long commandIndex, Consumer<Result> callback )
    {
        stateMachinesForCommand( transaction ).commandDispatcher().dispatch( transaction, commandIndex, callback );
    }

    @Override
    public void dispatch( ReplicatedIdAllocationRequest idRequest, long commandIndex, Consumer<Result> callback )
    {
        stateMachinesForCommand( idRequest ).commandDispatcher().dispatch( idRequest, commandIndex, callback );
    }

    @Override
    public void dispatch( ReplicatedTokenRequest tokenRequest, long commandIndex, Consumer<Result> callback )
    {
        stateMachinesForCommand( tokenRequest ).commandDispatcher().dispatch( tokenRequest, commandIndex, callback );
    }

    @Override
    public void dispatch( ReplicatedLockTokenRequest lockRequest, long commandIndex, Consumer<Result> callback )
    {
        stateMachinesForCommand( lockRequest ).commandDispatcher().dispatch( lockRequest, commandIndex, callback );
    }

    @Override
    public void dispatch( DummyRequest dummyRequest, long commandIndex, Consumer<Result> callback )
    {
        stateMachinesForCommand( dummyRequest ).commandDispatcher().dispatch( dummyRequest, commandIndex, callback );
    }

    @Override
    public void close()
    {
        coreStateRepository.getAllDatabaseStates().values().forEach( dbState -> dbState.stateMachines().commandDispatcher().close() );
    }

    private CoreStateMachines stateMachinesForCommand( CoreReplicatedContent command )
    {
        var databaseId = command.databaseId();
        //TODO: Move the healthy check to the CoreStateService. If its going to take a databaseManager as a param anyway.
        databaseManager.assertHealthy( databaseId, IllegalStateException.class );
        DatabaseCoreStateComponents dbState = coreStateRepository.getDatabaseState( command.databaseId() )
                .orElseThrow( () -> new IllegalStateException( String.format( "The replicated command %s specifies a database %s " +
                    "which does not exist or has not been initialised.", command, command.databaseId().name() ) ) );
        return dbState.stateMachines();
    }
}
