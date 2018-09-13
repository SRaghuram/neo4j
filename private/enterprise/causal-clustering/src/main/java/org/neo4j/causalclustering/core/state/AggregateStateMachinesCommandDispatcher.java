/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import java.util.function.Consumer;

import org.neo4j.causalclustering.common.DatabaseService;
import org.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import org.neo4j.causalclustering.core.state.machines.dummy.DummyRequest;
import org.neo4j.causalclustering.core.state.machines.id.ReplicatedIdAllocationRequest;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenRequest;
import org.neo4j.causalclustering.core.state.machines.token.ReplicatedTokenRequest;
import org.neo4j.causalclustering.core.state.machines.tx.CoreReplicatedContent;
import org.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransaction;

class AggregateStateMachinesCommandDispatcher implements CommandDispatcher
{
    private final DatabaseService databaseService;
    private final CoreStateRepository coreStateRepository;

    AggregateStateMachinesCommandDispatcher( DatabaseService databaseService, CoreStateRepository coreStateRepository )
    {
        this.databaseService = databaseService;
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
        databaseService.assertHealthy( IllegalStateException.class );
        PerDatabaseCoreStateComponents dbState = coreStateRepository.getDatabaseState( command.databaseName() )
                .orElseThrow( () -> new IllegalStateException( String.format( "The replicated command %s specifies a database %s " +
                    "which does not exist or has not been initialised.", command, command.databaseName() ) ) );

        return dbState.stateMachines();
    }
}
