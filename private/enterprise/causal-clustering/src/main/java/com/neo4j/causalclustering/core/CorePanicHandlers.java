/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.DatabasePanicHandlers;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.state.CommandApplicationProcess;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.dbms.ClusterInternalDbmsOperator;

import java.util.List;

import org.neo4j.kernel.database.Database;

import static com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler.markUnhealthy;
import static com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler.panicDbmsIfSystemDatabasePanics;
import static com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler.raiseAvailabilityGuard;
import static com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler.stopDatabase;

class CorePanicHandlers extends DatabasePanicHandlers
{
    CorePanicHandlers( RaftMachine raftMachine, Database kernelDatabase, CommandApplicationProcess applicationProcess,
                       ClusterInternalDbmsOperator clusterInternalOperator, PanicService panicService )
    {
        super( panicService, kernelDatabase.getNamedDatabaseId(), List.of(
                raiseAvailabilityGuard( kernelDatabase ),
                markUnhealthy( kernelDatabase ),
                applicationProcess,
                raftMachine,
                stopDatabase( clusterInternalOperator ),
                // panic Dbms should always be the last handler
                panicDbmsIfSystemDatabasePanics( panicService.panicker() )
        ) );
    }
}
