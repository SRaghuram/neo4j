/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.DatabasePanicHandlers;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.state.CommandApplicationProcess;
import com.neo4j.dbms.DatabaseStartAborter;
import com.neo4j.dbms.error_handling.DatabasePanicEventHandler;
import com.neo4j.dbms.error_handling.MarkUnhealthyHandler;
import com.neo4j.dbms.error_handling.PanicDbmsIfSystemDatabaseHandler;
import com.neo4j.dbms.error_handling.PanicService;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.error_handling.RaiseAvailabilityGuardHandler;
import com.neo4j.dbms.error_handling.StopDatabaseHandler;

import java.util.List;

import org.neo4j.kernel.database.Database;

class CorePanicHandlers extends DatabasePanicHandlers
{
    CorePanicHandlers( RaftMachine raftMachine, Database kernelDatabase, CommandApplicationProcess applicationProcess,
            ClusterInternalDbmsOperator clusterInternalOperator, PanicService panicService,
            DatabaseStartAborter databaseStartAborter )
    {
        super( panicService, kernelDatabase.getNamedDatabaseId(),
               panicEventHandlers( applicationProcess, raftMachine,
                                   kernelDatabase, clusterInternalOperator,
                                   panicService, databaseStartAborter ) );
    }

    private static List<DatabasePanicEventHandler> panicEventHandlers( CommandApplicationProcess applicationProcess,
            RaftMachine raftMachine, Database kernelDatabase, ClusterInternalDbmsOperator clusterInternalOperator,
            PanicService panicService, DatabaseStartAborter databaseStartAborter )
    {
        return List.of(
                RaiseAvailabilityGuardHandler.factory( kernelDatabase ),
                MarkUnhealthyHandler.factory( kernelDatabase ),
                applicationProcess,
                raftMachine,
                databaseStartAborter,
                StopDatabaseHandler.factory( clusterInternalOperator ),
                // panic Dbms should always be the last handler
                PanicDbmsIfSystemDatabaseHandler.factory( panicService.panicker() )
        );
    }
}
