/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.common.DatabasePanicHandlers;
import com.neo4j.dbms.DatabaseStartAborter;
import com.neo4j.dbms.error_handling.DatabasePanicEventHandler;
import com.neo4j.dbms.error_handling.MarkUnhealthyHandler;
import com.neo4j.dbms.error_handling.PanicService;
import com.neo4j.dbms.ClusterInternalDbmsOperator;
import com.neo4j.dbms.error_handling.RaiseAvailabilityGuardHandler;
import com.neo4j.dbms.error_handling.StopDatabaseHandler;

import java.util.List;

import org.neo4j.kernel.database.Database;

public class ReadReplicaPanicHandlers extends DatabasePanicHandlers
{
    public ReadReplicaPanicHandlers( PanicService panicService, Database kernelDatabase, ClusterInternalDbmsOperator clusterInternalOperator,
            DatabaseStartAborter databaseStartAborter )
    {
        super( panicService, kernelDatabase.getNamedDatabaseId(), panicEventHandlers( kernelDatabase, clusterInternalOperator, databaseStartAborter ) );
    }

    private static List<DatabasePanicEventHandler> panicEventHandlers( Database kernelDatabase, ClusterInternalDbmsOperator clusterInternalOperator,
            DatabaseStartAborter databaseStartAborter )
    {
        return List.of(
                RaiseAvailabilityGuardHandler.create( kernelDatabase ),
                MarkUnhealthyHandler.create( kernelDatabase ),
                databaseStartAborter,
                StopDatabaseHandler.create( clusterInternalOperator )
        );
    }
}
