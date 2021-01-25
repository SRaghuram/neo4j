/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.common.DatabasePanicHandlers;
import com.neo4j.causalclustering.error_handling.PanicService;
import com.neo4j.dbms.ClusterInternalDbmsOperator;

import java.util.List;

import org.neo4j.kernel.database.Database;

import static com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler.markUnhealthy;
import static com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler.raiseAvailabilityGuard;
import static com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler.stopDatabase;

public class ReadReplicaPanicHandlers extends DatabasePanicHandlers
{
    public ReadReplicaPanicHandlers( PanicService panicService, Database kernelDatabase, ClusterInternalDbmsOperator clusterInternalOperator )
    {
        super( panicService, kernelDatabase.getNamedDatabaseId(), List.of(
                raiseAvailabilityGuard( kernelDatabase ),
                markUnhealthy( kernelDatabase ),
                stopDatabase( clusterInternalOperator ) ) );
    }
}
