/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.routing.load_balancing.procedure.ReadReplicaGetRoutingTableProcedure;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.kernel.api.proc.CallableProcedure;
import org.neo4j.kernel.builtinprocs.routing.CommunityRoutingProcedureInstaller;

public class ReadReplicaRoutingProcedureInstaller extends CommunityRoutingProcedureInstaller
{
    public ReadReplicaRoutingProcedureInstaller( ConnectorPortRegister portRegister, Config config )
    {
        super( portRegister, config );
    }

    @Override
    protected CallableProcedure createProcedure( List<String> namespace )
    {
        return new ReadReplicaGetRoutingTableProcedure( namespace, portRegister, config );
    }
}
