/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.procedure;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.procedure.builtin.routing.SingleInstanceGetRoutingTableProcedure;
import org.neo4j.procedure.builtin.routing.SingleInstanceGetRoutingTableProcedureTest;

import static java.util.Collections.emptyList;
import static org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller.DEFAULT_NAMESPACE;

class ReadReplicaGetRoutingTableProcedureTest extends SingleInstanceGetRoutingTableProcedureTest
{
    @Override
    protected SingleInstanceGetRoutingTableProcedure newProcedure( ConnectorPortRegister portRegister, Config config )
    {
        return new ReadReplicaGetRoutingTableProcedure( DEFAULT_NAMESPACE, portRegister, config );
    }

    @Override
    protected List<AdvertisedSocketAddress> expectedWriters( AdvertisedSocketAddress selfAddress )
    {
        return emptyList();
    }
}
