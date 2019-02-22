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
import org.neo4j.kernel.builtinprocs.routing.CommunityGetRoutingTableProcedure;
import org.neo4j.kernel.builtinprocs.routing.RoutingResult;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class ReadReplicaGetRoutingTableProcedure extends CommunityGetRoutingTableProcedure
{
    public ReadReplicaGetRoutingTableProcedure( List<String> namespace, ConnectorPortRegister portRegister, Config config )
    {
        super( namespace, portRegister, config );
    }

    @Override
    protected RoutingResult createRoutingResult( AdvertisedSocketAddress address, long routingTableTtl )
    {
        List<AdvertisedSocketAddress> addresses = singletonList( address );
        // read replicas do not expose any writers
        return new RoutingResult( addresses, emptyList(), addresses, routingTableTtl );
    }
}
