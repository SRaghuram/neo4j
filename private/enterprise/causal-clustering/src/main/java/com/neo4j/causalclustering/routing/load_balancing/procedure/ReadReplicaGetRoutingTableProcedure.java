/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.procedure;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.procedure.builtin.routing.SingleInstanceGetRoutingTableProcedure;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

public class ReadReplicaGetRoutingTableProcedure extends SingleInstanceGetRoutingTableProcedure
{
    public ReadReplicaGetRoutingTableProcedure( List<String> namespace, DatabaseManager<?> databaseManager, ConnectorPortRegister portRegister,
            DatabaseIdRepository databaseIdRepository, Config config )
    {
        super( namespace, databaseManager, portRegister, databaseIdRepository, config );
    }

    @Override
    protected RoutingResult createRoutingResult( AdvertisedSocketAddress address, long routingTableTtl )
    {
        List<AdvertisedSocketAddress> addresses = singletonList( address );
        // read replicas do not expose any writers
        return new RoutingResult( addresses, emptyList(), addresses, routingTableTtl );
    }
}
