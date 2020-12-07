/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.procedure;

import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.AddressCollector;
import com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies.ClusterServerInfosProvider;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.LogProvider;
import org.neo4j.procedure.builtin.routing.BaseGetRoutingTableProcedure;
import org.neo4j.procedure.builtin.routing.RoutingResult;
import org.neo4j.values.virtual.MapValue;

/**
 * Returns endpoints and their capabilities.
 *
 * V2 extends upon V1 by allowing a client context consisting of
 * key-value pairs to be supplied to and used by the concrete load
 * balancing strategies.
 */
public class GetRoutingTableProcedureForSingleDC extends BaseGetRoutingTableProcedure
{
    private static final String DESCRIPTION = "Returns cluster endpoints and their capabilities for single data center setup.";

    private final AddressCollector addressCollector;

    public GetRoutingTableProcedureForSingleDC( List<String> namespace, TopologyService topologyService, LeaderService leaderService,
            DatabaseManager<?> databaseManager, Config config, LogProvider logProvider )
    {
        super( namespace, databaseManager, config, logProvider );
        this.addressCollector = new AddressCollector( new ClusterServerInfosProvider( topologyService, leaderService ), leaderService, config,
                logProvider.getLog( getClass() ) );
    }

    @Override
    protected String description()
    {
        return DESCRIPTION;
    }

    @Override
    protected RoutingResult invoke( NamedDatabaseId namedDatabaseId, MapValue routingContext )
    {
        return addressCollector.createRoutingResult( namedDatabaseId );
    }
}
