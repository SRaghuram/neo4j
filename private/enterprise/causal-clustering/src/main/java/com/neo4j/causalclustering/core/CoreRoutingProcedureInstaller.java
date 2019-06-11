/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.routing.load_balancing.LeaderService;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingPluginLoader;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingProcessor;
import com.neo4j.causalclustering.routing.load_balancing.procedure.GetRoutingTableProcedureForMultiDC;
import com.neo4j.causalclustering.routing.load_balancing.procedure.GetRoutingTableProcedureForSingleDC;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.logging.LogProvider;
import org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller;

public class CoreRoutingProcedureInstaller extends BaseRoutingProcedureInstaller
{
    private final TopologyService topologyService;
    private final LeaderService leaderService;
    private final DatabaseIdRepository databaseIdRepository;
    private final Config config;
    private final LogProvider logProvider;

    public CoreRoutingProcedureInstaller( TopologyService topologyService, LeaderService leaderService, DatabaseIdRepository databaseIdRepository,
            Config config, LogProvider logProvider )
    {
        this.topologyService = topologyService;
        this.leaderService = leaderService;
        this.databaseIdRepository = databaseIdRepository;
        this.config = config;
        this.logProvider = logProvider;
    }

    @Override
    protected CallableProcedure createProcedure( List<String> namespace )
    {
        if ( config.get( CausalClusteringSettings.multi_dc_license ) )
        {
            LoadBalancingProcessor loadBalancingProcessor = loadLoadBalancingProcessor( databaseIdRepository );
            return new GetRoutingTableProcedureForMultiDC( namespace, loadBalancingProcessor, databaseIdRepository, config );
        }
        else
        {
            return new GetRoutingTableProcedureForSingleDC( namespace, topologyService, leaderService, databaseIdRepository, config, logProvider );
        }
    }

    private LoadBalancingProcessor loadLoadBalancingProcessor( DatabaseIdRepository databaseIdRepository )
    {
        try
        {
            return LoadBalancingPluginLoader.load( topologyService, leaderService, databaseIdRepository, logProvider, config );
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( e );
        }
    }
}
