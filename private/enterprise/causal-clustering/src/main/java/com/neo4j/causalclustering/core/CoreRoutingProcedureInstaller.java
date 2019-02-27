/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingPluginLoader;
import com.neo4j.causalclustering.routing.load_balancing.LoadBalancingProcessor;
import com.neo4j.causalclustering.routing.load_balancing.procedure.GetServersProcedureForMultiDC;
import com.neo4j.causalclustering.routing.load_balancing.procedure.GetServersProcedureForSingleDC;

import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.logging.LogProvider;
import org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller;

public class CoreRoutingProcedureInstaller extends BaseRoutingProcedureInstaller
{
    private final TopologyService topologyService;
    private final LeaderLocator leaderLocator;
    private final Config config;
    private final LogProvider logProvider;

    public CoreRoutingProcedureInstaller( TopologyService topologyService, LeaderLocator leaderLocator, Config config, LogProvider logProvider )
    {
        this.topologyService = topologyService;
        this.leaderLocator = leaderLocator;
        this.config = config;
        this.logProvider = logProvider;
    }

    @Override
    protected CallableProcedure createProcedure( List<String> namespace )
    {
        if ( config.get( CausalClusteringSettings.multi_dc_license ) )
        {
            LoadBalancingProcessor loadBalancingProcessor = loadLoadBalancingProcessor();
            return new GetServersProcedureForMultiDC( namespace, loadBalancingProcessor );
        }
        else
        {
            return new GetServersProcedureForSingleDC( namespace, topologyService, leaderLocator, config, logProvider );
        }
    }

    private LoadBalancingProcessor loadLoadBalancingProcessor()
    {
        try
        {
            return LoadBalancingPluginLoader.load( topologyService, leaderLocator, logProvider, config );
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( e );
        }
    }
}
