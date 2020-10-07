/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import com.neo4j.configuration.CausalClusteringSettings;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.api.procedure.CallableProcedure;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.procedure.builtin.routing.BaseRoutingProcedureInstaller;

public class CoreRoutingProcedureInstaller extends BaseRoutingProcedureInstaller
{
    static final String MULTI_DC_NOT_ENABLED_WARN = "Custom load balancing policy/plugin provided but multi dc not enabled";

    private final TopologyService topologyService;
    private final LeaderService leaderService;
    private final DatabaseManager<?> databaseManager;
    private final Config config;
    private final LogProvider logProvider;
    private final Log log;

    public CoreRoutingProcedureInstaller( TopologyService topologyService, LeaderService leaderService, DatabaseManager<?> databaseManager, Config config,
                                          LogProvider logProvider )
    {
        this.topologyService = topologyService;
        this.leaderService = leaderService;
        this.databaseManager = databaseManager;
        this.config = config;
        this.logProvider = logProvider;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    protected CallableProcedure createProcedure( List<String> namespace )
    {
        if ( config.get( CausalClusteringSettings.multi_dc_license ) )
        {
            var loadBalancingProcessor = loadLoadBalancingProcessor();
            return new GetRoutingTableProcedureForMultiDC( namespace, loadBalancingProcessor, databaseManager, config, logProvider );
        }
        else
        {
            if ( customBalancingConfigured() )
            {
                log.warn( MULTI_DC_NOT_ENABLED_WARN );
            }
            return new GetRoutingTableProcedureForSingleDC( namespace, topologyService, leaderService, databaseManager, config, logProvider );
        }
    }

    private boolean customBalancingConfigured()
    {
        var lbPlugin = config.get( CausalClusteringSettings.load_balancing_plugin );
        var lbDefault = CausalClusteringSettings.load_balancing_plugin.defaultValue();
        var customLBRequested = !Objects.equals( lbDefault, lbPlugin );

        var groups = config.getGroups( LoadBalancingServerPoliciesGroup.class );
        var customServerPolicyConfigured = !groups.isEmpty();

        return customLBRequested || customServerPolicyConfigured;
    }

    private LoadBalancingProcessor loadLoadBalancingProcessor()
    {
        try
        {
            return LoadBalancingPluginLoader.load( topologyService, leaderService, logProvider, config );
        }
        catch ( Throwable e )
        {
            throw new RuntimeException( e );
        }
    }
}
