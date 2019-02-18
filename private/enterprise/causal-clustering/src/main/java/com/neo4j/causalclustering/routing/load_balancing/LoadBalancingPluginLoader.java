/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.routing.load_balancing.plugins.ServerShufflingProcessor;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.common.Service;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Loads and initialises any service implementations of <class>LoadBalancingPlugin</class>.
 * Exposes configured instances of that interface via an iterator.
 */
public class LoadBalancingPluginLoader
{
    private LoadBalancingPluginLoader()
    {
    }

    public static void validate( Config config, Log log ) throws InvalidSettingException
    {
        LoadBalancingPlugin plugin = findPlugin( config );
        plugin.validate( config, log );
    }

    public static LoadBalancingProcessor load( TopologyService topologyService, LeaderLocator leaderLocator,
            LogProvider logProvider, Config config ) throws Throwable
    {
        LoadBalancingPlugin plugin = findPlugin( config );
        plugin.init( topologyService, leaderLocator, logProvider, config );

        if ( config.get( CausalClusteringSettings.load_balancing_shuffle ) )
        {
            return new ServerShufflingProcessor( plugin );
        }

        return plugin;
    }

    private static LoadBalancingPlugin findPlugin( Config config ) throws InvalidSettingException
    {
        Set<String> availableOptions = new HashSet<>();
        Iterable<LoadBalancingPlugin> allImplementationsOnClasspath = Service.loadAll( LoadBalancingPlugin.class );

        String configuredName = config.get( CausalClusteringSettings.load_balancing_plugin );

        for ( LoadBalancingPlugin plugin : allImplementationsOnClasspath )
        {
            if ( plugin.pluginName().equals( configuredName ) )
            {
                return plugin;
            }
            availableOptions.add( plugin.pluginName() );
        }

        throw new InvalidSettingException( String.format( "Could not find load balancing plugin with name: '%s'" +
                                                   " among available options: %s", configuredName, availableOptions ) );
    }
}
