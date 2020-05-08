/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.routing.load_balancing.plugins.ServerShufflingProcessor;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.SettingConstraint;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.service.Services;

/**
 * Loads and initialises any service implementations of <class>LoadBalancingPlugin</class>.
 * Exposes configured instances of that interface via an iterator.
 */
public class LoadBalancingPluginLoader
{
    private LoadBalancingPluginLoader()
    {
    }

    public static void validate( Config config, Log log )
    {
        LoadBalancingPlugin plugin = findPlugin( getConfiguredName( config ) );
        plugin.validate( config, log );
    }

    public static LoadBalancingProcessor load( TopologyService topologyService, LeaderService leaderService, LogProvider logProvider, Config config )
            throws Throwable
    {
        LoadBalancingPlugin plugin = findPlugin( getConfiguredName( config ) );
        plugin.init( topologyService, leaderService, logProvider, config );

        if ( config.get( CausalClusteringSettings.load_balancing_shuffle ) && !plugin.isShufflingPlugin() )
        {
            return new ServerShufflingProcessor( plugin );
        }

        return plugin;
    }

    private static String getConfiguredName( Config config )
    {
        return config.get( CausalClusteringSettings.load_balancing_plugin );
    }

    private static LoadBalancingPlugin findPlugin( String configuredName )
    {
        Set<String> availableOptions = new HashSet<>();
        Iterable<LoadBalancingPlugin> allImplementationsOnClasspath = Services.loadAll( LoadBalancingPlugin.class );

        for ( LoadBalancingPlugin plugin : allImplementationsOnClasspath )
        {
            if ( plugin.pluginName().equals( configuredName ) )
            {
                return plugin;
            }
            availableOptions.add( plugin.pluginName() );
        }

        throw new IllegalArgumentException( String.format( "Could not find load balancing plugin with name: '%s'" +
                                                           " among available options: %s", configuredName, availableOptions ) );
    }

    public static SettingConstraint<String> hasPlugin()
    {
        return new SettingConstraint<>()
        {
            @Override
            public void validate( String configuredName, Configuration config )
            {
                findPlugin( configuredName );
            }

            @Override
            public String getDescription()
            {
                return "specified load balancer plugin exist.";
            }
        };
    }
}
