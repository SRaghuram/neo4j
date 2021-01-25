/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing;

import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.routing.load_balancing.plugins.ServerShufflingProcessor;
import com.neo4j.configuration.CausalClusteringSettings;

import org.neo4j.annotations.service.Service;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Defines the interface for an implementation of the GetServersV2
 * cluster discovery and load balancing procedure.
 */
@Service
public interface LoadBalancingPlugin extends LoadBalancingProcessor
{
    void validate( Configuration configuration, Log log );

    void init( TopologyService topologyService, LeaderService leaderService, LogProvider logProvider, Config config )
            throws Throwable;

    String pluginName();

    /**
     * Check if this plugin handles its own shuffling of addresses in the returned routing tables.
     * The results from shuffling plugins will be unmodified by {@link ServerShufflingProcessor}
     * regardless of the value of {@link CausalClusteringSettings#load_balancing_shuffle}.
     *
     * @return {@code true} when the plugin randomly shuffles the returned addresses, {@code false} otherwise.
     */
    default boolean isShufflingPlugin()
    {
        return false;
    }
}
