/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing;

import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.discovery.TopologyService;

import org.neo4j.annotations.service.Service;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Defines the interface for an implementation of the GetServersV2
 * cluster discovery and load balancing procedure.
 */
@Service
public interface LoadBalancingPlugin extends LoadBalancingProcessor
{
    void validate( Config config, Log log ) throws InvalidSettingException;

    void init( TopologyService topologyService, LeaderLocator leaderLocator,
            LogProvider logProvider, Config config ) throws Throwable;

    String pluginName();
}
