/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.routing.load_balancing;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.routing.load_balancing.plugins.ServerShufflingProcessor;

/**
 * Marker trait for Load balancing plugins which handle their own shuffling of
 * routing tables. The results from these plugins will be unmodified by {@link ServerShufflingProcessor}
 * regardless of the value of {@link CausalClusteringSettings#load_balancing_shuffle}.
 */
public interface ShufflingPlugin extends LoadBalancingPlugin
{
}
