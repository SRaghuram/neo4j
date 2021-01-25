/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

/**
 * If this is our first ever start then we can choose to speed up (Akka) cluster formation.
 */
public interface DiscoveryFirstStartupDetector
{
    /**
     * @return true if we think that this Neo4j server has never formed a cluster before.
     */
    Boolean isFirstStartup();
}
