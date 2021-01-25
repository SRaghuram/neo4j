/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.monitoring;

public interface ReplicatedDataMonitor
{
    /**
     * Number of entries of maps / sets as seen by discovery
     */
    void setVisibleDataSize( ReplicatedDataIdentifier key, int size );

    /**
     * Number of entries of internal data structures, such as version vectors or tombstones
     */
    void setInvisibleDataSize( ReplicatedDataIdentifier key, int size );
}
