/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.monitoring;

public interface ClusterSizeMonitor
{
    void setMembers( int size );

    void setUnreachable( int size );

    void setConverged( boolean converged );
}
