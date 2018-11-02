/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.cluster;

import java.net.URI;
import java.util.concurrent.Future;

/**
 * Cluster membership management. This is implemented by {@link ClusterState}
 *
 * @see ClusterState
 * @see ClusterMessage
 */
public interface Cluster
{
    void create( String clusterName );

    Future<ClusterConfiguration> join( String clusterName, URI... otherServerUrls );

    void leave();

    void addClusterListener( ClusterListener listener );

    void removeClusterListener( ClusterListener listener );
}
