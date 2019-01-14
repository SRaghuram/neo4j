/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.cluster_load;

import org.neo4j.causalclustering.common.Cluster;

public interface ClusterLoad
{
    void start( Cluster<?> cluster ) throws Exception;

    void stop();
}
