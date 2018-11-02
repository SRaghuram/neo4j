/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.cluster_load;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.helpers.DataCreator;

public class SmallBurst implements ClusterLoad
{
    @Override
    public void start( Cluster<?> cluster ) throws Exception
    {
        DataCreator.createEmptyNodes( cluster, 10 );
    }

    @Override
    public void stop()
    {
        // do nothing
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }
}
