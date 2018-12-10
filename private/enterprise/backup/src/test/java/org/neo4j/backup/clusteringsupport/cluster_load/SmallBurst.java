/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.clusteringsupport.cluster_load;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.helpers.DataCreator;

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
