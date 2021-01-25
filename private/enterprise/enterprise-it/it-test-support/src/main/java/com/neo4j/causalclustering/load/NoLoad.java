/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.load;

import com.neo4j.causalclustering.common.Cluster;

public class NoLoad implements ClusterLoad
{
    @Override
    public void start( Cluster cluster )
    {
        // do nothing
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
