/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.multi_cluster;

import com.neo4j.causalclustering.routing.Endpoint;
import com.neo4j.causalclustering.routing.RoutingResult;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Simple struct containing the the result of multi-cluster routing procedure execution.
 */
public class MultiClusterRoutingResult implements RoutingResult
{
    private final Map<String,List<Endpoint>> routers;
    private final long timeToLiveMillis;

    public MultiClusterRoutingResult( Map<String,List<Endpoint>> routers, long timeToLiveMillis )
    {
        this.routers = routers;
        this.timeToLiveMillis = timeToLiveMillis;
    }

    public Map<String,List<Endpoint>> routers()
    {
        return routers;
    }

    @Override
    public long ttlMillis()
    {
        return timeToLiveMillis;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        MultiClusterRoutingResult that = (MultiClusterRoutingResult) o;
        return timeToLiveMillis == that.timeToLiveMillis && Objects.equals( routers, that.routers );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( routers, timeToLiveMillis );
    }
}

