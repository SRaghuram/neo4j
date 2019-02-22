/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.multi_cluster;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.neo4j.helpers.AdvertisedSocketAddress;

/**
 * Simple struct containing the the result of multi-cluster routing procedure execution.
 */
public class MultiClusterRoutingResult
{
    private final Map<String,List<AdvertisedSocketAddress>> routers;
    private final long timeToLiveMillis;

    public MultiClusterRoutingResult( Map<String,List<AdvertisedSocketAddress>> routers, long timeToLiveMillis )
    {
        this.routers = routers;
        this.timeToLiveMillis = timeToLiveMillis;
    }

    public Map<String,List<AdvertisedSocketAddress>> routers()
    {
        return routers;
    }

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

