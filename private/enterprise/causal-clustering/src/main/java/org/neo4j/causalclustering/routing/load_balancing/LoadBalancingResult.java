/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.routing.load_balancing;

import java.util.List;
import java.util.Objects;

import org.neo4j.causalclustering.routing.Endpoint;

/**
 * The outcome of applying a load balancing plugin, which will be used by client
 * software for scheduling work at the endpoints.
 */
public class LoadBalancingResult implements LoadBalancingProcessor.Result
{
    private final List<Endpoint> routeEndpoints;
    private final List<Endpoint> writeEndpoints;
    private final List<Endpoint> readEndpoints;
    private final long timeToLiveMillis;

    public LoadBalancingResult( List<Endpoint> routeEndpoints, List<Endpoint> writeEndpoints,
            List<Endpoint> readEndpoints, long timeToLiveMillis )
    {
        this.routeEndpoints = routeEndpoints;
        this.writeEndpoints = writeEndpoints;
        this.readEndpoints = readEndpoints;
        this.timeToLiveMillis = timeToLiveMillis;
    }

    @Override
    public long ttlMillis()
    {
        return timeToLiveMillis;
    }

    @Override
    public List<Endpoint> routeEndpoints()
    {
        return routeEndpoints;
    }

    @Override
    public List<Endpoint> writeEndpoints()
    {
        return writeEndpoints;
    }

    @Override
    public List<Endpoint> readEndpoints()
    {
        return readEndpoints;
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
        LoadBalancingResult that = (LoadBalancingResult) o;
        return timeToLiveMillis == that.timeToLiveMillis &&
               Objects.equals( routeEndpoints, that.routeEndpoints ) &&
               Objects.equals( writeEndpoints, that.writeEndpoints ) &&
               Objects.equals( readEndpoints, that.readEndpoints );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( routeEndpoints, writeEndpoints, readEndpoints, timeToLiveMillis );
    }

    @Override
    public String toString()
    {
        return "LoadBalancingResult{" +
               "routeEndpoints=" + routeEndpoints +
               ", writeEndpoints=" + writeEndpoints +
               ", readEndpoints=" + readEndpoints +
               ", timeToLiveMillis=" + timeToLiveMillis +
               '}';
    }
}
