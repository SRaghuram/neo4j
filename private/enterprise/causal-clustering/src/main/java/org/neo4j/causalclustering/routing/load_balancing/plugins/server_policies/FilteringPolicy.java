/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import java.util.Objects;
import java.util.Set;

import org.neo4j.causalclustering.routing.load_balancing.filters.Filter;

public class FilteringPolicy implements Policy
{
    private final Filter<ServerInfo> filter;

    FilteringPolicy( Filter<ServerInfo> filter )
    {
        this.filter = filter;
    }

    @Override
    public Set<ServerInfo> apply( Set<ServerInfo> data )
    {
        return filter.apply( data );
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
        FilteringPolicy that = (FilteringPolicy) o;
        return Objects.equals( filter, that.filter );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( filter );
    }

    @Override
    public String toString()
    {
        return "FilteringPolicy{" +
               "filter=" + filter +
               '}';
    }
}
