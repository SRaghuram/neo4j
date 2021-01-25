/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.filters;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Filters the set through each filter of the chain in order.
 */
public class FilterChain<T> implements Filter<T>
{
    private List<Filter<T>> chain;

    public FilterChain( List<Filter<T>> chain )
    {
        this.chain = chain;
    }

    @Override
    public Set<T> apply( Set<T> data )
    {
        for ( Filter<T> filter : chain )
        {
            data = filter.apply( data );
        }
        return data;
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
        FilterChain<?> that = (FilterChain<?>) o;
        return Objects.equals( chain, that.chain );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( chain );
    }

    @Override
    public String toString()
    {
        return "FilterChain{" +
               "chain=" + chain +
               '}';
    }
}
