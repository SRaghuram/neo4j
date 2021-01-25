/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.filters;

import java.util.Set;

/**
 * Performs no filtering.
 */
public class IdentityFilter<T> implements Filter<T>
{
    public static final IdentityFilter INSTANCE = new IdentityFilter();

    private IdentityFilter()
    {
    }

    public static <T> IdentityFilter<T> as()
    {
        //noinspection unchecked
        return INSTANCE;
    }

    @Override
    public Set<T> apply( Set<T> data )
    {
        return data;
    }

    @Override
    public String toString()
    {
        return "IdentityFilter{}";
    }
}
