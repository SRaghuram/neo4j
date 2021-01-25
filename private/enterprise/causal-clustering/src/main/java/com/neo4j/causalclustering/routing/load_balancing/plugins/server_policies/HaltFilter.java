/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.routing.load_balancing.filters.Filter;

import java.util.Set;

/**
 * This filter is not meant for actual use. No more filters nor
 * rules shall follow a halt.
 *
 * In actuality it is implemented by not putting the otherwise
 * implicit all() rule at the end of a policy.
 */
class HaltFilter implements Filter<ServerInfo>
{
    public static final HaltFilter INSTANCE = new HaltFilter();

    private HaltFilter()
    {
    }

    @Override
    public Set<ServerInfo> apply( Set<ServerInfo> data )
    {
        throw new UnsupportedOperationException();
    }
}
