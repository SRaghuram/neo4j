/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import java.util.Set;

import org.neo4j.causalclustering.routing.load_balancing.filters.Filter;

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
