/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.load_balancing.plugins.server_policies;

import com.neo4j.causalclustering.core.ServerGroupName;
import com.neo4j.causalclustering.routing.load_balancing.filters.Filter;
import com.neo4j.causalclustering.routing.load_balancing.filters.FilterChain;
import com.neo4j.causalclustering.routing.load_balancing.filters.FirstValidRule;
import com.neo4j.causalclustering.routing.load_balancing.filters.IdentityFilter;
import com.neo4j.causalclustering.routing.load_balancing.filters.MinimumCountFilter;

import java.util.ArrayList;
import java.util.List;

class FilterBuilder
{
    private List<Filter<ServerInfo>> current = new ArrayList<>();
    private List<FilterChain<ServerInfo>> rules = new ArrayList<>();

    static FilterBuilder filter()
    {
        return new FilterBuilder();
    }

    FilterBuilder min( int minCount )
    {
        current.add( new MinimumCountFilter<>( minCount ) );
        return this;
    }

    FilterBuilder groups( String... groups )
    {
        current.add( new AnyGroupFilter( ServerGroupName.setOf( groups ) ) );
        return this;
    }

    FilterBuilder all()
    {
        current.add( IdentityFilter.as() );
        return this;
    }

    FilterBuilder newRule()
    {
        if ( !current.isEmpty() )
        {
            rules.add( new FilterChain<>( current ) );
            current = new ArrayList<>();
        }
        return this;
    }

    Filter<ServerInfo> build()
    {
        newRule();
        return new FirstValidRule<>( rules );
    }
}
