/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream;

import com.neo4j.causalclustering.identity.MemberId;

import java.util.LinkedHashSet;
import java.util.Set;

import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.internal.helpers.collection.Iterables.empty;

public class UpstreamDatabaseStrategySelector
{
    private final Set<UpstreamDatabaseSelectionStrategy> strategies = new LinkedHashSet<>();
    private final Log log;

    public UpstreamDatabaseStrategySelector( UpstreamDatabaseSelectionStrategy defaultStrategy )
    {
        this( defaultStrategy, empty(), NullLogProvider.getInstance() );
    }

    public UpstreamDatabaseStrategySelector( UpstreamDatabaseSelectionStrategy defaultStrategy, Iterable<UpstreamDatabaseSelectionStrategy> otherStrategies,
            LogProvider logProvider )
    {
        log = logProvider.getLog( getClass() );
        Iterables.addAll( strategies, otherStrategies );
        strategies.add( defaultStrategy );
    }

    public MemberId bestUpstreamMemberForDatabase( NamedDatabaseId namedDatabaseId ) throws UpstreamDatabaseSelectionException
    {
        for ( var strategy : strategies )
        {
            log.debug( "Trying selection strategy [%s]", strategy );

            var upstreamMember = strategy.upstreamMemberForDatabase( namedDatabaseId );
            if ( upstreamMember.isPresent() )
            {
                var memberId = upstreamMember.get();
                log.debug( "Selected upstream database [%s]", memberId );
                return memberId;
            }
        }
        throw new UpstreamDatabaseSelectionException( "Could not find an upstream member for database " + namedDatabaseId.name() + " with which to connect" );
    }
}
