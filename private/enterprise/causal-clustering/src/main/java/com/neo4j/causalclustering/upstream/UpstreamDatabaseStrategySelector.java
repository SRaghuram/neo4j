/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.dbms.identity.ServerId;
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
        log.debug( "Strategy selector has strategies [%s]", strategies );
    }

    public Collection<ServerId> bestUpstreamServersForDatabase( NamedDatabaseId namedDatabaseId ) throws UpstreamDatabaseSelectionException
    {
        for ( var strategy : strategies )
        {
            var upstreamServers = strategy.upstreamServersForDatabase( namedDatabaseId );
            if ( !upstreamServers.isEmpty() )
            {
                var servers = upstreamServers.stream()
                                             .map( ServerId::toString )
                                             .collect( Collectors.joining( ",", "[", "]" ) );
                log.debug( "Selected upstream servers %s for database %s", servers, namedDatabaseId.name() );
                return upstreamServers;
            }
        }
        throw cannotFindUpstreamException( namedDatabaseId );
    }

    public ServerId bestUpstreamServerForDatabase( NamedDatabaseId namedDatabaseId ) throws UpstreamDatabaseSelectionException
    {
        //Do not factor this out in favour of `bestUpstreamMembersForDatabase(...).stream().findFirst()` or similar.
        //  Strategy implementations may implement the method returning `Optional<MemberId>` with specific optimisations.
        for ( var strategy : strategies )
        {
            var upstreamServer = strategy.upstreamServerForDatabase( namedDatabaseId );
            if ( upstreamServer.isPresent() )
            {
                var choice = upstreamServer.get();
                log.debug( "Selected upstream server %s for database %s", upstreamServer, namedDatabaseId.name() );
                return choice;
            }
        }
        throw cannotFindUpstreamException( namedDatabaseId );
    }

    private UpstreamDatabaseSelectionException cannotFindUpstreamException( NamedDatabaseId namedDatabaseId )
    {
        return new UpstreamDatabaseSelectionException( "Could not find an upstream server for database " + namedDatabaseId.name() + " with which to connect" );
    }
}
