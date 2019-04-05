/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream;

import com.neo4j.causalclustering.identity.MemberId;

import java.util.LinkedHashSet;
import java.util.Optional;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.neo4j.helpers.collection.Iterables.empty;

public class UpstreamDatabaseStrategySelector
{
    private LinkedHashSet<UpstreamDatabaseSelectionStrategy> strategies = new LinkedHashSet<>();
    private Log log;

    public UpstreamDatabaseStrategySelector( UpstreamDatabaseSelectionStrategy defaultStrategy )
    {
        this( defaultStrategy, empty(), NullLogProvider.getInstance() );
    }

    public UpstreamDatabaseStrategySelector( UpstreamDatabaseSelectionStrategy defaultStrategy, Iterable<UpstreamDatabaseSelectionStrategy> otherStrategies,
            LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );

        if ( otherStrategies != null )
        {
            for ( UpstreamDatabaseSelectionStrategy otherStrategy : otherStrategies )
            {
                strategies.add( otherStrategy );
            }
        }
        strategies.add( defaultStrategy );
    }

    public MemberId bestUpstreamMemberForDatabase( DatabaseId databaseId ) throws UpstreamDatabaseSelectionException
    {
        MemberId result = null;
        for ( UpstreamDatabaseSelectionStrategy strategy : strategies )
        {
            log.debug( "Trying selection strategy [%s]", strategy.toString() );

            Optional<MemberId> upstreamMember = strategy.upstreamMemberForDatabase( databaseId );
            if ( upstreamMember.isPresent() )
            {
                result = upstreamMember.get();
                break;
            }
        }

        if ( result == null )
        {
            throw new UpstreamDatabaseSelectionException( "Could not find an upstream database with which to connect." );
        }

        log.debug( "Selected upstream database [%s]", result );
        return result;
    }
}
