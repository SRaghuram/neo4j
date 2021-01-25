/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.stream.Collectors.toSet;

class PruningStateSink<T>
{
    private final String stateType;
    private final SourceQueueWithComplete<T> stateSink;
    private final Function<DatabaseId,T> emptyStateFactory;
    private final Function<T,DatabaseId> getDatabaseId;
    private final Duration maxStateLifetime;
    private final Clock clock;
    private final Log log;

    private final Map<DatabaseId,Instant> lastUpdated = new HashMap<>();

    private PruningStateSink( String stateType, SourceQueueWithComplete<T> underlying,
            Function<DatabaseId,T> emptyStateFactory, Function<T,DatabaseId> getDatabaseId,
            Duration maxStateLifetime, Clock clock, LogProvider logProvider )
    {
        this.stateType = stateType;
        this.stateSink = underlying;
        this.emptyStateFactory = emptyStateFactory;
        this.maxStateLifetime = maxStateLifetime;
        this.clock = clock;
        this.log = logProvider.getLog( getClass() );
        this.getDatabaseId = getDatabaseId;
    }

    static PruningStateSink<DatabaseCoreTopology> forCoreTopologies( SourceQueueWithComplete<DatabaseCoreTopology> topologySink,
            Duration maxTopologyLifetime, Clock clock, LogProvider logProvider )
    {
        return new PruningStateSink<>( "Core topology", topologySink, DatabaseCoreTopology::empty,
                DatabaseCoreTopology::databaseId, maxTopologyLifetime, clock, logProvider );
    }

    static PruningStateSink<DatabaseReadReplicaTopology> forReadReplicaTopologies( SourceQueueWithComplete<DatabaseReadReplicaTopology> topologySink,
            Duration maxTopologyLifetime, Clock clock, LogProvider logProvider )
    {
        return new PruningStateSink<>( "Read replica topology", topologySink, DatabaseReadReplicaTopology::empty,
                DatabaseReadReplicaTopology::databaseId, maxTopologyLifetime, clock, logProvider );
    }

    static PruningStateSink<ReplicatedDatabaseState> forCoreDatabaseStates( SourceQueueWithComplete<ReplicatedDatabaseState> databaseStateSink,
            Duration maxDatabaseStateLifetime, Clock clock, LogProvider logProvider )
    {
        return new PruningStateSink<>( "Core member database states", databaseStateSink, id -> ReplicatedDatabaseState.ofCores( id, Map.of() ),
                ReplicatedDatabaseState::databaseId, maxDatabaseStateLifetime, clock, logProvider );
    }

    static PruningStateSink<ReplicatedDatabaseState> forReadReplicaDatabaseStates( SourceQueueWithComplete<ReplicatedDatabaseState> databaseStateSink,
            Duration maxDatabaseStateLifetime, Clock clock, LogProvider logProvider )
    {
        return new PruningStateSink<>( "Read replica member database states", databaseStateSink,
                id -> ReplicatedDatabaseState.ofReadReplicas( id, Map.of() ), ReplicatedDatabaseState::databaseId,
                maxDatabaseStateLifetime, clock, logProvider );
    }

    void offer( T state )
    {
        stateSink.offer( state );
        lastUpdated.put( getDatabaseId.apply( state ), clock.instant() );
    }

    void pruneStaleState()
    {
        var databaseIdsForStaleState = findDatabaseIdsForStaleState();

        for ( var databaseId : databaseIdsForStaleState )
        {
            log.info( "%s for database %s hasn't been updated for more than %s and will be removed", stateType, databaseId, maxStateLifetime );
            var empty = emptyStateFactory.apply( databaseId );
            stateSink.offer( empty );
            lastUpdated.remove( databaseId );
        }
    }

    private Set<DatabaseId> findDatabaseIdsForStaleState()
    {
        var oldestAllowed = clock.instant().minus( maxStateLifetime );

        return lastUpdated.entrySet()
                .stream()
                .filter( entry -> entry.getValue().isBefore( oldestAllowed ) )
                .map( Map.Entry::getKey )
                .collect( toSet() );
    }
}
