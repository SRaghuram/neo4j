/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.stream.javadsl.SourceQueueWithComplete;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.Topology;

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

class TopologiesUpdater<T extends Topology<?>>
{
    private final String topologyType;
    private final SourceQueueWithComplete<T> topologySink;
    private final Function<DatabaseId,T> emptyTopologyFactory;
    private final Duration maxTopologyLifetime;
    private final Clock clock;
    private final Log log;

    private final Map<DatabaseId,Instant> lastUpdated = new HashMap<>();

    private TopologiesUpdater( String topologyType, SourceQueueWithComplete<T> topologySink,
            Function<DatabaseId,T> emptyTopologyFactory, Duration maxTopologyLifetime, Clock clock, LogProvider logProvider )
    {
        this.topologyType = topologyType;
        this.topologySink = topologySink;
        this.emptyTopologyFactory = emptyTopologyFactory;
        this.maxTopologyLifetime = maxTopologyLifetime;
        this.clock = clock;
        this.log = logProvider.getLog( getClass() );
    }

    static TopologiesUpdater<DatabaseCoreTopology> forCoreTopologies( SourceQueueWithComplete<DatabaseCoreTopology> topologySink,
            Duration maxTopologyLifetime, Clock clock, LogProvider logProvider )
    {
        return new TopologiesUpdater<>( "Core", topologySink, DatabaseCoreTopology::empty, maxTopologyLifetime, clock, logProvider );
    }

    static TopologiesUpdater<DatabaseReadReplicaTopology> forReadReplicaTopologies( SourceQueueWithComplete<DatabaseReadReplicaTopology> topologySink,
            Duration maxTopologyLifetime, Clock clock, LogProvider logProvider )
    {
        return new TopologiesUpdater<>( "Read replica", topologySink, DatabaseReadReplicaTopology::empty, maxTopologyLifetime, clock, logProvider );
    }

    void offer( T topology )
    {
        topologySink.offer( topology );
        lastUpdated.put( topology.databaseId(), clock.instant() );
    }

    void pruneStaleTopologies()
    {
        var databaseIdsForStaleTopologies = findDatabaseIdsForStaleTopologies();

        for ( var databaseId : databaseIdsForStaleTopologies )
        {
            log.debug( "%s topology for database %s hasn't been updated for more than %s and will be removed", topologyType, databaseId, maxTopologyLifetime );
            var emptyTopology = emptyTopologyFactory.apply( databaseId );
            topologySink.offer( emptyTopology );
            lastUpdated.remove( databaseId );
        }
    }

    private Set<DatabaseId> findDatabaseIdsForStaleTopologies()
    {
        var oldestAllowed = clock.instant().minus( maxTopologyLifetime );

        return lastUpdated.entrySet()
                .stream()
                .filter( entry -> entry.getValue().isBefore( oldestAllowed ) )
                .map( Map.Entry::getKey )
                .collect( toSet() );
    }
}
