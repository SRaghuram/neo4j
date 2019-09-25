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
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import org.neo4j.time.FakeClock;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static com.neo4j.causalclustering.discovery.TestTopology.addressesForReadReplica;
import static com.neo4j.causalclustering.discovery.akka.readreplicatopology.TopologiesUpdater.forCoreTopologies;
import static com.neo4j.causalclustering.discovery.akka.readreplicatopology.TopologiesUpdater.forReadReplicaTopologies;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

class TopologiesUpdaterTest
{
    private final SourceQueueWithComplete<DatabaseCoreTopology> coreTopologySink = newTopologySinkMock();
    private final SourceQueueWithComplete<DatabaseReadReplicaTopology> readReplicaTopologySink = newTopologySinkMock();
    private final Duration maxTopologyLifetime = Duration.ofSeconds( 5 );
    private final FakeClock clock = new FakeClock();

    private final TopologiesUpdater<DatabaseCoreTopology> coreTopologiesUpdater =
            forCoreTopologies( coreTopologySink, maxTopologyLifetime, clock, nullLogProvider() );

    private final TopologiesUpdater<DatabaseReadReplicaTopology> readReplicaTopologiesUpdater =
            forReadReplicaTopologies( readReplicaTopologySink, maxTopologyLifetime, clock, nullLogProvider() );

    @Test
    void shouldOfferCoreTopologyToSink()
    {
        var coreTopology = randomCoreTopology();

        coreTopologiesUpdater.offer( coreTopology );

        verify( coreTopologySink ).offer( coreTopology );
    }

    @Test
    void shouldOfferReadReplicaTopologyToSink()
    {
        var readReplicaTopology = randomReadReplicaTopology();

        readReplicaTopologiesUpdater.offer( readReplicaTopology );

        verify( readReplicaTopologySink ).offer( readReplicaTopology );
    }

    @Test
    void shouldSendEmptyCoreTopologiesToSinkWhenPruning()
    {
        var coreTopology1 = randomCoreTopology();
        var coreTopology2 = randomCoreTopology();
        var coreTopology3 = randomCoreTopology();

        coreTopologiesUpdater.offer( coreTopology1 );
        clock.forward( 5, SECONDS );
        coreTopologiesUpdater.offer( coreTopology2 );
        clock.forward( 10, SECONDS );
        coreTopologiesUpdater.offer( coreTopology3 );
        clock.forward( 3, SECONDS );

        coreTopologiesUpdater.pruneStaleTopologies();

        verify( coreTopologySink ).offer( DatabaseCoreTopology.empty( coreTopology1.databaseId() ) );
        verify( coreTopologySink ).offer( DatabaseCoreTopology.empty( coreTopology2.databaseId() ) );
        verify( coreTopologySink, never() ).offer( DatabaseCoreTopology.empty( coreTopology3.databaseId() ) );
    }

    @Test
    void shouldSendEmptyReadReplicaTopologiesToSinkWhenPruning()
    {
        var readReplicaTopology1 = randomReadReplicaTopology();
        var readReplicaTopology2 = randomReadReplicaTopology();
        var readReplicaTopology3 = randomReadReplicaTopology();
        var readReplicaTopology4 = randomReadReplicaTopology();

        readReplicaTopologiesUpdater.offer( readReplicaTopology1 );
        clock.forward( 2, SECONDS );
        readReplicaTopologiesUpdater.offer( readReplicaTopology2 );
        clock.forward( 15, SECONDS );
        readReplicaTopologiesUpdater.offer( readReplicaTopology3 );
        clock.forward( 1, SECONDS );
        readReplicaTopologiesUpdater.offer( readReplicaTopology4 );
        clock.forward( 3, SECONDS );

        readReplicaTopologiesUpdater.pruneStaleTopologies();

        verify( readReplicaTopologySink ).offer( DatabaseReadReplicaTopology.empty( readReplicaTopology1.databaseId() ) );
        verify( readReplicaTopologySink ).offer( DatabaseReadReplicaTopology.empty( readReplicaTopology2.databaseId() ) );
        verify( readReplicaTopologySink, never() ).offer( DatabaseReadReplicaTopology.empty( readReplicaTopology3.databaseId() ) );
        verify( readReplicaTopologySink, never() ).offer( DatabaseReadReplicaTopology.empty( readReplicaTopology4.databaseId() ) );
    }

    private static DatabaseCoreTopology randomCoreTopology()
    {
        var databaseId = randomDatabaseId();
        var raftId = RaftId.from( databaseId );
        var coreMembers = Map.of(
                new MemberId( randomUUID() ), addressesForCore( 0, false ),
                new MemberId( randomUUID() ), addressesForCore( 1, false ),
                new MemberId( randomUUID() ), addressesForCore( 2, false ) );

        return new DatabaseCoreTopology( databaseId, raftId, coreMembers );
    }

    private static DatabaseReadReplicaTopology randomReadReplicaTopology()
    {
        var databaseId = randomDatabaseId();
        var readReplicas = Map.of(
                new MemberId( randomUUID() ), addressesForReadReplica( 0 ),
                new MemberId( randomUUID() ), addressesForReadReplica( 1 ) );

        return new DatabaseReadReplicaTopology( databaseId, readReplicas );
    }

    @SuppressWarnings( "unchecked" )
    private static <T extends Topology<?>> SourceQueueWithComplete<T> newTopologySinkMock()
    {
        return mock( SourceQueueWithComplete.class );
    }
}
