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
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.RaftGroupId;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import org.neo4j.time.FakeClock;

import static com.neo4j.causalclustering.discovery.TestTopology.addressesForCore;
import static com.neo4j.causalclustering.discovery.TestTopology.addressesForReadReplica;
import static com.neo4j.causalclustering.discovery.akka.readreplicatopology.PruningStateSink.forCoreDatabaseStates;
import static com.neo4j.causalclustering.discovery.akka.readreplicatopology.PruningStateSink.forCoreTopologies;
import static com.neo4j.causalclustering.discovery.akka.readreplicatopology.PruningStateSink.forReadReplicaTopologies;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

class PruningStateSinkTest
{
    private final SourceQueueWithComplete<DatabaseCoreTopology> coreTopologySink = newSinkMock();
    private final SourceQueueWithComplete<DatabaseReadReplicaTopology> readReplicaTopologySink = newSinkMock();
    private final SourceQueueWithComplete<ReplicatedDatabaseState> databaseStateSink = newSinkMock();
    private final Duration maxStateLifetime = Duration.ofSeconds( 5 );
    private final FakeClock clock = new FakeClock();

    private final PruningStateSink<DatabaseCoreTopology> pruningCoreTopologySink =
            forCoreTopologies( coreTopologySink, maxStateLifetime, clock, nullLogProvider() );

    private final PruningStateSink<DatabaseReadReplicaTopology> pruningReadReplicaTopologySink =
            forReadReplicaTopologies( readReplicaTopologySink, maxStateLifetime, clock, nullLogProvider() );

    private final PruningStateSink<ReplicatedDatabaseState> pruningStateSink =
            forCoreDatabaseStates( databaseStateSink, maxStateLifetime, clock, nullLogProvider() );

    @Test
    void shouldOfferCoreTopologyToSink()
    {
        var coreTopology = randomCoreTopology();

        pruningCoreTopologySink.offer( coreTopology );

        verify( coreTopologySink ).offer( coreTopology );
    }

    @Test
    void shouldOfferReadReplicaTopologyToSink()
    {
        var readReplicaTopology = randomReadReplicaTopology();

        pruningReadReplicaTopologySink.offer( readReplicaTopology );

        verify( readReplicaTopologySink ).offer( readReplicaTopology );
    }

    @Test
    void shouldOfferDatabaseStateToSink()
    {
        var replicatedDatabaseState = randomDatabaseState();

        pruningStateSink.offer( replicatedDatabaseState );

        verify( databaseStateSink ).offer( replicatedDatabaseState );
    }

    @Test
    void shouldSendEmptyCoreTopologiesToSinkWhenPruning()
    {
        var coreTopology1 = randomCoreTopology();
        var coreTopology2 = randomCoreTopology();
        var coreTopology3 = randomCoreTopology();

        pruningCoreTopologySink.offer( coreTopology1 );
        clock.forward( 5, SECONDS );
        pruningCoreTopologySink.offer( coreTopology2 );
        clock.forward( 10, SECONDS );
        pruningCoreTopologySink.offer( coreTopology3 );
        clock.forward( 3, SECONDS );

        pruningCoreTopologySink.pruneStaleState();

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

        pruningReadReplicaTopologySink.offer( readReplicaTopology1 );
        clock.forward( 2, SECONDS );
        pruningReadReplicaTopologySink.offer( readReplicaTopology2 );
        clock.forward( 15, SECONDS );
        pruningReadReplicaTopologySink.offer( readReplicaTopology3 );
        clock.forward( 1, SECONDS );
        pruningReadReplicaTopologySink.offer( readReplicaTopology4 );
        clock.forward( 3, SECONDS );

        pruningReadReplicaTopologySink.pruneStaleState();

        verify( readReplicaTopologySink ).offer( DatabaseReadReplicaTopology.empty( readReplicaTopology1.databaseId() ) );
        verify( readReplicaTopologySink ).offer( DatabaseReadReplicaTopology.empty( readReplicaTopology2.databaseId() ) );
        verify( readReplicaTopologySink, never() ).offer( DatabaseReadReplicaTopology.empty( readReplicaTopology3.databaseId() ) );
        verify( readReplicaTopologySink, never() ).offer( DatabaseReadReplicaTopology.empty( readReplicaTopology4.databaseId() ) );
    }

    @Test
    void shouldSendEmptyStatesToSinkWhenPruning()
    {
        var replicatedDatabaseState1 = randomDatabaseState();
        var replicatedDatabaseState2 = randomDatabaseState();

        pruningStateSink.offer( replicatedDatabaseState1 );
        clock.forward( 6, SECONDS );
        pruningStateSink.offer( replicatedDatabaseState2 );
        clock.forward( 3, SECONDS );

        pruningStateSink.pruneStaleState();

        verify( databaseStateSink ).offer( ReplicatedDatabaseState.ofCores( replicatedDatabaseState1.databaseId(), Map.of() ) );
        verify( databaseStateSink, never() ).offer( ReplicatedDatabaseState.ofCores( replicatedDatabaseState2.databaseId(), Map.of() ) );
    }

    private static DatabaseCoreTopology randomCoreTopology()
    {
        var databaseId = randomNamedDatabaseId().databaseId();
        var raftGroupId = RaftGroupId.from( databaseId );
        var coreMembers = Map.of(
                IdFactory.randomServerId(), addressesForCore( 0 ),
                IdFactory.randomServerId(), addressesForCore( 1 ),
                IdFactory.randomServerId(), addressesForCore( 2 ) );

        return new DatabaseCoreTopology( databaseId, raftGroupId, coreMembers );
    }

    private static DatabaseReadReplicaTopology randomReadReplicaTopology()
    {
        var databaseId = randomNamedDatabaseId().databaseId();
        var readReplicas = Map.of(
                IdFactory.randomServerId(), addressesForReadReplica( 0 ),
                IdFactory.randomServerId(), addressesForReadReplica( 1 ) );

        return new DatabaseReadReplicaTopology( databaseId, readReplicas );
    }

    private static ReplicatedDatabaseState randomDatabaseState()
    {
        var databaseId = randomDatabaseId();
        var states = Map.of(
                IdFactory.randomServerId(), new DiscoveryDatabaseState( databaseId, STARTED ),
                IdFactory.randomServerId(), new DiscoveryDatabaseState( databaseId, STARTED ),
                IdFactory.randomServerId(), new DiscoveryDatabaseState( databaseId, STARTED ) );
        return ReplicatedDatabaseState.ofCores( databaseId, states );
    }

    @SuppressWarnings( "unchecked" )
    private static <T> SourceQueueWithComplete<T> newSinkMock()
    {
        return mock( SourceQueueWithComplete.class );
    }
}
