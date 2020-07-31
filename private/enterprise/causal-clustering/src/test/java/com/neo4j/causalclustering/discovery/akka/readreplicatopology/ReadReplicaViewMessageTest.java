/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.testkit.TestProbe;
import akka.testkit.javadsl.TestKit;
import co.unruly.matchers.StreamMatchers;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.identity.IdFactory;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomNamedDatabaseId;

class ReadReplicaViewMessageTest extends TestKit
{
    private final TestProbe clientManager = TestProbe.apply( getSystem() );
    private final ActorRef clusterClient = clientManager.childActorOf( Props.create( FakeClusterClient.class ) );
    private final ReadReplicaInfo readReplicaInfo = TestTopology.addressesForReadReplica( 0 );
    private final MemberId memberId = IdFactory.randomMemberId();
    private final Instant now = Instant.now();

    private final ReadReplicaViewRecord record = new ReadReplicaViewRecord( readReplicaInfo, clientManager.ref(), memberId, now, emptyMap() );

    private final ReadReplicaViewMessage readReplicaViewMessage = new ReadReplicaViewMessage( Map.of( clientManager.ref().path(), record ) );

    ReadReplicaViewMessageTest()
    {
        super( ActorSystem.create( "ReadReplicaViewMessage" ) );
    }

    @AfterEach
    void tearDown()
    {
        TestKit.shutdownActorSystem( getSystem() );
    }

    @Test
    void shouldReturnEmptyTopologyClientIfClusterClientUnknown()
    {
        assertThat( ReadReplicaViewMessage.EMPTY.topologyActorsForKnownClients( Set.of( clusterClient ) ), StreamMatchers.empty() );
    }

    @Test
    void shouldGetTopologyClientForClusterClient()
    {
        assertThat( readReplicaViewMessage.topologyActorsForKnownClients( Set.of( clusterClient ) ), StreamMatchers.contains( clientManager.ref() ) );
    }

    @Test
    void shouldReturnEmptyTopologyIfEmptyView()
    {
        DatabaseId databaseId = randomNamedDatabaseId().databaseId();
        assertThat( ReadReplicaViewMessage.EMPTY.toReadReplicaTopology( databaseId ),
                equalTo( new DatabaseReadReplicaTopology( databaseId, Map.of() ) ) );
    }

    @Test
    void shouldReturnReadReplicaTopology()
    {
        DatabaseId databaseId = Iterables.single( readReplicaInfo.startedDatabaseIds() );
        DatabaseReadReplicaTopology expected = new DatabaseReadReplicaTopology( databaseId,
                Map.of( memberId, readReplicaInfo ) );

        assertThat( readReplicaViewMessage.toReadReplicaTopology( databaseId ), equalTo( expected ) );
    }

    @Test
    void shouldReturnDatabaseIds()
    {
        var clusterClient1 = TestProbe.apply( getSystem() ).ref();
        var clusterClient2 = TestProbe.apply( getSystem() ).ref();
        var clusterClient3 = TestProbe.apply( getSystem() ).ref();

        var dbId1 = randomNamedDatabaseId().databaseId();
        var dbId2 = randomNamedDatabaseId().databaseId();
        var dbId3 = randomNamedDatabaseId().databaseId();

        var info1 = TestTopology.addressesForReadReplica( 0, Set.of( dbId1, dbId2 ) );
        var info2 = TestTopology.addressesForReadReplica( 1, Set.of( dbId3, dbId1 ) );
        var info3 = TestTopology.addressesForReadReplica( 2, Set.of( dbId2, dbId3 ) );

        var record1 = new ReadReplicaViewRecord( info1, clientManager.ref(), memberId, now, emptyMap() );
        var record2 = new ReadReplicaViewRecord( info2, clientManager.ref(), memberId, now, emptyMap() );
        var record3 = new ReadReplicaViewRecord( info3, clientManager.ref(), memberId, now, emptyMap() );

        var clusterClientReadReplicas = Map.of(
                clusterClient1.path(), record1,
                clusterClient2.path(), record2,
                clusterClient3.path(), record3 );
        var readReplicaViewMessage = new ReadReplicaViewMessage( clusterClientReadReplicas );

        var expectedDatabaseIds =
                Set.of( dbId1, dbId2, dbId3 );
        var actualDatabaseIds = readReplicaViewMessage.databaseIds();
        assertEquals( expectedDatabaseIds, actualDatabaseIds );
    }

    @Test
    void shouldReturnDatabaseStates()
    {
        var clusterClient1 = TestProbe.apply( getSystem() ).ref();
        var clusterClient2 = TestProbe.apply( getSystem() ).ref();
        var clusterClient3 = TestProbe.apply( getSystem() ).ref();

        var dbId1 = randomDatabaseId();
        var dbId2 = randomDatabaseId();
        var dbId3 = randomDatabaseId();

        var memberId1 = IdFactory.randomMemberId();
        var memberId2 = IdFactory.randomMemberId();
        var memberId3 = IdFactory.randomMemberId();

        var info1 = TestTopology.addressesForReadReplica( 0, Set.of( dbId1, dbId2 ) );
        var info2 = TestTopology.addressesForReadReplica( 1, Set.of( dbId3, dbId1 ) );
        var info3 = TestTopology.addressesForReadReplica( 2, Set.of( dbId2, dbId3 ) );

        Map<DatabaseId,DiscoveryDatabaseState> states1 = Map.of(
                dbId1, new DiscoveryDatabaseState( dbId1, STARTED ),
                dbId2, new DiscoveryDatabaseState( dbId2, STOPPED ) );
        Map<DatabaseId,DiscoveryDatabaseState> states2 = Map.of(
                dbId3, new DiscoveryDatabaseState( dbId1, STARTED ),
                dbId2, new DiscoveryDatabaseState( dbId3, STARTED ) );
        Map<DatabaseId,DiscoveryDatabaseState> states3 = Map.of(
                dbId2, new DiscoveryDatabaseState( dbId2, STARTED ),
                dbId3, new DiscoveryDatabaseState( dbId3, STARTED ) );

        var record1 = new ReadReplicaViewRecord( info1, clientManager.ref(), memberId1, now, states1 );
        var record2 = new ReadReplicaViewRecord( info2, clientManager.ref(), memberId2, now, states2 );
        var record3 = new ReadReplicaViewRecord( info3, clientManager.ref(), memberId3, now, states3 );

        var clusterClientReadReplicas = Map.of(
                clusterClient1.path(), record1,
                clusterClient2.path(), record2,
                clusterClient3.path(), record3 );
        var readReplicaViewMessage = new ReadReplicaViewMessage( clusterClientReadReplicas );

        var expectedState1 = ReplicatedDatabaseState.ofReadReplicas( dbId1,
                Map.of( memberId1, new DiscoveryDatabaseState( dbId1, STARTED ),
                        memberId2, new DiscoveryDatabaseState( dbId1, STARTED ) ) );
        var expectedState2 = ReplicatedDatabaseState.ofReadReplicas( dbId2,
                Map.of( memberId1, new DiscoveryDatabaseState( dbId2, STOPPED ),
                        memberId3, new DiscoveryDatabaseState( dbId2, STARTED ) ) );
        var expectedState3 = ReplicatedDatabaseState.ofReadReplicas( dbId3,
                Map.of( memberId2, new DiscoveryDatabaseState( dbId3, STARTED ),
                        memberId3, new DiscoveryDatabaseState( dbId3, STARTED ) ) );

        var allExpectedStates = Map.of( dbId1, expectedState1, dbId2, expectedState2, dbId3, expectedState3 );
        var allActualStates = readReplicaViewMessage.allReadReplicaDatabaseStates();
        assertEquals( allExpectedStates, allActualStates );
    }

    static class FakeClusterClient extends AbstractActor
    {
        @Override
        public Receive createReceive()
        {
            return receiveBuilder().build();
        }
    }
}
