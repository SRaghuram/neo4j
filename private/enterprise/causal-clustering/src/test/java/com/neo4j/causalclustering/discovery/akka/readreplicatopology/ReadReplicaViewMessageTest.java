/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.readreplicatopology;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.testkit.TestProbe;
import akka.testkit.javadsl.TestKit;
import co.unruly.matchers.StreamMatchers;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.discovery.akka.database.state.ReplicatedDatabaseState;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.dbms.EnterpriseDatabaseState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.neo4j.dbms.DatabaseState;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

class ReadReplicaViewMessageTest extends TestKit
{
    private ActorRef clusterClient = TestProbe.apply( getSystem() ).ref();
    private ActorRef topologyClient = TestProbe.apply( getSystem() ).ref();
    private ReadReplicaInfo readReplicaInfo = TestTopology.addressesForReadReplica( 0 );
    private MemberId memberId = new MemberId( UUID.randomUUID() );
    private Instant now = Instant.now();

    private ReadReplicaViewRecord record = new ReadReplicaViewRecord( readReplicaInfo, topologyClient, memberId, now, emptyMap() );

    private ReadReplicaViewMessage readReplicaViewMessage = new ReadReplicaViewMessage( Map.of( clusterClient, record ) );

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
        assertThat( ReadReplicaViewMessage.EMPTY.topologyClient( clusterClient ), StreamMatchers.empty() );
    }

    @Test
    void shouldGetTopologyClientForClusterClient()
    {
        assertThat( readReplicaViewMessage.topologyClient( clusterClient ), StreamMatchers.contains( topologyClient ) );
    }

    @Test
    void shouldReturnEmptyTopologyIfEmptyView()
    {
        DatabaseId databaseId = randomDatabaseId();
        assertThat( ReadReplicaViewMessage.EMPTY.toReadReplicaTopology( databaseId ), equalTo( new DatabaseReadReplicaTopology( databaseId, Map.of() ) ) );
    }

    @Test
    void shouldReturnReadReplicaTopology()
    {
        DatabaseId databaseId = Iterables.single( readReplicaInfo.databaseIds() );
        DatabaseReadReplicaTopology expected = new DatabaseReadReplicaTopology( databaseId, Map.of( memberId, readReplicaInfo ) );

        assertThat( readReplicaViewMessage.toReadReplicaTopology( databaseId ), equalTo( expected ) );
    }

    @Test
    void shouldReturnDatabaseIds()
    {
        var clusterClient1 = TestProbe.apply( getSystem() ).ref();
        var clusterClient2 = TestProbe.apply( getSystem() ).ref();
        var clusterClient3 = TestProbe.apply( getSystem() ).ref();

        var dbId1 = randomDatabaseId();
        var dbId2 = randomDatabaseId();
        var dbId3 = randomDatabaseId();

        var info1 = TestTopology.addressesForReadReplica( 0, Set.of( dbId1, dbId2 ) );
        var info2 = TestTopology.addressesForReadReplica( 1, Set.of( dbId3, dbId1 ) );
        var info3 = TestTopology.addressesForReadReplica( 2, Set.of( dbId2, dbId3 ) );

        var record1 = new ReadReplicaViewRecord( info1, topologyClient, memberId, now, emptyMap() );
        var record2 = new ReadReplicaViewRecord( info2, topologyClient, memberId, now, emptyMap() );
        var record3 = new ReadReplicaViewRecord( info3, topologyClient, memberId, now, emptyMap() );

        var clusterClientReadReplicas = Map.of( clusterClient1, record1, clusterClient2, record2, clusterClient3, record3 );
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

        var memberId1 = new MemberId( UUID.randomUUID() );
        var memberId2 = new MemberId( UUID.randomUUID() );
        var memberId3 = new MemberId( UUID.randomUUID() );

        var info1 = TestTopology.addressesForReadReplica( 0, Set.of( dbId1, dbId2 ) );
        var info2 = TestTopology.addressesForReadReplica( 1, Set.of( dbId3, dbId1 ) );
        var info3 = TestTopology.addressesForReadReplica( 2, Set.of( dbId2, dbId3 ) );

        Map<DatabaseId,DatabaseState> states1 = Map.of(
                dbId1, new EnterpriseDatabaseState( dbId1, STARTED ),
                dbId2, new EnterpriseDatabaseState( dbId2, STOPPED ) );
        Map<DatabaseId,DatabaseState> states2 = Map.of(
                dbId3, new EnterpriseDatabaseState( dbId1, STARTED ),
                dbId2, new EnterpriseDatabaseState( dbId3, STARTED ) );
        Map<DatabaseId,DatabaseState> states3 = Map.of(
                dbId2, new EnterpriseDatabaseState( dbId2, STARTED ),
                dbId3, new EnterpriseDatabaseState( dbId3, STARTED ) );

        var record1 = new ReadReplicaViewRecord( info1, topologyClient, memberId1, now, states1 );
        var record2 = new ReadReplicaViewRecord( info2, topologyClient, memberId2, now, states2 );
        var record3 = new ReadReplicaViewRecord( info3, topologyClient, memberId3, now, states3 );

        var clusterClientReadReplicas = Map.of( clusterClient1, record1, clusterClient2, record2, clusterClient3, record3 );
        var readReplicaViewMessage = new ReadReplicaViewMessage( clusterClientReadReplicas );

        var expectedState1 = ReplicatedDatabaseState.ofReadReplicas( dbId1,
                Map.of( memberId1, new EnterpriseDatabaseState( dbId1, STARTED ),
                        memberId2, new EnterpriseDatabaseState( dbId1, STARTED ) ) );
        var expectedState2 = ReplicatedDatabaseState.ofReadReplicas( dbId2,
                Map.of( memberId1, new EnterpriseDatabaseState( dbId2, STOPPED ),
                        memberId3, new EnterpriseDatabaseState( dbId2, STARTED ) ) );
        var expectedState3 = ReplicatedDatabaseState.ofReadReplicas( dbId3,
                Map.of( memberId2, new EnterpriseDatabaseState( dbId3, STARTED ),
                        memberId3, new EnterpriseDatabaseState( dbId3, STARTED ) ) );

        var allExpectedStates = Map.of( dbId1, expectedState1, dbId2, expectedState2, dbId3, expectedState3 );
        var allActualStates = readReplicaViewMessage.allReplicatedDatabaseStates();
        assertEquals( allExpectedStates, allActualStates );
    }
}
