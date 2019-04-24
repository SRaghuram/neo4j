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
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.database.DatabaseId;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ReadReplicaViewMessageTest extends TestKit
{
    private ActorRef clusterClient = TestProbe.apply( getSystem() ).ref();
    private ActorRef topologyClient = TestProbe.apply( getSystem() ).ref();
    private ReadReplicaInfo readReplicaInfo = TestTopology.addressesForReadReplica( 0 );
    private MemberId memberId = new MemberId( UUID.randomUUID() );
    private Instant now = Instant.now();

    private ReadReplicaViewRecord record = new ReadReplicaViewRecord( readReplicaInfo, topologyClient, memberId, now );

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
        DatabaseId databaseId = new DatabaseId( "no_such_database" );
        assertThat( ReadReplicaViewMessage.EMPTY.toReadReplicaTopology( databaseId ), equalTo( new DatabaseReadReplicaTopology( databaseId, Map.of() ) ) );
    }

    @Test
    void shouldReturnReadReplicaTopology()
    {
        DatabaseId databaseId = Iterables.single( readReplicaInfo.getDatabaseIds() );
        DatabaseReadReplicaTopology expected = new DatabaseReadReplicaTopology( databaseId, Map.of( memberId, readReplicaInfo ) );

        assertThat( readReplicaViewMessage.toReadReplicaTopology( databaseId ), equalTo( expected ) );
    }

    @Test
    void shouldReturnDatabaseIds()
    {
        var clusterClient1 = TestProbe.apply( getSystem() ).ref();
        var clusterClient2 = TestProbe.apply( getSystem() ).ref();
        var clusterClient3 = TestProbe.apply( getSystem() ).ref();

        var info1 = TestTopology.addressesForReadReplica( 0, Set.of( new DatabaseId( "orders" ), new DatabaseId( "customers" ) ) );
        var info2 = TestTopology.addressesForReadReplica( 0, Set.of( new DatabaseId( "employees" ), new DatabaseId( "orders" ) ) );
        var info3 = TestTopology.addressesForReadReplica( 0, Set.of( new DatabaseId( "customers" ), new DatabaseId( "employees" ) ) );

        var record1 = new ReadReplicaViewRecord( info1, topologyClient, memberId, now );
        var record2 = new ReadReplicaViewRecord( info2, topologyClient, memberId, now );
        var record3 = new ReadReplicaViewRecord( info3, topologyClient, memberId, now );

        var clusterClientReadReplicas = Map.of( clusterClient1, record1, clusterClient2, record2, clusterClient3, record3 );
        var readReplicaViewMessage = new ReadReplicaViewMessage( clusterClientReadReplicas );

        var expectedDatabaseIds = Set.of( new DatabaseId( "orders" ), new DatabaseId( "customers" ), new DatabaseId( "employees" ) );
        var actualDatabaseIds = readReplicaViewMessage.databaseIds();
        assertEquals( expectedDatabaseIds, actualDatabaseIds );
    }
}
