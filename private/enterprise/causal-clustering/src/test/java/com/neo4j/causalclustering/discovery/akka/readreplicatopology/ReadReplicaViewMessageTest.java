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
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.ReadReplicaTopology;
import com.neo4j.causalclustering.discovery.TestTopology;
import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.database.DatabaseId;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

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
        assertThat( ReadReplicaViewMessage.EMPTY.toReadReplicaTopology( databaseId ), equalTo( new ReadReplicaTopology( databaseId, Map.of() ) ) );
    }

    @Test
    void shouldReturnReadReplicaTopology()
    {
        DatabaseId databaseId = Iterables.single( readReplicaInfo.getDatabaseIds() );
        ReadReplicaTopology expected = new ReadReplicaTopology( databaseId, Map.of( memberId, readReplicaInfo ) );

        assertThat( readReplicaViewMessage.toReadReplicaTopology( databaseId ), equalTo( expected ) );
    }
}
