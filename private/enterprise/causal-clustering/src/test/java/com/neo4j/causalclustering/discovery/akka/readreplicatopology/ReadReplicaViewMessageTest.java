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
import org.hamcrest.Matchers;
import org.junit.Test;

import java.time.Instant;
import java.util.UUID;

import org.neo4j.helpers.collection.MapUtil;

import static org.junit.Assert.assertThat;

public class ReadReplicaViewMessageTest extends TestKit
{
    public ReadReplicaViewMessageTest()
    {
        super( ActorSystem.create( "ReadReplicaViewMessage" ) );
    }

    private ActorRef clusterClient = TestProbe.apply( getSystem() ).ref();
    private ActorRef topologyClient = TestProbe.apply( getSystem() ).ref();
    private ReadReplicaInfo readReplicaInfo = TestTopology.addressesForReadReplica( 0 );
    private MemberId memberId = new MemberId( UUID.randomUUID() );
    private Instant now = Instant.now();

    private ReadReplicaViewRecord record = new ReadReplicaViewRecord( readReplicaInfo, topologyClient, memberId, now );

    private ReadReplicaViewMessage readReplicaViewMessage = new ReadReplicaViewMessage( MapUtil.genericMap( clusterClient, record ) );

    @Test
    public void shouldReturnEmptyTopologyClientIfClusterClientUnknown()
    {
        assertThat( ReadReplicaViewMessage.EMPTY.topologyClient( clusterClient ), StreamMatchers.empty() );
    }

    @Test
    public void shouldGetTopologyClientForClusterClient()
    {
        assertThat( readReplicaViewMessage.topologyClient( clusterClient ), StreamMatchers.contains( topologyClient ) );
    }

    @Test
    public void shouldReturnEmptyTopologyIfEmptyView()
    {
        assertThat( ReadReplicaViewMessage.EMPTY.toReadReplicaTopology(), Matchers.equalTo( ReadReplicaTopology.EMPTY ) );
    }

    @Test
    public void shouldReturnReadReplicaTopology()
    {
        ReadReplicaTopology expected = new ReadReplicaTopology( MapUtil.genericMap( memberId, readReplicaInfo ) );

        assertThat( readReplicaViewMessage.toReadReplicaTopology(), Matchers.equalTo( expected ) );
    }
}
