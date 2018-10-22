/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.ExtendedActorSystem;
import akka.cluster.UniqueAddress;
import akka.testkit.javadsl.TestKit;
import com.neo4j.causalclustering.discovery.akka.coretopology.CoreServerInfoForMemberId;
import com.neo4j.causalclustering.discovery.akka.directory.ReplicatedLeaderInfo;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaRefreshMessage;
import com.neo4j.causalclustering.discovery.akka.readreplicatopology.ReadReplicaRemovalMessage;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.TestTopology;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;

@RunWith( Parameterized.class )
public class BaseAkkaSerializerTest
{
    private static ActorSystem system = ActorSystem.create();

    static
    {
        system.actorOf( ActorRefMarshalTest.Actor.props(), ActorRefMarshalTest.Actor.name + "1" );
        system.actorOf( ActorRefMarshalTest.Actor.props(), ActorRefMarshalTest.Actor.name + "2" );
    }

    @AfterClass
    public static void teardown()
    {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Parameterized.Parameter( 0 )
    public Object original;

    @Parameterized.Parameter( 1 )
    public BaseAkkaSerializer<?> serializer;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> data()
    {
        String actorPath = String.format( "akka://%s/user/%s", system.name(), ActorRefMarshalTest.Actor.name );
        return Arrays.asList(
            new Object[]{new LeaderInfo( new MemberId( UUID.randomUUID() ), 37L ),
                    new LeaderInfoSerializer()},
            new Object[]{new ClusterId( UUID.randomUUID() ),
                    new ClusterIdSerializer()},
            new Object[]{new UniqueAddress( new Address( "protocol", "system" ), 398L ),
                    new UniqueAddressSerializer()},
            new Object[]{new UniqueAddress( new Address( "protocol", "system", "host", 79 ), 398L ),
                    new UniqueAddressSerializer()},
            new Object[]{new CoreServerInfoForMemberId( new MemberId( UUID.randomUUID() ), TestTopology.addressesForCore( 1, false ) ),
                    new CoreServerInfoForMemberIdSerializer()},
            new Object[]{new ReadReplicaRefreshMessage(
                        TestTopology.addressesForReadReplica( 432 ),
                        new MemberId( UUID.randomUUID() ),
                        system.provider().resolveActorRef( actorPath + 1 ),
                        system.provider().resolveActorRef( actorPath + 2 ) ),
                    new ReadReplicaRefreshMessageSerializer( (ExtendedActorSystem)system )},
            new Object[]{new MemberId( UUID.randomUUID() ),
                    new MemberIdSerializer()},
            new Object[]{TestTopology.addressesForReadReplica( 74839 ),
                    new ReadReplicaInfoSerializer()},
            new Object[]{new CoreTopology( new ClusterId( UUID.randomUUID() ), true, CoreTopologyMarshalTest.coreServerInfos( 3 ) ),
                    new CoreTopologySerializer()},
            new Object[]{new ReadReplicaRemovalMessage( system.provider().resolveActorRef( actorPath + 2 ) ),
                    new ReadReplicaRemovalMessageSerializer( (ExtendedActorSystem)system )},
            new Object[]{ReadReplicaTopologyMarshalTest.generate(), new ReadReplicaTopologySerializer()},
            new Object[]{LeaderInfoDirectoryMessageMarshalTest.generate(), new DatabaseLeaderInfoMessageSerializer()},
            new Object[]{new ReplicatedLeaderInfo( new LeaderInfo( new MemberId( UUID.randomUUID() ), 14L ) ), new ReplicatedLeaderInfoSerializer()}
        );
    }

    @Test
    public void shouldSerializeAndDeserialize()
    {
        // when
        byte[] binary = serializer.toBinary( original );
        Object result = serializer.fromBinaryJava( binary, original.getClass() );

        // then
        assertNotSame( original, result );
        assertEquals( original, result );
    }

    @Test
    public void shouldHaveAppropriateSizeHint()
    {
        // when
        byte[] binary = serializer.toBinary( original );

        // then
        assertThat( binary.length, Matchers.lessThanOrEqualTo( serializer.sizeHint() ) );
    }
}
